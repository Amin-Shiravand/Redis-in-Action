package model

import (
	"context"
	"encoding/json"
	"log"
	"math"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"redis-in-action/chapter02/common"
	"redis-in-action/chapter02/repository"
)

type Client struct {
	Conn *redis.Client
}

func NewClient(conn *redis.Client) *Client {
	return &Client{Conn: conn}
}

func (r *Client) CheckToken(ctx context.Context, token string) string {
	return r.Conn.HGet(ctx, common.LOGIN, token).Val()
}

func (r *Client) UpdateToken(ctx context.Context, token, user, item string) {
	timestamp := time.Now().Unix()
	r.Conn.HMSet(ctx, common.LOGIN, token, user)
	r.Conn.ZAdd(ctx, common.RECENT, redis.Z{
		Score:  float64(timestamp),
		Member: token,
	})
	if item != "" {
		r.Conn.ZAdd(ctx, common.VIEWED+token, redis.Z{
			Score:  float64(timestamp),
			Member: item,
		})
		r.Conn.ZRemRangeByRank(ctx, common.VIEWED+token, 0, -26)
		r.Conn.ZIncrBy(ctx, common.VIEWED, -1, item)
	}
}

func (r *Client) CleanSessions(ctx context.Context) {
	for !common.QUIT {
		size := r.Conn.ZCard(ctx, common.RECENT).Val()
		if size <= common.LIMIT {
			time.Sleep(1 * time.Second)
			continue
		}
		endIndex := math.Min(float64(size-common.LIMIT), 100)
		tokens := r.Conn.ZRange(ctx, common.RECENT, 0, int64(endIndex-1)).Val()
		var sessionKey []string
		for _, token := range tokens {
			sessionKey = append(sessionKey, token)
		}
		r.Conn.Del(ctx, sessionKey...)
		r.Conn.HDel(ctx, common.LOGIN, tokens...)
		r.Conn.ZRem(ctx, common.RECENT, tokens)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

func (r *Client) AddToCart(ctx context.Context, session, item string, count int) {
	switch {
	case count < 0:
		r.Conn.HDel(ctx, common.CART+session, item)
	default:
		r.Conn.HSet(ctx, common.CART+session, item, count)
	}
}

func (r *Client) CleanFullSessions(ctx context.Context) {
	for !common.QUIT {
		size := r.Conn.ZCard(ctx, common.RECENT).Val()
		if size < common.LIMIT {
			time.Sleep(1 * time.Second)
			continue
		}

		endIndex := math.Min(float64(size-common.LIMIT), 100)
		sessions := r.Conn.ZRange(ctx, common.RECENT, 0, int64(endIndex-1)).Val()
		var sessionKeys []string
		for _, session := range sessions {
			sessionKeys = append(sessionKeys, common.VIEWED+session)
			sessionKeys = append(sessionKeys, common.CART+session)
		}
		r.Conn.Del(ctx, sessionKeys...)
		r.Conn.HDel(ctx, common.LOGIN, sessions...)
		r.Conn.ZRem(ctx, common.RECENT, sessions)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

func (r *Client) ScheduleRowCache(ctx context.Context, rowId string, delay int64) {
	r.Conn.ZAdd(ctx, common.DELAY, redis.Z{Member: rowId, Score: float64(delay)})
	r.Conn.ZAdd(ctx, common.SCHEDULE, redis.Z{Member: rowId, Score: float64(time.Now().Unix())})
}

func (r *Client) CacheRows(ctx context.Context) {
	for !common.QUIT {
		next := r.Conn.ZRangeWithScores(ctx, common.SCHEDULE, 0, 0).Val()
		now := time.Now().Unix()
		if len(next) == 0 || next[0].Score > float64(now) {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		rowId := next[0].Member.(string)
		delay := r.Conn.ZScore(ctx, common.DELAY, rowId).Val()
		if delay <= 0 {
			r.Conn.ZRem(ctx, common.DELAY, rowId)
			r.Conn.ZRem(ctx, common.SCHEDULE, rowId)
			r.Conn.Del(ctx, common.INVENTORY+rowId)
			continue
		}

		row := repository.Get(rowId)
		r.Conn.ZAdd(ctx, common.SCHEDULE, redis.Z{Member: rowId, Score: float64(now) + delay})
		jsonRow, err := json.Marshal(row)
		if err != nil {
			log.Printf("marshal json failed, data is: %v, err is: %v\n", row, err)
			continue
		}
		r.Conn.Set(ctx, common.INVENTORY+rowId, jsonRow, 0)
	}
	defer atomic.AddInt32(&common.FLAG, -1)
}

func (r *Client) RescaleViewed(ctx context.Context) {
	for !common.QUIT {
		r.Conn.ZRemRangeByRank(ctx, common.VIEWED, 20000, -1)
		r.Conn.ZInterStore(ctx, common.VIEWED, &redis.ZStore{Weights: []float64{0.5}, Keys: []string{common.VIEWED}})
		time.Sleep(300 * time.Second)
	}
}

func (r *Client) CanCache(ctx context.Context, request string) bool {
	itemId := extractItemId(request)
	if itemId == "" || isDynamic(request) {
		return false
	}
	rank := r.Conn.ZRank(ctx, common.VIEWED, itemId).Val()
	return rank != 0 && rank < 10000
}

func (r *Client) Reset(ctx context.Context) {
	r.Conn.FlushDB(ctx)

	common.SetQuit(false)
	common.SetLimit(10000000)
	common.SetFlag(1)
}

func extractItemId(request string) string {
	parsed, _ := url.Parse(request)
	queryValue, _ := url.ParseQuery(parsed.RawQuery)
	query := queryValue.Get(common.ITEM)
	return query
}

func isDynamic(request string) bool {
	parsed, _ := url.Parse(request)
	queryValue, _ := url.ParseQuery(parsed.RawQuery)
	for _, v := range queryValue {
		for _, j := range v {
			if strings.Contains(j, "_") {
				return false
			}
		}
	}
	return true
}
