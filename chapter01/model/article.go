package model

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"redis-in-action/chapter01/common"
	"strconv"
	"strings"
	"time"
)

// Constants for key prefixes
type ArticleRepo struct {
	Connection *redis.Client
}

// ArticleData represents the data structure for an article
type ArticleData struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Link   string `json:"link"`
	Poster string `json:"poster"`
	Time   int64  `json:"time"`
	Votes  int64  `json:"votes"`
}

func NewArticleRepo(connection *redis.Client) *ArticleRepo {
	return &ArticleRepo{Connection: connection}
}

func (r *ArticleRepo) AddRemoveGroups(ctx context.Context, articleID string, toAdd, toRemove []string) error {
	articleKey := common.ArticlePrefix + articleID

	// Add to groups
	for _, group := range toAdd {
		r.Connection.SAdd(ctx, common.GroupPrefix+group, articleKey)
	}

	// Remove from groups
	for _, group := range toRemove {
		r.Connection.SRem(ctx, common.GroupPrefix+group, articleKey)
	}

	return nil
}

func (r *ArticleRepo) GetArticles(ctx context.Context, page int64, order string) ([]ArticleData, error) {
	if order == "" {
		order = "score:"
	}
	start := (page - 1) * common.ArticlePerPage
	end := start + common.ArticlePerPage - 1
	ids := r.Connection.ZRevRange(ctx, order, start, end).Val()
	var articles []ArticleData
	for _, id := range ids {
		articleData, err := r.getArticleData(ctx, id)
		if err != nil {
			return nil, err
		}
		articles = append(articles, articleData)
	}
	return articles, nil
}

func (r *ArticleRepo) PostArticle(ctx context.Context, user, title, link string) (string, error) {
	articleID := fmt.Sprintf("%d", r.Connection.Incr(ctx, "Article:").Val())
	voted := common.VotedPrefix + articleID
	r.Connection.SAdd(ctx, voted, user)
	r.Connection.Expire(ctx, voted, common.OneWeekInSecond*time.Second)
	now := time.Now().Unix()
	articleKey := common.ArticlePrefix + articleID
	articleData := ArticleData{
		ID:     articleID,
		Title:  title,
		Link:   link,
		Poster: user,
		Time:   now,
		Votes:  1,
	}
	if err := r.setArticleData(ctx, articleKey, articleData); err != nil {
		return "", err
	}
	r.Connection.ZAdd(ctx, "score:",
		redis.Z{
			Score:  float64(now + common.VoteScore),
			Member: articleKey,
		})
	r.Connection.ZAdd(ctx, "time:",
		redis.Z{
			Score:  float64(now),
			Member: articleKey,
		})
	return articleID, nil
}

// Add this method to the ArticleRepo

func (r *ArticleRepo) DownVoteArticle(ctx context.Context, articleKey, user string) error {
	cutOff := time.Now().Unix() - common.OneWeekInSecond
	score := r.Connection.ZScore(ctx, "time:", articleKey).Val()
	if score < float64(cutOff) {
		return errors.New("article is too old to vote")
	}
	articleID := strings.Split(articleKey, ":")[1]
	votedKey := common.VotedPrefix + articleID

	// Check if the user has already voted for this article
	if r.Connection.SIsMember(ctx, votedKey, user).Val() {
		return errors.New("user has already voted for this article")
	}

	// Decrease the article's score and update the votes
	r.Connection.ZIncrBy(ctx, "score:", -common.VoteScore, articleKey)
	r.Connection.HIncrBy(ctx, articleKey, "votes", -1)

	// Add the user to the list of voters
	r.Connection.SAdd(ctx, votedKey, user)

	return nil
}

func (r *ArticleRepo) ArticleVote(ctx context.Context, articleKey, user string) error {
	cutOff := time.Now().Unix() - common.OneWeekInSecond
	score := r.Connection.ZScore(ctx, "time:", articleKey).Val()
	if score < float64(cutOff) {
		return errors.New("article is too old to vote")
	}
	articleID := strings.Split(articleKey, ":")[1]
	votedKey := common.VotedPrefix + articleID
	if r.Connection.SAdd(ctx, votedKey, user).Val() != 0 {
		r.Connection.ZIncrBy(ctx, "score:", common.VoteScore, articleKey)
		r.Connection.HIncrBy(ctx, articleKey, "votes", 1)
	}
	return nil
}

func (r *ArticleRepo) GetGroupArticles(ctx context.Context, group, order string, page int64) ([]ArticleData, error) {
	if order == "" {
		order = "score:"
	}
	key := order + group
	if r.Connection.Exists(ctx, key).Val() == 0 {
		res := r.Connection.ZInterStore(ctx, key, &redis.ZStore{Aggregate: "MAX", Keys: []string{common.GroupPrefix + group, order}}).Val()
		if res <= 0 {
			log.Print("Zinterstore returned 0")
		}
		r.Connection.Expire(ctx, key, 60*time.Second)
	}
	return r.GetArticles(ctx, page, key)
}

func (r *ArticleRepo) setArticleData(ctx context.Context, articleKey string, data ArticleData) error {
	dataMap := map[string]interface{}{
		"id":     data.ID,
		"title":  data.Title,
		"link":   data.Link,
		"poster": data.Poster,
		"time":   data.Time,
		"votes":  data.Votes,
	}
	_, err := r.Connection.HMSet(ctx, articleKey, dataMap).Result()
	return err
}

func (r *ArticleRepo) getArticleData(ctx context.Context, articleKey string) (ArticleData, error) {
	dataMap, err := r.Connection.HGetAll(ctx, articleKey).Result()
	if err != nil {
		return ArticleData{}, err
	}
	data := ArticleData{
		ID:     dataMap["id"],
		Title:  dataMap["title"],
		Link:   dataMap["link"],
		Poster: dataMap["poster"],
	}
	if timeStr, ok := dataMap["time"]; ok {
		if timeInt, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
			data.Time = timeInt
		}
	}
	if votesStr, ok := dataMap["votes"]; ok {
		if votesInt, err := strconv.ParseInt(votesStr, 10, 64); err == nil {
			data.Votes = votesInt
		}
	}
	return data, nil
}

func (r *ArticleRepo) Reset(ctx context.Context) {
	r.Connection.FlushDB(ctx)
}
