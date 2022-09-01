package TokenBucket

import (
	"time"
)


type TokenBucket struct {
	Rate                int64
	MaxTokens           int64
	currentTokens       int64
	lastRefillTimestamp int64
}

func NewTokenBucket(Rate int64, MaxTokens int64) *TokenBucket {
	return &TokenBucket{
		Rate:                Rate,
		MaxTokens:           MaxTokens,
		lastRefillTimestamp: time.Now().Unix(),
		currentTokens:       MaxTokens,
	}
}

func (tokenBucket *TokenBucket) IsRequestAllowed() bool {
	timeNow := time.Now().Unix()
	elapsedTime := timeNow - tokenBucket.lastRefillTimestamp
	if elapsedTime>tokenBucket.Rate{
		tokenBucket.currentTokens = tokenBucket.MaxTokens
		tokenBucket.lastRefillTimestamp = timeNow
	}
	if tokenBucket.currentTokens <= 0 {
		return false
	}
	tokenBucket.currentTokens--
	return true
}