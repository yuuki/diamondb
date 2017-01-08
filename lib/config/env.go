package config

import (
	"github.com/yuuki/diamondb/lib/storage"
)

type Env struct {
	Fetcher storage.Fetcher
}

func NewFetcher() storage.Fetcher {
	return &storage.Store{}
}
