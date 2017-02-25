package env

import (
	"github.com/yuuki/diamondb/pkg/storage"
)

// Env represents a store of persistent objects such as database client
type Env struct {
	ReadWriter storage.ReadWriter
}
