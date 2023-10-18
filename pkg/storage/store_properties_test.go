package storage

import (
	"context"
	"testing"
)

func TestStoreProperties(t *testing.T) {
	props := getFileSystemProperties(context.Background(), "/home/pavel")
	t.Logf("%+v", props)
	t.FailNow()
}
