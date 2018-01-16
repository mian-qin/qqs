// Licensed under the Apache License, Version 2.0
// Details: https://raw.githubusercontent.com/square/quotaservice/master/LICENSE

package memory

import (
	"os"
	"testing"

	"github.com/square/quotaservice/buckets"
	"github.com/square/quotaservice/config"
)

var factory = NewBucketFactory()

func TestMain(m *testing.M) {
	setUp()
	r := m.Run()
	os.Exit(r)
}

func setUp() {
	factory.Init(config.NewDefaultServiceConfig())
}

func TestTokenAcquisition(t *testing.T) {
	bucket := factory.NewBucket("memory", "memory", config.NewDefaultBucketConfig(""), false)
	buckets.TestTokenAcquisition(t, bucket)
}

func TestGC(t *testing.T) {
	buckets.TestGC(t, factory, "memory")
}
