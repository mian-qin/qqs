package main

import (
	"context"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/square/quotaservice/experiment/qqs"
	"github.com/square/quotaservice/experiment/qqs/util"

	flags "github.com/jessevdk/go-flags"
)

type Options struct {
	Cap           int    `short:"m" long:"max" description:"max quota per request" default:"1"`
	ServerAddr    string `short:"a" long:"server-address" description:"server address" default:"localhost:10990"`
	QueryCount    int    `short:"n" long:"query-count" description:"query count for testing" default:"1"`
	QueryInterval int    `short:"i" long:"query-interval" description:"query interval in milliseconds between two queries" default:"100"`
	Blocking      int    `short:"b" long:"blocking" description:"use blocking quota queries" default:"1"`
	Projects      int    `short:"p" long:"projects" description:"number of projects for testing" default:"1"`
	Timeout       int64  `short:"t" long:"timeout" description:"request timeout in ms" default:"1000"`
}

func main() {
	var o Options
	parser := flags.NewParser(&o, flags.Default)
	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	// Catch interrupt
	ctx, cancel := context.WithCancel(context.Background())
	cc := make(chan os.Signal, 1)
	signal.Notify(cc, os.Interrupt)

	go func() {
		for range cc {
			cancel()
		}
	}()

	client, err := qqs.NewClient(ctx, o.ServerAddr)
	if err != nil {
		qqsutil.LogError(err)
		return
	}

	timeout := time.Duration(o.Timeout) * time.Millisecond
	var wg sync.WaitGroup
	for p := 1; p <= o.Projects; p++ {
		wg.Add(1)
		go func(p int) {
			after := time.After(0)
			for i := 0; i < o.QueryCount; i++ {
				select {
				case <-ctx.Done():
					return
				case <-after:
					r := qqs.AskForQuota(ctx,
						client,
						int64(o.Cap),
						o.Blocking == 1,
						strconv.FormatInt(int64(i), 10),
						strconv.FormatInt(int64(p), 10),
						timeout,
					)
					if r.Error != nil {
						qqsutil.LogError(r.Error)
					}
					if i < o.QueryCount-1 {
						after = time.After(time.Duration(o.QueryInterval) * time.Millisecond)
					}
				}
			}

			wg.Done()
		}(p)
	}

	wg.Wait()
	qqsutil.Log("Client closed")
}
