package main

import (
	"context"
	"math/rand"
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
	Cap           int    `short:"m" long:"max" description:"max quota per project" default:"5"`
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
					t := rand.Int63n(int64(o.Cap)) + 1
					rctx, rcancel := context.WithTimeout(ctx, time.Duration(o.Timeout)*time.Millisecond)
					defer rcancel()

					qqsutil.Log("#%d requesting %d token(s) for project %d\n", i, t, p)
					gr, wait, err := client.Allow(rctx,
						o.Blocking == 1,
						"project",
						strconv.FormatInt(int64(p), 10),
						t,
						20*1000,
					)

					if err != nil {
						qqsutil.LogError(err)
						continue
					}

					qqsutil.Log("#%d: %d tokens granted for project: %d. Need to wait for %d ms.\n", i, gr, p, wait)
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
