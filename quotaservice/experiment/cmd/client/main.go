package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/mian-qin/qqs/quotaservice/experiment"
	"github.com/mian-qin/qqs/quotaservice/experiment/util"

	flags "github.com/jessevdk/go-flags"
)

type Options struct {
	ServerAddr string `short:"a" long:"server-address" description:"server address" default:"localhost:10990"`
	Timeout    int64  `short:"t" long:"timeout" description:"request timeout in ms" default:"1000"`

	Cap           int `short:"m" long:"max" description:"max quota per request" default:"1"`
	QueryCount    int `short:"n" long:"query-count" description:"query count for testing" default:"1"`
	QueryInterval int `short:"i" long:"query-interval" description:"query interval in milliseconds between two queries" default:"100"`
	Blocking      int `short:"b" long:"blocking" description:"use blocking quota queries" default:"1"`
	Projects      int `short:"p" long:"projects" description:"number of projects for testing" default:"1"`
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
			defer func() {
				wg.Done()
			}()
			for i := 0; i < o.QueryCount; i++ {
				select {
				case <-ctx.Done():
					return
				case <-after:
					r := AskForQuota(ctx,
						client,
						int64(o.Cap),
						o.Blocking == 1,
						strconv.FormatInt(int64(p), 10),
						strconv.FormatInt(int64(i), 10),
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
		}(p)
	}

	wg.Wait()
	qqsutil.Log("Client closed")
}

type AskResult struct {
	Granted int64
	WaitMs  int64
	Error   error
}

// AskForQuota sends quota query request with client timeout
func AskForQuota(ctx context.Context, cl *qqs.QQClient, cap int64, blocking bool, p, name string, to time.Duration) AskResult {
	rctx, rcancel := context.WithTimeout(ctx, to)
	defer rcancel()

	t := rand.Int63n(cap) + 1
	qqsutil.Log("#%s requesting %d token(s) for project %s\n", name, t, p)
	rs := make(chan AskResult)
	if to <= 0 {
		to = time.Hour
	}
	after := time.After(to)
	go func() {
		gr, wait, err := cl.Allow(rctx,
			blocking,
			"project",
			p,
			t,
			20*1000,
		)

		rs <- AskResult{
			Granted: gr,
			WaitMs:  wait,
			Error:   err,
		}
	}()

	select {
	case r := <-rs:
		if r.Error == nil {
			if r.Granted == 0 && r.WaitMs == 0 {
				r.Error = fmt.Errorf("No quota available for project %s", p)
			} else {
				qqsutil.Log("#%s: %d tokens granted for project: %s. Need to wait for %d ms.\n", name, r.Granted, p, r.WaitMs)
			}
		}

		return r
	case <-after:
		return AskResult{
			Error: fmt.Errorf("Quota query call timed out"),
		}
	}
}
