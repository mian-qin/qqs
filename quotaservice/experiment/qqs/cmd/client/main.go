package main

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	qsclient "github.com/square/quotaservice/client"
	"github.com/square/quotaservice/experiment/qqs/util"
	pb "github.com/square/quotaservice/protos"
	"google.golang.org/grpc"

	flags "github.com/jessevdk/go-flags"
)

type Options struct {
	Cap           int    `short:"m" long:"max" description:"max quota per project" default:"5"`
	ServerAddr    string `short:"a" long:"server-address" description:"server address" default:"localhost:10990"`
	QueryCount    int    `short:"n" long:"query-count" description:"query count for testing" default:"1"`
	QueryInterval int    `short:"i" long:"query-interval" description:"query interval in milliseconds between two queries" default:"100"`
	Blocking      int    `short:"b" long:"blocking" description:"use blocking quota queries" default:"1"`
	Projects      int    `short:"p" long:"projects" description:"number of projects for testing" default:"1"`
}

func setUpClient(addr string) (*qsclient.Client, error) {
	client, err := qsclient.New(addr, grpc.WithInsecure())
	return client, err
}

func main() {
	var o Options
	parser := flags.NewParser(&o, flags.Default)
	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	client, err := setUpClient(o.ServerAddr)
	if err != nil {
		qqs.LogError(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	cc := make(chan os.Signal, 1)
	signal.Notify(cc, os.Interrupt)

	go func() {
		for range cc {
			cancel()
		}
	}()

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
					req := &pb.AllowRequest{
						Namespace:       "project",
						BucketName:      strconv.FormatInt(int64(p), 10),
						TokensRequested: t,
						// MaxWaitTimeOverride:   true,
						MaxWaitMillisOverride: 20 * 1000,
					}

					qqs.Log("#%d requesting %d tokens for project %d\n", i, t, p)
					if o.Blocking == 1 {
						err := client.AllowBlocking(req)
						if err != nil {
							qqs.LogError(err)
						} else {
							qqs.Log("#%d is approved\n", i)
						}
					} else {
						resp, err := client.Allow(req)
						qqs.Log("#%d: %d tokens granted for project: %d. Need to wait for %d ms.\n", i, resp.TokensGranted, p, resp.WaitMillis)
						if resp.WaitMillis == 0 && resp.TokensGranted == t && err == nil {
							qqs.Log("#%d is approved\n", i)
						} else if err != nil {
							qqs.LogError(err)
						}
					}
					// if resp.Status != pb.AllowResponse_OK {
					// 	qqs.LogError(fmt.Errorf("Response not OK. %v", pb.AllowResponse_Status_name[int32(resp.Status)]))
					// }

					if i < o.QueryCount-1 {
						after = time.After(time.Duration(o.QueryInterval) * time.Millisecond)
					}
				}
			}

			wg.Done()
		}(p)
	}

	wg.Wait()
	qqs.Log("Client closed")
}
