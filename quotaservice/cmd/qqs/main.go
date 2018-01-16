package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/square/quotaservice"
	"github.com/square/quotaservice/buckets/memory"
	qsclient "github.com/square/quotaservice/client"
	"github.com/square/quotaservice/config"
	pb "github.com/square/quotaservice/protos"
	qsgrpc "github.com/square/quotaservice/rpc/grpc"
	"google.golang.org/grpc"

	flags "github.com/jessevdk/go-flags"
)

type Options struct {
	Cap             int    `short:"m" long:"max" description:"max quota per project" default:"5"`
	FillRate        int    `short:"f" long:"fill-rate" description:"quota accumulation rate." default:"1"`
	ServerAddr      string `short:"a" long:"server-address" description:"server address" default:"localhost:10990"`
	QueryCount      int    `short:"n" long:"query-count" description:"query count for testing" default:"1"`
	QueryInterval   int    `short:"i" long:"query-interval" description:"query interval in milliseconds between two queries" default:"100"`
	RejectThreshold int    `short:"r" long:"reject-threshold" description:"debt threshold in seconds for rejection" default:"100"`
	Blocking        int    `short:"b" long:"blocking" description:"use blocking quota queries" default:"1"`
	Projects        int    `short:"p" long:"projects" description:"number of projects for testing" default:"1"`
}

func setUpServer(addr string, cap int64, fr int64, rs int64, p int) quotaservice.Server {
	cfg := config.NewDefaultServiceConfig()
	cfg.GlobalDefaultBucket = config.NewDefaultBucketConfig(config.DefaultBucketName)

	nsc := config.NewDefaultNamespaceConfig("project")
	fmt.Printf("Creating server with %d projects. Fill rate is %d.\n", p, fr)
	for i := 1; i <= p; i++ {
		bc := config.NewDefaultBucketConfig(strconv.FormatInt(int64(i), 10))
		bc.Size = cap
		bc.FillRate = fr
		bc.WaitTimeoutMillis = 120 * 1000
		bc.MaxDebtMillis = rs * 1000
		config.AddBucket(nsc, bc)
	}

	config.AddNamespace(cfg, nsc)

	r := config.NewReaperConfig()
	r.MinFrequency = 60 * 1000 * time.Millisecond
	r.InitSleep = 60 * 1000 * time.Millisecond
	server := quotaservice.New(memory.NewBucketFactory(),
		config.NewMemoryConfig(cfg),
		r,
		0,
		qsgrpc.New(addr))

	return server
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

	server := setUpServer(o.ServerAddr, int64(o.Cap), int64(o.FillRate), int64(o.RejectThreshold), o.Projects)
	if _, err := server.Start(); err != nil {
		LogError(err)
	}

	client, err := setUpClient(o.ServerAddr)
	if err != nil {
		LogError(err)
	}

	// for p := 1; p <= o.Projects; p++ {
	// 	for i := 0; i < o.QueryCount; i++ {
	// 		t := rand.Int63n(int64(o.Cap)) + 1
	// 		req := &pb.AllowRequest{
	// 			Namespace:       "project",
	// 			BucketName:      strconv.FormatInt(int64(p), 10),
	// 			TokensRequested: t,
	// 			// MaxWaitTimeOverride:   true,
	// 			MaxWaitMillisOverride: 20 * 1000,
	// 		}
	//
	// 		Log("#%d requesting %d tokens for project %d\n", i, t, p)
	// 		if o.Blocking == 1 {
	// 			err := client.AllowBlocking(req)
	// 			if err != nil {
	// 				LogError(err)
	// 			} else {
	// 				Log("#%d for project %d is approved\n", i, p)
	// 			}
	// 		} else {
	// 			resp, err := client.Allow(req)
	// 			Log("Tokens granted for project: %d. Need to wait for %d ms.\n", p, resp.TokensGranted, resp.WaitMillis)
	// 			if resp.WaitMillis == 0 && resp.TokensGranted == t && err == nil {
	// 				Log("#%d is approved\n", i)
	// 			} else if err != nil {
	// 				LogError(err)
	// 			} else {
	// 				Log("%d tokens have been granted", resp.TokensGranted)
	// 			}
	// 		}
	//
	// 		if i < o.QueryCount-1 {
	// 			time.Sleep(time.Duration(o.QueryInterval) * time.Millisecond)
	// 		}
	// 	}
	//
	// 	// if resp.Status != pb.AllowResponse_OK {
	// 	// 	LogError(fmt.Errorf("Response not OK. %v", pb.AllowResponse_Status_name[int32(resp.Status)]))
	// 	// }
	//
	// }

	var wg sync.WaitGroup
	for p := 1; p <= o.Projects; p++ {
		wg.Add(1)
		go func(p int) {
			for i := 0; i < o.QueryCount; i++ {
				t := rand.Int63n(int64(o.Cap)) + 1
				req := &pb.AllowRequest{
					Namespace:       "project",
					BucketName:      strconv.FormatInt(int64(p), 10),
					TokensRequested: t,
					// MaxWaitTimeOverride:   true,
					MaxWaitMillisOverride: 20 * 1000,
				}

				Log("#%d requesting %d tokens for project %d\n", i, t, p)
				if o.Blocking == 1 {
					err := client.AllowBlocking(req)
					if err != nil {
						LogError(err)
					} else {
						Log("#%d is approved\n", i)
					}
				} else {
					resp, err := client.Allow(req)
					Log("Tokens granted for project: %d. Need to wait for %d ms.\n", p, resp.TokensGranted, resp.WaitMillis)
					if resp.WaitMillis == 0 && resp.TokensGranted == t && err == nil {
						Log("#%d is approved\n", i)
					} else if err != nil {
						LogError(err)
					}

				}

				// if resp.Status != pb.AllowResponse_OK {
				// 	LogError(fmt.Errorf("Response not OK. %v", pb.AllowResponse_Status_name[int32(resp.Status)]))
				// }

				if i < o.QueryCount-1 {
					time.Sleep(time.Duration(o.QueryInterval) * time.Millisecond)
				}
			}

			wg.Done()
		}(p)
	}

	wg.Wait()

	server.Stop()
	Log("Complete.")
}

func LogError(err error) {
	println("[Error]" + err.Error())
}

func Log(s string, a ...interface{}) {
	str := s
	if len(a) > 0 {
		str = fmt.Sprintf(s, a...)
	}

	fmt.Printf("[%v]%s\n", time.Now(), str)
}
