package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/mian-qin/qqs/quotaservice"
	"github.com/mian-qin/qqs/quotaservice/buckets/redis"
	"github.com/mian-qin/qqs/quotaservice/config"
	qqs "github.com/mian-qin/qqs/quotaservice/experiment/util"
	pb "github.com/mian-qin/qqs/quotaservice/protos/config"
	qsgrpc "github.com/mian-qin/qqs/quotaservice/rpc/grpc"
	redisclient "gopkg.in/redis.v5"

	flags "github.com/jessevdk/go-flags"
)

type Options struct {
	Cap             int    `short:"m" long:"max" description:"max quota per project" default:"5"`
	FillRate        int    `short:"f" long:"fill-rate" description:"quota accumulation rate." default:"1"`
	ServerAddr      string `short:"a" long:"server-address" description:"server address" default:"localhost:10990"`
	RejectThreshold int    `short:"r" long:"reject-threshold" description:"debt threshold in seconds for rejection" default:"100"`
	Projects        int    `short:"p" long:"projects" description:"number of projects for testing" default:"1"`
}

type QQServer struct {
	cfg        *pb.ServiceConfig
	nscProject *pb.NamespaceConfig
	Srv        quotaservice.Server
}

func (qs *QQServer) Init(addr string, cap int64, fr int64, rs int64, p int) {
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
	r.MinFrequency = 10 * 60 * 1000 * time.Millisecond
	r.InitSleep = 10 * 60 * 1000 * time.Millisecond
	server := quotaservice.New(redis.NewBucketFactory(&redisclient.Options{Addr: "localhost:6379"}, 2, "qqs"),
		config.NewMemoryConfig(cfg),
		r,
		0,
		qsgrpc.New(addr))

	// Set buffered chan size
	server.SetListener(nil, p)

	qs.Srv = server
}

func main() {
	var o Options
	parser := flags.NewParser(&o, flags.Default)
	if _, err := parser.Parse(); err != nil {
		os.Exit(1)
	}

	qs := &QQServer{}
	qs.Init(o.ServerAddr, int64(o.Cap), int64(o.FillRate), int64(o.RejectThreshold), o.Projects)

	if _, err := qs.Srv.Start(); err != nil {
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

	<-ctx.Done()
	qs.Srv.Stop()
	qqs.Log("Server closed")
}
