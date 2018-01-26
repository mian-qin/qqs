// This is the admin command client to retrieve bucket information and update bucket configs

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/square/quotaservice/experiment/qqs"
	"github.com/square/quotaservice/experiment/qqs/util"

	flags "github.com/jessevdk/go-flags"
)

const ClientTimeout = time.Hour

type Options struct {
	ServerAddr string `short:"a" long:"server-address" description:"server address" default:"localhost:10990"`
	Timeout    int64  `short:"t" long:"timeout" description:"request timeout in ms" default:"1000"`

	Namespace string `short:"n" long:"namespace" required:"true" description:"namespace name" default:""`
	Bucket    string `short:"b" long:"bucket" required:"true" description:"bucket name" default:""`
	Operation string `short:"o" long:"operation" description:"get|set" default:"get"`

	Cap               int64 `short:"m" long:"max" description:"max quota for a bucket (Only for set)" default:"0"`
	FillRate          int64 `short:"f" long:"fill_rate" description:"fill rate for a bucket (Only for set)" default:"0"`
	WaitTimeoutMillis int64 `short:"w" long:"wait_timeout" description:"wait timeout in ms for a bucket (Only for set)" default:"0"`
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

	switch o.Operation {
	case "get":
		r := GetInfo(ctx, client, o.Namespace, o.Bucket, time.Duration(o.Timeout)*time.Millisecond)
		if r.Error != nil {
			qqsutil.LogError(r.Error)
		} else {
			qqsutil.Log("[%s/%s]: max_quota=%d, fill_rate=%d, wait_timeout_ms=%d", o.Namespace, o.Bucket, r.Size, r.FillRate, r.WaitTimeoutMillis)
		}
	case "set":
		r := SetConfig(ctx, client, o.Namespace, o.Bucket, o.Cap, o.FillRate, o.WaitTimeoutMillis, time.Duration(o.Timeout)*time.Millisecond)
		if r.Error != nil {
			qqsutil.LogError(r.Error)
		} else {
			qqsutil.Log("[%s/%s]: max_quota=%d, fill_rate=%d, wait_timeout_ms=%d", o.Namespace, o.Bucket, r.Size, r.FillRate, r.WaitTimeoutMillis)
		}
	default:
		qqsutil.Log("Unrecognized operation")
		return
	}

	qqsutil.Log("Admin operation complete")
}

type BucketParams struct {
	Size              int64
	FillRate          int64
	WaitTimeoutMillis int64
	Error             error
}

func GetInfo(ctx context.Context, cl *qqs.QQClient, ns, name string, to time.Duration) BucketParams {
	rctx, rcancel := context.WithTimeout(ctx, to)
	defer rcancel()

	qqsutil.Log("Getting bucket information for %s/%s", ns, name)
	rs := make(chan BucketParams)
	if to <= 0 {
		to = ClientTimeout
	}
	after := time.After(to)
	go func() {
		sz, fr, wt, err := cl.GetInfo(
			rctx,
			ns,
			name,
		)

		r := BucketParams{
			Size:              sz,
			FillRate:          fr,
			WaitTimeoutMillis: wt,
			Error:             err,
		}

		rs <- r
	}()

	select {
	case r := <-rs:
		return r
	case <-after:
		return BucketParams{
			Error: fmt.Errorf("Admin command call timed out"),
		}
	}
}

func SetConfig(ctx context.Context, cl *qqs.QQClient, ns, name string, size, fillrate, waitms int64, to time.Duration) BucketParams {
	rctx, rcancel := context.WithTimeout(ctx, to)
	defer rcancel()

	qqsutil.Log("Setting bucket parameters for %s/%s", ns, name)
	rs := make(chan BucketParams)
	if to <= 0 {
		to = ClientTimeout
	}
	after := time.After(to)
	go func() {
		err := cl.SetBucket(
			rctx,
			ns,
			name,
			size,
			fillrate,
			waitms,
		)

		if err != nil {
			rs <- BucketParams{
				Error: err,
			}

			return
		}

		sz, fr, wt, err := cl.GetInfo(
			rctx,
			ns,
			name,
		)

		r := BucketParams{
			Size:              sz,
			FillRate:          fr,
			WaitTimeoutMillis: wt,
			Error:             err,
		}

		rs <- r
	}()

	select {
	case r := <-rs:
		return r
	case <-after:
		return BucketParams{
			Error: fmt.Errorf("Admin command call timed out"),
		}
	}
}
