package qqs

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	qsclient "github.com/square/quotaservice/client"
	"github.com/square/quotaservice/experiment/qqs/util"
	pb "github.com/square/quotaservice/protos"
	"google.golang.org/grpc"
)

// QQClient is the query quota service client
type QQClient struct {
	cl *qsclient.Client
}

// NewClient creates a query quota client with context for connection
func NewClient(ctx context.Context, addr string) (*QQClient, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("Context cancelled")
	default:
		client, err := qsclient.NewWithContext(ctx, addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		qc := &QQClient{
			cl: client,
		}

		return qc, nil
	}
}

// Close shuts down the connection
func (qc *QQClient) Close() {
	if qc != nil && qc.cl != nil {
		qc.cl.Close()
	}
}

// Allow invokes the quotaservice Allow methods with context
// Returns granted_tokens, wait_ms and error
func (qc *QQClient) Allow(ctx context.Context, blocking bool, ns, bucket string, tokens, maxWaitMs int64) (int64, int64, error) {
	req := &pb.AllowRequest{
		Namespace:       ns,
		BucketName:      bucket,
		TokensRequested: tokens,
		// MaxWaitTimeOverride:   true,
		MaxWaitMillisOverride: maxWaitMs,
	}

	select {
	case <-ctx.Done():
		return 0, 0, fmt.Errorf("Context cancelled before quota query starts")
	default:
		if blocking {
			err := qc.cl.AllowBlockingWithContext(ctx, req)
			if err != nil {
				return 0, 0, err
			}

			return tokens, 0, nil
		}

		resp, err := qc.cl.AllowWithContext(ctx, req)
		if err != nil {
			return 0, 0, err
		}

		return resp.TokensGranted, resp.WaitMillis, nil
	}
}

type AskResult struct {
	Granted int64
	WaitMs  int64
	Error   error
}

// AskForQuota sends quota query request with client timeout
func AskForQuota(ctx context.Context, cl *QQClient, cap int64, blocking bool, name, p string, to time.Duration) AskResult {
	rctx, rcancel := context.WithTimeout(ctx, to)
	defer rcancel()

	t := rand.Int63n(cap) + 1
	qqsutil.Log("#%s requesting %d token(s) for project %s\n", name, t, p)
	result := make(chan AskResult)
	after := time.After(to)
	go func() {
		gr, wait, err := cl.Allow(rctx,
			blocking,
			"project",
			p,
			t,
			20*1000,
		)

		result <- AskResult{
			Granted: gr,
			WaitMs:  wait,
			Error:   err,
		}
	}()

	select {
	case r := <-result:
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
