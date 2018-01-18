package qqs

import (
	"context"
	"fmt"

	qsclient "github.com/square/quotaservice/client"
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
		return 0, 0, fmt.Errorf("Context cancelled")
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
