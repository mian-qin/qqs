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
	a  *qsclient.Admin
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
		admin, err := qsclient.NewAdminWithContext(ctx, addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		qc := &QQClient{
			cl: client,
			a:  admin,
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

// Allow invokes the quotaservice Allow method with context
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

// GetInfo invokes the quotaservice GetInfo method with context
// Returns size, fill_rate, wait_timeout_ms and error
func (qc *QQClient) GetInfo(ctx context.Context, ns, bucket string) (int64, int64, int64, error) {
	req := &pb.InfoRequest{
		Namespace:  ns,
		BucketName: bucket,
	}

	select {
	case <-ctx.Done():
		return 0, 0, 0, fmt.Errorf("Context cancelled before admin command starts")
	default:
		resp, err := qc.a.GetInfo(ctx, req)
		if err != nil {
			return 0, 0, 0, err
		}

		return resp.Size, resp.FillRate, resp.WaitTimeoutMillis, nil
	}
}

// SetBucket invokes the quotaservice Update method with context
// Returns any error
func (qc *QQClient) SetBucket(ctx context.Context, ns, bucket string, sz, fr, wt int64) error {
	req := &pb.UpdateRequest{
		Namespace:         ns,
		BucketName:        bucket,
		Size:              sz,
		FillRate:          fr,
		WaitTimeoutMillis: wt,
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("Context cancelled before admin command starts")
	default:
		_, err := qc.a.Update(ctx, req)
		return err
	}
}
