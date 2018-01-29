package client

import (
	"context"

	"github.com/mian-qin/qqs/quotaservice/protos"
	"google.golang.org/grpc"
)

// Admin is a QuotaService Admin class, simple wrapper encapsulating grpc connection
type Admin struct {
	cc       *grpc.ClientConn
	qsClient quotaservice.QuotaServiceClient
}

// GetInfo invokes "GetInfo()" on the quotaservice, taking in a raw InfoRequest message and
// returning the raw InfoResponse message, and optionally any error encountered.
func (c *Admin) GetInfo(ctx context.Context, request *quotaservice.InfoRequest) (*quotaservice.InfoResponse, error) {
	return c.qsClient.GetInfo(ctx, request)
}

// Update invokes "Update()" on the quotaservice, taking in a raw UpdateRequest message and
// returning the raw UpdateResponse message, and optionally any error encountered.
func (c *Admin) Update(ctx context.Context, request *quotaservice.UpdateRequest) (*quotaservice.UpdateResponse, error) {
	return c.qsClient.Update(ctx, request)
}

// Close releases any resources associated with Admin connections.
func (c *Admin) Close() error {
	return c.cc.Close()
}

// NewAdmin creates a simple new Admin, connected to a single server. opts can be used to set a
// grpc.Balancer, name resolver, etc. See
// https://godoc.org/google.golang.org/grpc#DialOption for more details.
func NewAdmin(target string, opts ...grpc.DialOption) (*Admin, error) {
	conn, err := grpc.Dial(target, opts...)
	if err != nil {
		return nil, err
	}

	return &Admin{conn, quotaservice.NewQuotaServiceClient(conn)}, nil
}

// NewAdminWithContext creates a new Admin with context
func NewAdminWithContext(ctx context.Context, target string, opts ...grpc.DialOption) (*Admin, error) {
	conn, err := grpc.DialContext(ctx, target, opts...)
	if err != nil {
		return nil, err
	}

	return &Admin{conn, quotaservice.NewQuotaServiceClient(conn)}, nil
}
