// Licensed under the Apache License, Version 2.0
// Details: https://raw.githubusercontent.com/square/quotaservice/master/LICENSE

package grpc

import (
	"context"
	"fmt"
	"net"
	"strings"

	"time"

	"github.com/mian-qin/qqs/quotaservice"
	"github.com/mian-qin/qqs/quotaservice/lifecycle"
	"github.com/mian-qin/qqs/quotaservice/logging"
	pb "github.com/mian-qin/qqs/quotaservice/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

type GrpcEndpoint struct {
	hostport      string
	grpcServer    *grpc.Server
	currentStatus lifecycle.Status
	qs            quotaservice.QuotaService
}

// New creates a new GrpcEndpoint, listening on hostport. Hostport is a string in the form
// "host:port"
func New(hostport string) *GrpcEndpoint {
	if !strings.Contains(hostport, ":") {
		panic(fmt.Sprintf("hostport should be in the format 'host:port', but is currently %v",
			hostport))
	}
	return &GrpcEndpoint{hostport: hostport}
}

func (g *GrpcEndpoint) Init(qs quotaservice.QuotaService) {
	g.qs = qs
}

func (g *GrpcEndpoint) Start() {
	lis, err := net.Listen("tcp", g.hostport)
	if err != nil {
		logging.Fatalf("Cannot start server on port %v. Error %v", g.hostport, err)
	}

	grpclog.SetLogger(logging.CurrentLogger())
	g.grpcServer = grpc.NewServer()
	// Each service should be registered
	pb.RegisterQuotaServiceServer(g.grpcServer, g)
	go func() {
		if e := g.grpcServer.Serve(lis); e != nil {
			logging.Fatalf("Cannot start gRPC server. Error %v", e)
		}
	}()
	g.currentStatus = lifecycle.Started
	logging.Printf("Starting server on %v", g.hostport)
	logging.Printf("Server status: %v", g.currentStatus)
}

func (g *GrpcEndpoint) Stop() {
	g.currentStatus = lifecycle.Stopped
}

func (g *GrpcEndpoint) Allow(ctx context.Context, req *pb.AllowRequest) (*pb.AllowResponse, error) {
	rsp := new(pb.AllowResponse)
	if invalid(req) {
		logging.Printf("Invalid request %+v", req)
		rsp.Status = pb.AllowResponse_REJECTED_INVALID_REQUEST
		return rsp, nil
	}

	var tokensRequested int64 = 1
	if req.TokensRequested > 0 {
		tokensRequested = req.TokensRequested
	}

	wait, _, err := g.qs.Allow(req.Namespace, req.BucketName, tokensRequested, req.MaxWaitMillisOverride, req.MaxWaitTimeOverride)

	if err != nil {
		if qsErr, ok := err.(quotaservice.QuotaServiceError); ok {
			rsp.Status = toPBStatus(qsErr)
		} else {
			logging.Printf("Caught error %v", err)
			rsp.Status = pb.AllowResponse_REJECTED_SERVER_ERROR
		}
	} else {
		rsp.Status = pb.AllowResponse_OK
		rsp.TokensGranted = req.TokensRequested
		rsp.WaitMillis = wait.Nanoseconds() / int64(time.Millisecond)
	}

	return rsp, nil
}

// Update is the endpoint for updating a quota service bucket config
func (g *GrpcEndpoint) Update(ctx context.Context, req *pb.UpdateRequest) (*pb.UpdateResponse, error) {
	rsp := &pb.UpdateResponse{}
	if !validUpdateReq(req) {
		logging.Printf("Invalid request %+v", req)
		rsp.Status = pb.UpdateResponse_REJECTED_INVALID_REQUEST
		return rsp, nil
	}

	err := g.qs.Update(req.Namespace, req.BucketName, req.Size, req.FillRate, req.WaitTimeoutMillis)
	if err != nil {
		if qsErr, ok := err.(quotaservice.QuotaServiceError); ok {
			rsp.Status = toPBStatusUpdate(qsErr)
		} else {
			logging.Printf("Internal error %v", err)
			rsp.Status = pb.UpdateResponse_REJECTED_SERVER_ERROR
		}
	} else {
		rsp.Status = pb.UpdateResponse_OK
	}

	return rsp, err
}

// GetInfo is the endpoint for getting information of a bucket
func (g *GrpcEndpoint) GetInfo(ctx context.Context, req *pb.InfoRequest) (*pb.InfoResponse, error) {
	rsp := &pb.InfoResponse{}

	if !validInfoReq(req) {
		logging.Printf("Invalid request %+v", req)
		rsp.Status = pb.InfoResponse_REJECTED_INVALID_REQUEST
		return rsp, nil
	}

	var err error
	rsp.Size, rsp.FillRate, rsp.WaitTimeoutMillis, err = g.qs.GetInfo(req.Namespace, req.BucketName)
	if err != nil {
		if qsErr, ok := err.(quotaservice.QuotaServiceError); ok {
			rsp.Status = toPBStatusInfo(qsErr)
		} else {
			logging.Printf("Internal error %v", err)
			rsp.Status = pb.InfoResponse_REJECTED_SERVER_ERROR
		}
	} else {
		rsp.Status = pb.InfoResponse_OK
	}

	return rsp, err
}

func invalid(req *pb.AllowRequest) bool {
	return req != nil && (req.BucketName == "" || req.Namespace == "")
}

func validUpdateReq(req *pb.UpdateRequest) bool {
	return req != nil && req.BucketName != "" && req.Namespace != ""
}

func validInfoReq(req *pb.InfoRequest) bool {
	return req != nil && req.BucketName != "" && req.Namespace != ""
}

func toPBStatus(qsErr quotaservice.QuotaServiceError) (r pb.AllowResponse_Status) {
	switch qsErr.Reason {
	case quotaservice.ER_NO_BUCKET:
		r = pb.AllowResponse_REJECTED_NO_BUCKET
	case quotaservice.ER_TOO_MANY_BUCKETS:
		r = pb.AllowResponse_REJECTED_TOO_MANY_BUCKETS
	case quotaservice.ER_TOO_MANY_TOKENS_REQUESTED:
		r = pb.AllowResponse_REJECTED_TOO_MANY_TOKENS_REQUESTED
	case quotaservice.ER_TIMEOUT:
		r = pb.AllowResponse_REJECTED_TIMEOUT
	default:
		r = pb.AllowResponse_REJECTED_SERVER_ERROR
	}

	return
}

func toPBStatusUpdate(qsErr quotaservice.QuotaServiceError) (r pb.UpdateResponse_Status) {
	switch qsErr.Reason {
	case quotaservice.ER_TIMEOUT:
		r = pb.UpdateResponse_REJECTED_TIMEOUT
	default:
		r = pb.UpdateResponse_REJECTED_SERVER_ERROR
	}

	return
}

func toPBStatusInfo(qsErr quotaservice.QuotaServiceError) (r pb.InfoResponse_Status) {
	switch qsErr.Reason {
	case quotaservice.ER_NO_BUCKET:
		r = pb.InfoResponse_REJECTED_NO_BUCKET
	case quotaservice.ER_TIMEOUT:
		r = pb.InfoResponse_REJECTED_TIMEOUT
	default:
		r = pb.InfoResponse_REJECTED_SERVER_ERROR
	}

	return
}
