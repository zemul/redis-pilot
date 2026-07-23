package xds

import (
	"context"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenerservice "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	xdsserver "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Service struct {
	cfg         *Config
	cache       cache.SnapshotCache
	client      *serverClient
	lastVersion string
	mu          sync.Mutex
}

func NewService(cfg *Config) *Service {
	httpClient := &http.Client{Timeout: cfg.Poll.Timeout}
	return &Service{
		cfg:    cfg,
		cache:  cache.NewSnapshotCache(false, cache.IDHash{}, nil),
		client: newServerClient(cfg.Server, httpClient),
	}
}

func (s *Service) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.serveGRPC(ctx)
	}()
	go s.poll(ctx)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (s *Service) serveGRPC(ctx context.Context) error {
	grpcServer := grpc.NewServer(
		grpc.MaxConcurrentStreams(1000000),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 5 * time.Second,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(s.authUnaryInterceptor),
		grpc.StreamInterceptor(s.authStreamInterceptor),
	)

	server := xdsserver.NewServer(ctx, s.cache, nil)
	discoverygrpc.RegisterAggregatedDiscoveryServiceServer(grpcServer, server)
	endpointservice.RegisterEndpointDiscoveryServiceServer(grpcServer, server)
	clusterservice.RegisterClusterDiscoveryServiceServer(grpcServer, server)
	listenerservice.RegisterListenerDiscoveryServiceServer(grpcServer, server)

	lis, err := net.Listen("tcp", s.cfg.Listen)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
	}()

	log.Printf("redis-pilot-xds listening on %s", s.cfg.Listen)
	return grpcServer.Serve(lis)
}

// checkAuth 校验 gRPC 请求 metadata 中的 "authorization: Bearer <token>"。
// AuthToken 为空则跳过鉴权，与 Server/Agent 的 Bearer Token 约定保持一致。
//
// xDS snapshot 中包含明文 Redis 密码（见 translator.go），任何能连接 :18000 的
// gRPC 客户端在无鉴权情况下都可拉取全部密码，因此这里需要与 HTTP API 同等的
// token 校验（S7）。
func (s *Service) checkAuth(ctx context.Context) error {
	if s.cfg.AuthToken == "" {
		return nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return status.Error(codes.Unauthenticated, "missing authorization header")
	}
	token := strings.TrimPrefix(values[0], "Bearer ")
	if token != s.cfg.AuthToken {
		return status.Error(codes.Unauthenticated, "invalid token")
	}
	return nil
}

// authUnaryInterceptor 拦截普通 gRPC 请求。
func (s *Service) authUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	if err := s.checkAuth(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

// authStreamInterceptor 拦截 ADS 使用的双向流式 gRPC 请求（StreamAggregatedResources 等）。
func (s *Service) authStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	if err := s.checkAuth(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

func (s *Service) poll(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Poll.Interval)
	defer ticker.Stop()

	s.refresh(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.refresh(ctx)
		}
	}
}

func (s *Service) refresh(ctx context.Context) {
	reqCtx, cancel := context.WithTimeout(ctx, s.cfg.Poll.Timeout)
	defer cancel()

	proxySnapshot, err := s.client.FetchProxySnapshot(reqCtx)
	if err != nil {
		log.Printf("poll proxy snapshot failed: %v", err)
		return
	}

	s.mu.Lock()
	if proxySnapshot.Version == s.lastVersion {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	xdsSnapshot, err := buildSnapshot(proxySnapshot)
	if err != nil {
		log.Printf("build xDS snapshot %s failed: %v", proxySnapshot.Version, err)
		return
	}

	for _, nodeID := range s.cfg.Envoy.NodeIDs {
		if err := s.cache.SetSnapshot(ctx, nodeID, xdsSnapshot); err != nil {
			log.Printf("set xDS snapshot for node %s failed: %v", nodeID, err)
			return
		}
	}

	s.mu.Lock()
	s.lastVersion = proxySnapshot.Version
	s.mu.Unlock()
	log.Printf("published xDS snapshot version=%s listeners=%d clusters=%d nodes=%d",
		proxySnapshot.Version, len(proxySnapshot.Listeners), len(proxySnapshot.Clusters), len(s.cfg.Envoy.NodeIDs))
}
