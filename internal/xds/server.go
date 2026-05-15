package xds

import (
	"context"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	clusterservice "github.com/envoyproxy/go-control-plane/envoy/service/cluster/v3"
	discoverygrpc "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	endpointservice "github.com/envoyproxy/go-control-plane/envoy/service/endpoint/v3"
	listenerservice "github.com/envoyproxy/go-control-plane/envoy/service/listener/v3"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	xdsserver "github.com/envoyproxy/go-control-plane/pkg/server/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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
