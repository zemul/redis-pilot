package xds

import (
	"context"
	"crypto/subtle"
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
	// 刻意不设 http.Client.Timeout：它会连长轮询的正常挂起一起掐断。
	// 每次请求的超时由 refresh 用 context 按 Wait+Timeout 单独设定。
	httpClient := &http.Client{}
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
	// 用 subtle.ConstantTimeCompare 而非 !=：字符串比较在首个不同字节处短路，
	// 耗时随匹配前缀长度变化。这条路径的可利用性很低（网络抖动远大于纳秒级差异），
	// 改动成本也极低，按纵深防御处理。
	if subtle.ConstantTimeCompare([]byte(token), []byte(s.cfg.AuthToken)) != 1 {
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

// poll 持续从 Server 拉取代理快照并下发给 Envoy。
//
// 两种模式由 Poll.Wait 决定：
//   - Wait > 0：长轮询。Server 在版本未变化时挂起请求，变化后立即返回，
//     故障转移传播到 Envoy 几乎没有延迟。
//   - Wait <= 0：按 Poll.Interval 定期轮询（原有行为）。
//
// 无论哪种模式，两次请求之间都至少间隔 Poll.Interval（除非确实拿到了新版本）。
// 这道下限是必需的：Server 若不认识 known_version/wait 参数会立即返回相同版本的完整
// 快照，没有下限就会变成一个不断打 Server 的忙循环。
func (s *Service) poll(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		start := time.Now()
		changed, err := s.refresh(ctx)

		switch {
		case err != nil:
			// 出错时退避，避免 Server 不可用期间被打成忙循环
			if !sleepCtx(ctx, s.cfg.Poll.Interval) {
				return
			}
		case changed:
			// 拿到新版本，立刻检查下一个
		default:
			// 版本没变：补足到 Poll.Interval 再重试
			if remaining := s.cfg.Poll.Interval - time.Since(start); remaining > 0 {
				if !sleepCtx(ctx, remaining) {
					return
				}
			}
		}
	}
}

// sleepCtx 睡 d，被 ctx 取消时返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// refresh 拉取一次快照，必要时下发给 Envoy。
// 返回 changed 表示是否拿到了与当前不同的版本。
func (s *Service) refresh(ctx context.Context) (bool, error) {
	s.mu.Lock()
	knownVersion := s.lastVersion
	s.mu.Unlock()

	wait := s.cfg.Poll.Wait
	// 首次还没有版本可比，直接取一次，不进入长轮询
	if knownVersion == "" {
		wait = 0
	}

	// 请求超时必须覆盖服务端的挂起时长，否则长轮询会被自己的超时掐断。
	// Poll.Timeout 在这里的含义是"网络往返的余量"。
	reqCtx, cancel := context.WithTimeout(ctx, wait+s.cfg.Poll.Timeout)
	defer cancel()

	proxySnapshot, unchanged, err := s.client.FetchProxySnapshot(reqCtx, knownVersion, wait)
	if err != nil {
		log.Printf("poll proxy snapshot failed: %v", err)
		return false, err
	}
	if unchanged {
		return false, nil
	}
	if proxySnapshot.Version == knownVersion {
		return false, nil
	}

	xdsSnapshot, err := buildSnapshot(proxySnapshot)
	if err != nil {
		log.Printf("build xDS snapshot %s failed: %v", proxySnapshot.Version, err)
		return false, err
	}

	for _, nodeID := range s.cfg.Envoy.NodeIDs {
		if err := s.cache.SetSnapshot(ctx, nodeID, xdsSnapshot); err != nil {
			log.Printf("set xDS snapshot for node %s failed: %v", nodeID, err)
			return false, err
		}
	}

	s.mu.Lock()
	s.lastVersion = proxySnapshot.Version
	s.mu.Unlock()
	log.Printf("published xDS snapshot version=%s listeners=%d clusters=%d nodes=%d",
		proxySnapshot.Version, len(proxySnapshot.Listeners), len(proxySnapshot.Clusters), len(s.cfg.Envoy.NodeIDs))
	return true, nil
}
