package xds

import (
	"fmt"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	redisproxy "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/redis_proxy/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	cache "github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"github.com/envoyproxy/go-control-plane/pkg/wellknown"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/pkg/apitypes"
)

func buildSnapshot(snapshot *apitypes.ProxySnapshot) (*cache.Snapshot, error) {
	clusters := make([]types.Resource, 0, len(snapshot.Clusters))
	endpoints := make([]types.Resource, 0, len(snapshot.Clusters))
	listeners := make([]types.Resource, 0, len(snapshot.Listeners))

	for _, c := range snapshot.Clusters {
		clusters = append(clusters, makeCluster(c))
		endpoints = append(endpoints, makeEndpoint(c))
	}
	for _, l := range snapshot.Listeners {
		resource, err := makeListener(l)
		if err != nil {
			return nil, err
		}
		listeners = append(listeners, resource)
	}

	xdsSnapshot, err := cache.NewSnapshot(snapshot.Version, map[resource.Type][]types.Resource{
		resource.ClusterType:  clusters,
		resource.EndpointType: endpoints,
		resource.ListenerType: listeners,
	})
	if err != nil {
		return nil, err
	}
	if err := xdsSnapshot.Consistent(); err != nil {
		return nil, err
	}
	return xdsSnapshot, nil
}

func makeCluster(c apitypes.ProxyCluster) *cluster.Cluster {
	out := &cluster.Cluster{
		Name: c.Name,
		ClusterDiscoveryType: &cluster.Cluster_Type{
			Type: cluster.Cluster_EDS,
		},
		EdsClusterConfig: &cluster.Cluster_EdsClusterConfig{
			EdsConfig: adsConfigSource(),
		},
		ConnectTimeout: durationpb.New(1 * time.Second),
		LbPolicy:       cluster.Cluster_ROUND_ROBIN,
	}
	if c.Password != "" {
		protocolOptions, _ := anypb.New(&redisproxy.RedisProtocolOptions{
			AuthPassword: inlineString(c.Password),
		})
		out.TypedExtensionProtocolOptions = map[string]*anypb.Any{
			wellknown.RedisProxy: protocolOptions,
		}
	}
	return out
}

func makeEndpoint(c apitypes.ProxyCluster) *endpoint.ClusterLoadAssignment {
	lbEndpoints := make([]*endpoint.LbEndpoint, 0, len(c.Endpoints))
	for _, ep := range c.Endpoints {
		lbEndpoints = append(lbEndpoints, &endpoint.LbEndpoint{
			HostIdentifier: &endpoint.LbEndpoint_Endpoint{
				Endpoint: &endpoint.Endpoint{
					Address: socketAddress(ep.Address, uint32(ep.Port)),
				},
			},
		})
	}
	return &endpoint.ClusterLoadAssignment{
		ClusterName: c.Name,
		Endpoints: []*endpoint.LocalityLbEndpoints{{
			LbEndpoints: lbEndpoints,
		}},
	}
}

func makeListener(l apitypes.ProxyListener) (*listener.Listener, error) {
	route := &redisproxy.RedisProxy_PrefixRoutes_Route{Cluster: l.Cluster}
	if l.ReadCluster != "" {
		route.ReadCommandPolicy = &redisproxy.RedisProxy_PrefixRoutes_Route_ReadCommandPolicy{
			Cluster: l.ReadCluster,
		}
	}

	filterConfig := &redisproxy.RedisProxy{
		StatPrefix: l.StatPrefix,
		Settings: &redisproxy.RedisProxy_ConnPoolSettings{
			OpTimeout:  durationpb.New(5 * time.Second),
			ReadPolicy: redisproxy.RedisProxy_ConnPoolSettings_MASTER,
		},
		PrefixRoutes: &redisproxy.RedisProxy_PrefixRoutes{
			CatchAllRoute: route,
		},
	}
	if l.Password != "" {
		filterConfig.DownstreamAuthPasswords = []*core.DataSource{inlineString(l.Password)}
	}

	typedConfig, err := anypb.New(filterConfig)
	if err != nil {
		return nil, fmt.Errorf("build redis proxy filter %s: %w", l.Name, err)
	}

	return &listener.Listener{
		Name:    l.Name,
		Address: socketAddress(l.Bind, uint32(l.Port)),
		FilterChains: []*listener.FilterChain{{
			Filters: []*listener.Filter{{
				Name: wellknown.RedisProxy,
				ConfigType: &listener.Filter_TypedConfig{
					TypedConfig: typedConfig,
				},
			}},
		}},
	}, nil
}

func socketAddress(address string, port uint32) *core.Address {
	return &core.Address{
		Address: &core.Address_SocketAddress{
			SocketAddress: &core.SocketAddress{
				Protocol: core.SocketAddress_TCP,
				Address:  address,
				PortSpecifier: &core.SocketAddress_PortValue{
					PortValue: port,
				},
			},
		},
	}
}

func adsConfigSource() *core.ConfigSource {
	return &core.ConfigSource{
		ResourceApiVersion: resource.DefaultAPIVersion,
		ConfigSourceSpecifier: &core.ConfigSource_Ads{
			Ads: &core.AggregatedConfigSource{},
		},
	}
}

func inlineString(value string) *core.DataSource {
	return &core.DataSource{
		Specifier: &core.DataSource_InlineString{
			InlineString: value,
		},
	}
}
