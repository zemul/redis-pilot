package xds

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestCheckAuth_NoTokenConfigured 验证 auth_token 为空时跳过鉴权，
// 与 Server/Agent 的 Bearer Token 约定保持一致。
func TestCheckAuth_NoTokenConfigured(t *testing.T) {
	s := &Service{cfg: &Config{AuthToken: ""}}

	if err := s.checkAuth(context.Background()); err != nil {
		t.Fatalf("checkAuth() with empty AuthToken = %v, want nil", err)
	}

	// 即使请求完全没有 metadata，也应该放行。
	md := metadata.New(map[string]string{"authorization": "Bearer wrong"})
	ctx := metadata.NewIncomingContext(context.Background(), md)
	if err := s.checkAuth(ctx); err != nil {
		t.Fatalf("checkAuth() with empty AuthToken and metadata present = %v, want nil", err)
	}
}

func TestCheckAuth_MissingMetadata(t *testing.T) {
	s := &Service{cfg: &Config{AuthToken: "secret"}}

	err := s.checkAuth(context.Background())
	if err == nil {
		t.Fatal("checkAuth() with no metadata = nil, want error")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("checkAuth() code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestCheckAuth_MissingAuthorizationHeader(t *testing.T) {
	s := &Service{cfg: &Config{AuthToken: "secret"}}

	md := metadata.New(map[string]string{"x-other": "value"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := s.checkAuth(ctx)
	if err == nil {
		t.Fatal("checkAuth() with missing authorization header = nil, want error")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("checkAuth() code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestCheckAuth_WrongToken(t *testing.T) {
	s := &Service{cfg: &Config{AuthToken: "secret"}}

	md := metadata.New(map[string]string{"authorization": "Bearer wrong"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	err := s.checkAuth(ctx)
	if err == nil {
		t.Fatal("checkAuth() with wrong token = nil, want error")
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Errorf("checkAuth() code = %v, want Unauthenticated", status.Code(err))
	}
}

func TestCheckAuth_CorrectToken(t *testing.T) {
	s := &Service{cfg: &Config{AuthToken: "secret"}}

	md := metadata.New(map[string]string{"authorization": "Bearer secret"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	if err := s.checkAuth(ctx); err != nil {
		t.Fatalf("checkAuth() with correct token = %v, want nil", err)
	}
}
