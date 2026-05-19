MODULE  := gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GOFLAGS := -trimpath

BINS := server agent cli
OUT  := bin
DASHBOARD_DIR := web/dashboard

DEPLOY_SERVER := redis01
DEPLOY_AGENTS := redis01 redis02 redis03

.PHONY: all clean dashboard $(BINS) xds build-linux deploy deploy-server deploy-agent deploy-cli setup

all: $(BINS) xds

server: dashboard

dashboard:
	cd $(DASHBOARD_DIR) && npm install
	cd $(DASHBOARD_DIR) && npm run build

$(BINS):
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(OUT)/redis-$@ ./cmd/$@

xds:
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(OUT)/redis-pilot-xds ./cmd/xds

clean:
	rm -rf $(OUT)

build-linux:
	GOOS=linux GOARCH=amd64 $(MAKE) all

deploy: build-linux deploy-server deploy-agent deploy-cli deploy-xds

deploy-server:
	ssh $(DEPLOY_SERVER) systemctl stop redis-pilot-server || true
	scp bin/redis-server $(DEPLOY_SERVER):/opt/redis-pilot-server/redis-pilot-server
	ssh $(DEPLOY_SERVER) systemctl start redis-pilot-server

deploy-agent:
	@for h in $(DEPLOY_AGENTS); do \
		echo "→ agent $$h"; \
		ssh $$h systemctl stop redis-pilot-agent || true; \
		scp bin/redis-agent $$h:/opt/redis-pilot-agent/redis-pilot-agent; \
		ssh $$h systemctl start redis-pilot-agent; \
	done

deploy-cli:
	@for h in $(DEPLOY_AGENTS); do \
		echo "→ cli $$h"; \
		scp bin/redis-cli $$h:/usr/local/bin/redis-pilot-cli; \
	done

deploy-xds:
	ssh $(DEPLOY_SERVER) systemctl stop redis-pilot-xds || true
	scp bin/redis-pilot-xds $(DEPLOY_SERVER):/opt/redis-pilot-xds/redis-pilot-xds
	ssh $(DEPLOY_SERVER) systemctl start redis-pilot-xds

setup:
	bash scripts/setup.sh
