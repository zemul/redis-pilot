MODULE  := gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GOFLAGS := -trimpath

BINS := server agent cli proxy
OUT  := bin
DASHBOARD_DIR := web/dashboard
SKILLS_EMBED := internal/server/ai/skills_embed

DEPLOY_SERVER := redis01
DEPLOY_AGENTS := redis01 redis02 redis03

.PHONY: all clean dashboard skills-embed $(BINS) build-linux deploy deploy-server deploy-agent deploy-cli deploy-proxy setup

all: $(BINS)

server: dashboard skills-embed

dashboard:
	cd $(DASHBOARD_DIR) && npm install
	cd $(DASHBOARD_DIR) && npm run build

# skills-embed 把仓库根 skills/ 同步到 ai 包内，供 go:embed 打进 server 二进制
# （go:embed 不能跨目录引用 ../../../skills）。见 docs/architecture/ai-assistant.md §3.4。
skills-embed:
	rm -rf $(SKILLS_EMBED)
	mkdir -p $(SKILLS_EMBED)
	cp -R skills/. $(SKILLS_EMBED)/

$(BINS):
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(OUT)/redis-pilot-$@ ./cmd/$@

clean:
	rm -rf $(OUT)

build-linux:
	GOOS=linux GOARCH=amd64 $(MAKE) all

deploy: build-linux deploy-server deploy-agent deploy-cli deploy-proxy

deploy-server:
	ssh $(DEPLOY_SERVER) systemctl stop redis-pilot-server || true
	scp bin/redis-pilot-server $(DEPLOY_SERVER):/opt/redis-pilot-server/redis-pilot-server
	ssh $(DEPLOY_SERVER) systemctl daemon-reload
	ssh $(DEPLOY_SERVER) systemctl start redis-pilot-server

deploy-agent:
	@for h in $(DEPLOY_AGENTS); do \
		echo "→ agent $$h"; \
		ssh $$h systemctl stop redis-pilot-agent || true; \
		scp bin/redis-pilot-agent $$h:/opt/redis-pilot-agent/redis-pilot-agent; \
		ssh $$h systemctl daemon-reload; \
		ssh $$h systemctl start redis-pilot-agent; \
	done

deploy-cli:
	@for h in $(DEPLOY_AGENTS); do \
		echo "→ cli $$h"; \
		scp bin/redis-pilot-cli $$h:/usr/local/bin/redis-pilot-cli; \
	done

deploy-proxy:
	ssh $(DEPLOY_SERVER) systemctl stop redis-pilot-proxy || true
	scp bin/redis-pilot-proxy $(DEPLOY_SERVER):/opt/redis-pilot-proxy/redis-pilot-proxy
	ssh $(DEPLOY_SERVER) systemctl daemon-reload
	ssh $(DEPLOY_SERVER) systemctl start redis-pilot-proxy

setup:
	bash scripts/setup.sh
