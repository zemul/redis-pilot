MODULE  := gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GOFLAGS := -trimpath

BINS := server agent cli
OUT  := bin

.PHONY: all clean $(BINS)

all: $(BINS)

$(BINS):
	go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(OUT)/redis-$@ ./cmd/$@

clean:
	rm -rf $(OUT)
