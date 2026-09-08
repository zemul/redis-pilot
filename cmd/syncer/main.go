// redis-syncer runs isolated Redis-to-Kvrocks synchronization tasks.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"gitlab.dev.ihuman.com/ihuman-infrastructure/dev/galaxy/common/redis-pilot/internal/syncer"
)

var version = "dev"

func main() {
	os.Exit(mainWithSignals())
}

func mainWithSignals() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, os.Args[1:], os.Stderr)
}

func run(ctx context.Context, args []string, output io.Writer) int {
	logger := slog.New(slog.NewTextHandler(output, nil))
	flags := flag.NewFlagSet("redis-syncer", flag.ContinueOnError)
	// flag's default errors echo arbitrary arguments. Only emit fixed messages.
	flags.SetOutput(io.Discard)
	file := flags.String("config", "", "required isolated synchronization YAML configuration")
	confirmationFile := flags.String("confirmation-config", "", "exclusive isolated confirmation recovery configuration")
	check := flags.Bool("check-config", false, "validate configuration and credential files without starting")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			fmt.Fprintln(output, "Usage: redis-syncer {-config /absolute/path/to/syncer.yaml | -confirmation-config /absolute/path/to/confirmation.yaml} [-check-config]")
			return 0
		}
		logger.Error("invalid syncer arguments")
		return 2
	}
	if (*file == "") == (*confirmationFile == "") || flags.NArg() != 0 {
		logger.Error("syncer requires exactly one configuration and accepts no positional arguments")
		return 2
	}
	if *confirmationFile != "" {
		cfg, err := syncer.LoadConfirmationConfig(*confirmationFile)
		if err != nil || cfg.CheckCredentials() != nil {
			logger.Error("invalid isolated confirmation configuration or credentials")
			return 1
		}
		if *check {
			logger.Info("isolated confirmation configuration valid")
			return 0
		}
		logger.Info("starting isolated confirmation endpoint", "version", version)
		if err := syncer.RunConfirmation(ctx, cfg); err != nil {
			logger.Error("isolated confirmation endpoint failed")
			return 1
		}
		logger.Info("isolated confirmation endpoint stopped")
		return 0
	}
	cfg, err := syncer.LoadConfig(*file)
	if err != nil {
		logger.Error("invalid syncer configuration")
		return 1
	}
	cfg.BuildVersion = version
	service, err := syncer.NewService(cfg)
	if err != nil {
		logger.Error("invalid syncer configuration")
		return 1
	}
	if err := service.CheckCredentials(); err != nil {
		logger.Error("invalid syncer credential files")
		return 1
	}
	if *check {
		logger.Info("isolated syncer configuration valid")
		return 0
	}
	logger.Info("starting isolated syncer", "version", version)
	// Service translates cancellation into the bounded graceful-stop protocol;
	// it must not pass this signal context directly to the capture runner.
	err = service.Run(ctx)
	status := service.Status()
	logFinalTaskStatus(logger, status)
	if err != nil {
		logger.Error("isolated syncer stopped with an error", "state", status.State, "service_error", status.LastError)
		return 1
	}
	logger.Info("isolated syncer stopped", "state", status.State, "service_error", status.LastError)
	return 0
}

// Status exposes only fixed, redacted categories and validated operational task
// IDs. Never log raw runner errors, backend endpoints, paths, or credentials.
func logFinalTaskStatus(logger *slog.Logger, status syncer.Status) {
	if len(status.Tasks) == 0 {
		logger.Info("isolated syncer task stopped", "task_state", status.Task.State, "task_error", status.Task.LastError, "task_running", status.Task.Running)
		return
	}
	for _, task := range status.Tasks {
		logger.Info("isolated syncer task stopped", "task_id", task.TaskID, "task_state", task.State, "task_error", task.LastError, "task_running", task.Running)
	}
}
