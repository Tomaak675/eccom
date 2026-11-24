// Command eccom-agent is the Go implementation of the eccom agent process.
// It takes the same command line as `eccom agent`, so a deployment swaps it
// in per role (scenario sender.implementation / receiver.implementation: go).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"eccom/internal/agent"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if len(os.Args) < 2 || os.Args[1] != "agent" {
		fmt.Fprintln(os.Stderr, "usage: eccom-agent agent --role sender|receiver --scenario <file> --run-dir <dir>")
		os.Exit(2)
	}
	flags := flag.NewFlagSet("eccom-agent agent", flag.ExitOnError)
	role := flags.String("role", "", "sender or receiver")
	scenario := flags.String("scenario", "", "scenario YAML file")
	runDir := flags.String("run-dir", "", "directory for run artifacts")
	peerControl := flags.String("peer-control", "", "sender only: host:port of the receiver's control endpoint for clock offset probes")
	flags.Int("instance", 0, "accepted for command-line parity; the Go sender is always one process")
	endpointHost := flags.String("endpoint-host", "", "connect to this address instead of the scenario's endpoint host (a resolved role label)")
	flags.Parse(os.Args[2:])
	if *scenario == "" || *runDir == "" {
		flags.Usage()
		os.Exit(2)
	}

	var err error
	switch *role {
	case "receiver":
		err = agent.RunReceiver(context.Background(), *scenario, *runDir, *endpointHost)
	case "sender":
		// SIGTERM (docker stop) ends the schedule early; the artifacts are
		// still written.
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
		err = agent.RunSender(ctx, *scenario, *runDir, *peerControl, *endpointHost)
		stop()
	default:
		fmt.Fprintf(os.Stderr, "--role must be sender or receiver, got %q\n", *role)
		os.Exit(2)
	}
	if err != nil {
		slog.Error("agent failed", "error", err)
		os.Exit(1)
	}
}
