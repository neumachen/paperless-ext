// Command fn-renamer runs the File Normalizer renamer application.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/naming"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/probe"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/renamer"
)

func main() { os.Exit(run()) }

func run() int {
	if handled, code := handleSubcommand(); handled {
		return code
	}

	cfg, err := config.LoadRenamer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		return 2
	}

	app, err := renamer.New(context.Background(), cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "renamer startup failed\n")
		return 1
	}
	return app.Run(context.Background())
}

func handleSubcommand() (bool, int) {
	if len(os.Args) < 2 {
		return false, 0
	}
	switch os.Args[1] {
	case "healthcheck":
		// Asks the running process, over its own HTTP surface, rather than
		// proving only that this binary can start.
		return true, probe.Healthcheck(os.Args[2:])
	case "probe":
		return true, probe.Probe(os.Args[2:])
	case "version":
		fmt.Printf("fn-renamer %s revision=%s source=%s built=%s go=%s policy=%s contract=%d\n",
			buildinfo.Version, buildinfo.Revision, buildinfo.SourceDigest,
			buildinfo.BuildDate, buildinfo.GoVersion(), naming.PolicyVersion, 1)
		return true, 0
	case "check-config":
		// Validation and the effective-configuration view are the same code
		// path, so "it validates" and "this is what it means" can never
		// disagree. The output carries no credentials.
		cfg, err := config.LoadRenamer()
		if err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return true, 2
		}
		fmt.Fprintln(os.Stderr, "configuration is valid")
		if err := cfg.Effective().WriteJSON(os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n", err)
			return true, 1
		}
		return true, 0
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: expected version, check-config, healthcheck or probe\n")
		return true, 2
	}
}
