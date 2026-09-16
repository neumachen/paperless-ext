// Command fnctl is the File Normalizer inspection client.
//
// It exists so the gRPC API is exercised the way it is meant to be used:
// a real client, over the network, against a running service. It is shipped as
// its own container image and joins the application network; it is not a test
// helper and it substitutes nothing.
//
// Every subcommand is read-only with respect to the system. `preview` and
// `validate` compute answers from values this client supplies and never claim
// a file, reserve a destination, enqueue work or change a ledger row.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/grpcapi/gen/filenamenormalizerv1"
)

const usage = `fnctl -- File Normalizer inspection client

Usage:
  fnctl [-addr HOST:PORT] [-timeout DURATION] <command> [arguments]

Commands:
  status                       service health, identity and dependencies
  state                        aggregate processing state (no document identities)
  config                       effective non-secret configuration and policy
  inspect <job-id>             one job, sanitized
  inspect -restricted <job-id> one job including document identities
  inspect -events <job-id>     one job with its history
  validate <file|->            check a configuration document; activates nothing
  preview <name> [name...]     apply the naming policy to supplied names
  preview -config <file> <name>...   preview against a candidate configuration

All commands are read-only. A preview is not a destination reservation.
`

func main() { os.Exit(run()) }

func run() int {
	addr := flag.String("addr", envOr("FN_GRPC_TARGET", "watcher:9090"), "service address")
	timeout := flag.Duration("timeout", 10*time.Second, "per-call timeout")
	flag.Usage = func() { fmt.Fprint(os.Stderr, usage) }
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		flag.Usage()
		return 2
	}

	// Subcommand flags are parsed with their own flag set, because the
	// standard parser stops at the first positional argument: with one global
	// set, "inspect -restricted ID" would silently ignore -restricted and
	// return the sanitized view while the caller believed otherwise.
	sub := flag.NewFlagSet(args[0], flag.ContinueOnError)
	sub.SetOutput(os.Stderr)
	restricted := sub.Bool("restricted", false, "inspect: include document identities")
	events := sub.Bool("events", false, "inspect: include job history")
	configFile := sub.String("config", "", "preview: candidate configuration file")
	if err := sub.Parse(args[1:]); err != nil {
		return 2
	}
	args = append([]string{args[0]}, sub.Args()...)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	// Plaintext, because the API is bound to the application network and is
	// not published. See the deployment README for the access boundary.
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect %s: %v\n", *addr, err)
		return 1
	}
	defer conn.Close()
	client := pb.NewNormalizerClient(conn)

	switch args[0] {
	case "status":
		return emit(client.GetStatus(ctx, &pb.GetStatusRequest{}))
	case "state":
		return emit(client.GetProcessingState(ctx, &pb.GetProcessingStateRequest{}))
	case "config":
		return emit(client.GetEffectiveConfig(ctx, &pb.GetEffectiveConfigRequest{}))
	case "inspect":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "inspect requires a job id")
			return 2
		}
		return emit(client.InspectJob(ctx, &pb.InspectJobRequest{
			JobId:                   args[1],
			IncludeRestrictedDetail: *restricted,
			IncludeEvents:           *events,
		}))
	case "validate":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "validate requires a file or -")
			return 2
		}
		body, err := readDocument(args[1])
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return 2
		}
		resp, err := client.ValidateConfig(ctx, &pb.ValidateConfigRequest{ConfigJson: string(body)})
		code := emit(resp, err)
		if code == 0 && !resp.GetValid() {
			// An invalid document is a non-zero exit, so a validation step in
			// a script fails rather than printing problems and continuing.
			return 1
		}
		return code
	case "preview":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "preview requires at least one filename")
			return 2
		}
		req := &pb.PreviewNameRequest{Filenames: args[1:]}
		if *configFile != "" {
			body, err := readDocument(*configFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				return 2
			}
			req.ConfigJson = string(body)
		}
		return emit(client.PreviewName(ctx, req))
	default:
		flag.Usage()
		return 2
	}
}

// emit prints a response as JSON, or the error, and returns an exit status.
func emit(msg proto.Message, err error) int {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return 1
	}
	out, merr := protojson.MarshalOptions{Multiline: true, Indent: "  ", EmitUnpopulated: true}.Marshal(msg)
	if merr != nil {
		fmt.Fprintf(os.Stderr, "render response: %v\n", merr)
		return 1
	}
	fmt.Println(string(out))
	return 0
}

func readDocument(path string) ([]byte, error) {
	if path == "-" {
		return readAll(os.Stdin)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if !json.Valid(body) {
		return nil, fmt.Errorf("%s is not valid JSON", path)
	}
	return body, nil
}

func readAll(f *os.File) ([]byte, error) {
	var sb strings.Builder
	buf := make([]byte, 32*1024)
	for {
		n, err := f.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
		if sb.Len() > 1<<20 {
			return nil, fmt.Errorf("configuration document is too large")
		}
	}
	return []byte(sb.String()), nil
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
