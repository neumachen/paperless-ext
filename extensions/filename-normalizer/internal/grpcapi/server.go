// Package grpcapi serves the inspection, validation and preview API.
//
// # Non-mutation
//
// Nothing in this package writes to the ledger, the broker or the filesystem.
// The ledger handle it holds is used for reads only, and the two RPCs that
// look like they might act -- ValidateConfig and PreviewName -- work entirely
// on values supplied by the caller. RabbitMQ therefore remains the only way
// work enters the system, and no RPC can create a job that skipped dispatch,
// durability or recovery.
//
// # Privacy
//
// The default job view carries no document identities. Restricted detail is
// opt-in per call and the disclosure is logged. Errors are closed-set codes
// with short messages: a gRPC status must never carry a filename, a path or a
// fingerprint back to a caller who did not ask for restricted detail.
package grpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"

	pb "github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/grpcapi/gen/filenamenormalizerv1"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/health"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/naming"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// previewLimit bounds one PreviewName call.
const previewLimit = 200

// maxConfigDocument bounds a submitted configuration document.
const maxConfigDocument = 1 << 20

// Deps is what the server reads. Every field is read-only in use.
type Deps struct {
	Common    config.Common
	Effective config.Effective
	Ledger    *ledger.Ledger
	Health    *health.Server
	Metrics   *telemetry.Metrics
	Logger    *slog.Logger
	// DiscoveryEnabled is reported in the aggregate state so "idle" and "not
	// running" can be told apart.
	DiscoveryEnabled bool
}

// Server is the gRPC surface.
type Server struct {
	pb.UnimplementedNormalizerServer
	deps Deps
	log  *slog.Logger
	srv  *grpc.Server
	addr string
}

// New builds the server. It does not listen until Serve is called.
func New(addr string, deps Deps) *Server {
	s := &Server{deps: deps, log: deps.Logger.With(slog.String("component", "grpc")), addr: addr}
	s.srv = grpc.NewServer(
		grpc.ChainUnaryInterceptor(s.logUnary),
		grpc.MaxRecvMsgSize(4<<20),
	)
	pb.RegisterNormalizerServer(s.srv, s)
	// Reflection makes the containerized client example possible without
	// shipping a descriptor set. It exposes only the schema, never data.
	reflection.Register(s.srv)
	return s
}

// Serve listens and blocks until the context is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	s.log.Info("grpc api listening",
		slog.String("event", "grpc_listening"),
		slog.String("addr", s.addr))

	done := make(chan error, 1)
	go func() { done <- s.srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		// GracefulStop lets in-flight reads finish. They are reads, so this is
		// quick, but cutting them off would surface as an error to a caller
		// for no reason.
		s.srv.GracefulStop()
		s.log.Info("grpc api stopped", slog.String("event", "grpc_stopped"))
		return nil
	case err := <-done:
		if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			return fmt.Errorf("grpc serve: %w", err)
		}
		return nil
	}
}

// logUnary records each call without recording its arguments.
func (s *Server) logUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := h(ctx, req)
	code := codes.OK
	if err != nil {
		code = status.Code(err)
	}
	// The method name and status code are safe; the request is not logged,
	// because a PreviewName request carries filenames.
	s.log.Info("grpc call",
		slog.String("event", "grpc_call"),
		slog.String("path", info.FullMethod),
		slog.String("outcome", code.String()),
		slog.Int64("duration_ms", time.Since(start).Milliseconds()))
	return resp, err
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

// GetStatus reports identity and readiness.
func (s *Server) GetStatus(context.Context, *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	ready, states := s.deps.Health.Snapshot()
	out := &pb.GetStatusResponse{
		Application:     string(s.deps.Common.Application),
		Instance:        s.deps.Common.Instance,
		Version:         buildinfo.Version,
		Revision:        buildinfo.Revision,
		BuildDate:       buildinfo.BuildDate,
		GoVersion:       buildinfo.GoVersion(),
		SourceDigest:    buildinfo.SourceDigest,
		PolicyIdentity:  s.deps.Common.Policy.Identity,
		PolicyStatus:    config.PolicyStatusCandidate,
		ContractVersion: int32(jobs.ContractVersion),
		Alive:           true,
		Ready:           ready,
		Draining:        s.deps.Health.Draining(),
	}
	for _, st := range states {
		out.Dependencies = append(out.Dependencies, &pb.DependencyState{
			Name: st.Name, Required: st.Required, Ok: st.OK, Category: st.Category,
		})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Aggregate processing state
// ---------------------------------------------------------------------------

// GetProcessingState reports counts only.
func (s *Server) GetProcessingState(ctx context.Context, _ *pb.GetProcessingStateRequest) (*pb.GetProcessingStateResponse, error) {
	snap, err := s.deps.Ledger.Snapshot(ctx)
	if err != nil {
		return nil, status.Error(codes.Unavailable, "the ledger is unavailable")
	}
	out := &pb.GetProcessingStateResponse{
		JobsByState:             map[string]int64{},
		HoldsByCategory:         map[string]int64{},
		OldestPendingAgeSeconds: snap.OldestPendingAge.Seconds(),
		DiscoveryEnabled:        s.deps.DiscoveryEnabled,
	}
	// The closed state set is emitted in full, including zeros, so a caller
	// can tell "no jobs in this state" from "this build has no such state".
	for _, st := range jobs.States() {
		out.JobsByState[string(st)] = int64(snap.ByState[st])
	}
	for _, c := range jobs.Categories() {
		out.HoldsByCategory[string(c)] = int64(snap.ByCategory[c])
	}
	out.DeliveredTotal = int64(snap.ByState[jobs.StateDelivered])

	if n, err := s.deps.Ledger.CountReservations(ctx); err == nil {
		out.ReservationsTotal = n
	}
	if ts, err := s.deps.Ledger.LastDiscoveryRun(ctx); err == nil && !ts.IsZero() {
		out.DiscoveryLastRunUnix = ts.Unix()
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Effective configuration
// ---------------------------------------------------------------------------

// GetEffectiveConfig reports the non-secret configuration.
func (s *Server) GetEffectiveConfig(context.Context, *pb.GetEffectiveConfigRequest) (*pb.GetEffectiveConfigResponse, error) {
	body, err := json.MarshalIndent(s.deps.Effective, "", "  ")
	if err != nil {
		return nil, status.Error(codes.Internal, "the effective configuration could not be rendered")
	}
	return &pb.GetEffectiveConfigResponse{
		EffectiveJson:  string(body),
		ConfigFile:     s.deps.Common.ConfigFile,
		PolicyIdentity: s.deps.Common.Policy.Identity,
	}, nil
}

// ---------------------------------------------------------------------------
// Job inspection
// ---------------------------------------------------------------------------

// InspectJob returns one job, sanitized unless restricted detail is requested.
func (s *Server) InspectJob(ctx context.Context, req *pb.InspectJobRequest) (*pb.InspectJobResponse, error) {
	if !jobs.IsJobID(req.GetJobId()) {
		// The malformed value is NOT echoed: it is caller-supplied text of
		// unknown provenance and it would end up in the caller's logs.
		return nil, status.Error(codes.InvalidArgument, "job_id must be a lowercase UUID")
	}
	job, err := s.deps.Ledger.GetJob(ctx, req.GetJobId())
	if errors.Is(err, ledger.ErrNotFound) {
		return nil, status.Error(codes.NotFound, "no such job")
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "the ledger is unavailable")
	}

	out := &pb.InspectJobResponse{
		JobId:            job.JobID,
		State:            string(job.State),
		PolicyIdentity:   job.PolicyVersion,
		DispatchAttempts: int32(job.DispatchAttempts),
		DeliveryAttempts: int32(job.DeliveryAttempts),
		CreatedAt:        job.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:        job.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}
	if job.FailureCategory != nil {
		out.Category = *job.FailureCategory
	}
	if job.SizeBytes != nil {
		out.SizeBytes = *job.SizeBytes
	}
	if job.TerminalAt != nil {
		out.TerminalAt = job.TerminalAt.UTC().Format(time.RFC3339Nano)
	}

	receipt, receiptErr := s.deps.Ledger.GetReceipt(ctx, job.JobID)
	out.HasReceipt = receiptErr == nil

	if req.GetIncludeRestrictedDetail() {
		// An explicit request for document identities is itself an event
		// worth recording, so an unexplained disclosure can be traced.
		s.log.Warn("restricted job detail disclosed over the API",
			slog.String("event", "restricted_detail_disclosed"),
			slog.String("job_id", job.JobID))
		d := &pb.RestrictedJobDetail{
			SourceRoot: job.SourceRoot,
			SourceName: job.SourceName,
		}
		if job.NormalizedName != nil {
			d.NormalizedName = *job.NormalizedName
		}
		if job.ReservedName != nil {
			d.ReservedName = *job.ReservedName
		}
		if receiptErr == nil {
			d.DeliveredName = receipt.DeliveredName
		}
		if job.Fingerprint != nil {
			d.ContentFingerprintHex = storage.HexFingerprint(job.Fingerprint)
		}
		out.Restricted = d
	}

	if req.GetIncludeEvents() {
		events, err := s.deps.Ledger.Events(ctx, job.JobID)
		if err != nil {
			return nil, status.Error(codes.Unavailable, "the ledger is unavailable")
		}
		for _, e := range events {
			je := &pb.JobEvent{
				OccurredAt: e.OccurredAt.UTC().Format(time.RFC3339Nano),
				EventType:  string(e.EventType),
				Actor:      e.Actor,
			}
			if e.FromState != nil {
				je.FromState = *e.FromState
			}
			if e.ToState != nil {
				je.ToState = *e.ToState
			}
			if e.Category != nil {
				je.Category = *e.Category
			}
			if e.Attempt != nil {
				je.Attempt = int32(*e.Attempt)
			}
			out.Events = append(out.Events, je)
		}
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Configuration validation
// ---------------------------------------------------------------------------

const activationInstructions = "Validation does not activate anything. Write the document to the file named by " +
	"FN_CONFIG_FILE, then restart the applications. Hot reload is not supported. " +
	"Jobs already accepted keep the policy identity they were accepted under; a renamer " +
	"running a different policy holds them as policy_version_mismatch rather than renaming them."

// ValidateConfig checks a candidate document without applying it.
func (s *Server) ValidateConfig(_ context.Context, req *pb.ValidateConfigRequest) (*pb.ValidateConfigResponse, error) {
	body := req.GetConfigJson()
	if len(body) > maxConfigDocument {
		return nil, status.Error(codes.InvalidArgument, "the configuration document is too large")
	}
	if body == "" {
		return nil, status.Error(codes.InvalidArgument, "config_json is required")
	}

	identity, problems := config.ValidateDocument([]byte(body))
	out := &pb.ValidateConfigResponse{
		Valid:          len(problems) == 0,
		Problems:       problems,
		PolicyIdentity: identity,
		Activation:     activationInstructions,
	}
	if out.Valid {
		out.DiffersFromRunning = identity != s.deps.Common.Policy.Identity
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Preview
// ---------------------------------------------------------------------------

const previewDisclaimer = "A preview is not a destination reservation. It shows what the policy would " +
	"produce for these names right now; the actual destination is allocated only when a real " +
	"submission is published, and a name shown here may be taken by then."

// PreviewName applies the policy to caller-supplied names.
//
// It allocates nothing. The collision candidates it reports are what the
// allocator WOULD try, computed from the policy alone without consulting or
// touching the reservation table.
func (s *Server) PreviewName(_ context.Context, req *pb.PreviewNameRequest) (*pb.PreviewNameResponse, error) {
	names := req.GetFilenames()
	if len(names) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one filename is required")
	}
	if len(names) > previewLimit {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d filenames per call", previewLimit)
	}

	policy := s.deps.Common.Policy
	if doc := req.GetConfigJson(); doc != "" {
		if len(doc) > maxConfigDocument {
			return nil, status.Error(codes.InvalidArgument, "the configuration document is too large")
		}
		candidate, problems := config.PolicyFromDocument([]byte(doc))
		if len(problems) > 0 {
			return nil, status.Errorf(codes.InvalidArgument,
				"the candidate configuration is not valid (%d problems); use ValidateConfig for the list",
				len(problems))
		}
		policy = candidate
	}

	// A fixed placeholder id, not a real one. Allocating a job id here would
	// be indistinguishable from creating work, and the empty-stem fallback is
	// derived from the id, so the preview says plainly which id it assumed.
	const previewJobID = "00000000-0000-4000-8000-000000000000"

	out := &pb.PreviewNameResponse{
		PolicyIdentity: policy.Identity,
		Disclaimer:     previewDisclaimer,
	}
	for _, name := range names {
		r := &pb.PreviewResult{Original: name}
		res, err := policy.Normalize(name, previewJobID)
		if err != nil {
			r.Accepted = false
			r.Category = naming.HoldCategory(err)
			out.Results = append(out.Results, r)
			continue
		}
		r.Accepted = true
		r.Normalized = res.Name
		r.UsedFallback = res.UsedFallback
		r.Shortened = res.Shortened
		r.RulesApplied = res.RuleHits
		for n := 0; n < 3; n++ {
			c, err := policy.Candidate(res, n)
			if err != nil {
				break
			}
			r.CollisionCandidates = append(r.CollisionCandidates, c)
		}
		out.Results = append(out.Results, r)
	}
	return out, nil
}

// Addr reports the configured listen address.
func (s *Server) Addr() string { return s.addr }
