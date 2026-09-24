// Package config loads and validates both applications' runtime configuration.
//
// Configuration comes only from the environment. Every secret may instead be
// supplied through a "<VAR>_FILE" indirection so that credentials arrive as a
// mounted file rather than an inherited environment variable, and no secret is
// ever placed in the loggable summary produced by Summary.
package config

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Application distinguishes the two executables.
type Application string

const (
	// AppWatcher hosts dispatch and completion-accounting workers.
	AppWatcher Application = "watcher"
	// AppRenamer consumes jobs and is independently scalable.
	AppRenamer Application = "renamer"
)

// Common holds settings both applications share.
type Common struct {
	Application Application
	Instance    string
	LogLevel    string
	HTTPAddr    string
	// GRPCAddr is the inspection API's listen address. It is bound on the
	// application network only and is deliberately not published to the host
	// by default: the API has no authentication, and restricted job detail is
	// reachable from it. Empty disables the server entirely.
	GRPCAddr        string
	ShutdownTimeout time.Duration
	Database        Database
	Broker          Broker
	Storage         Storage
	// Policy is the compiled naming policy and its identity.
	Policy Policy
	// ConfigFile is the configuration file this process read, or "".
	ConfigFile string
	// Roots is the verified identity of every configured root. Roots are
	// compared by device and inode, not by pathname, so a symlinked root is
	// supported while two roots secretly naming one directory is refused.
	Roots RootSet
	// Faults names the injected interruption points, normally empty.
	Faults FaultPoints
}

// Database describes the PostgreSQL cluster endpoints.
//
// The primary is required. The replica is a distinct endpoint because the
// deployed topology is a streaming primary/standby pair: the applications must
// be able to observe the standby without treating it as interchangeable with
// the primary.
type Database struct {
	PrimaryHost     string
	PrimaryPort     int
	ReplicaHost     string
	ReplicaPort     int
	ReplicaRequired bool
	Name            string
	User            string
	Password        string
	SSLMode         string
	MaxConns        int
	ConnectTimeout  time.Duration
	ApplyMigrations bool
}

// Broker describes the RabbitMQ endpoint and the durable topology.
type Broker struct {
	Host           string
	Port           int
	VHost          string
	User           string
	Password       string
	Exchange       string
	Queue          string
	RoutingKey     string
	DeadLetterX    string
	DeadLetterQ    string
	DeliveryLimit  int
	ConfirmTimeout time.Duration
	DialTimeout    time.Duration
	Heartbeat      time.Duration
	ReconnectDelay time.Duration
}

// Storage lists the configured filesystem roles.
//
// Roles are probed for availability, never merely listed. An unreadable or
// unmounted root must be reported as unavailable storage and must never be
// interpreted as an empty directory.
type Storage struct {
	Incoming string
	Queued   string
	Staging  string
	Consume  string
	Failed   string
	Required bool
	// ArchiveAction is what this process does with a delivered original:
	// "" (nothing), "move" into ArchiveDir -- a directory inside the incoming
	// root -- or "remove". Only the watcher ever sets it, and setting it is
	// what makes the watcher's incoming role writable.
	ArchiveAction string
	ArchiveDir    string
}

// RolesFor returns the configured roots with the access each application
// actually needs, in a stable order.
//
// The split is least privilege, and it is behavioural rather than cosmetic:
// the watcher must not be able to write into the consumer's directory, because
// publication belongs to the renamer. The deployment mounts consume read-only
// for the watcher, and the probe below expects exactly that.
//
// Incoming is read-only for both, except for a watcher that archives: moving a
// delivered original out of the drop folder is a write, and it is the
// watcher's alone. The renamers still only read it.
func (s Storage) RolesFor(app Application) []Role {
	write := func(name, path string) Role { return Role{Name: name, Path: path, WriteRequired: true} }
	read := func(name, path string) Role { return Role{Name: name, Path: path} }

	incoming := read("incoming", s.Incoming)
	if app == AppWatcher && s.ArchiveAction != "" {
		incoming = write("incoming", s.Incoming)
	}
	roles := []Role{
		incoming,
		write("queued", s.Queued),
		write("staging", s.Staging),
		write("failed", s.Failed),
	}
	switch app {
	case AppRenamer:
		roles = append(roles, write("consume", s.Consume))
	default:
		roles = append(roles, read("consume", s.Consume))
	}
	return roles
}

// Role is one configured storage role.
type Role struct {
	Name string
	Path string
	// WriteRequired marks a role this application must be able to write.
	WriteRequired bool
}

// WatcherConfig is the watcher application's configuration.
type WatcherConfig struct {
	Common
	// Discovery is the compiled discovery and completion configuration.
	Discovery           Discovery
	DispatchInterval    time.Duration
	DispatchBatch       int
	DispatchClaimMaxAge time.Duration
	AccountingInterval  time.Duration
	// ReconcileOnStart re-examines the incoming root at startup so work that
	// arrived while the watcher was down is picked up.
	ReconcileOnStart bool
	// Archive moves each delivered original out of the incoming root.
	Archive Archive
}

// Archive is the compiled source-archival configuration.
//
// It is off unless a deployment turns it on. With it on, every submission the
// watcher registers is accepted with a request to deal with its original once
// the job is delivered -- so a drop folder holds only what has not been handled
// yet. Action says how:
//
//   - "move" (the default) moves the original into Directory, inside the
//     incoming root. Nothing is deleted.
//   - "remove" removes it. The delivered copy is the same bytes -- verified
//     before it was published, and verified against the original again before
//     the original goes -- so a deployment that wants exactly two folders, the
//     drop folder and the consumer's, has no third one filling up.
//
// Either way an original that changed after it was delivered is left where it
// is, and originals of held and uncertain jobs are never touched.
type Archive struct {
	Enabled bool
	// Action is "move" or "remove".
	Action string
	// Directory is one directory name inside the incoming root, used by a
	// move. It must exist: it is never created, for the reason a root is never
	// created.
	Directory string
	Interval  time.Duration
	Batch     int
}

// RenamerConfig is the renamer application's configuration.
type RenamerConfig struct {
	Common
	// Concurrency bounds simultaneous in-flight deliveries per instance.
	Concurrency int
	// Prefetch is the AMQP QoS window; it defaults to Concurrency so an
	// instance never buffers work it has no capacity to process.
	Prefetch int
	// MaxDeliveryAttempts bounds redelivery before a job is held.
	MaxDeliveryAttempts int
	// PublishTakeoverAfter is how old a publication claim must be before
	// another attempt may take it over.
	//
	// It is its own setting rather than the shutdown timeout it used to
	// borrow. Those answer different questions -- "how long do we wait for a
	// graceful stop" and "how long before we presume the worker holding this
	// publication is gone" -- and tying them together meant an operator could
	// not lengthen one without lengthening the other. Too short and a live
	// publication gets taken over mid-flight; too long and a genuinely dead
	// worker's claim strands a document.
	PublishTakeoverAfter time.Duration
	// DryRun switches the renamer into preview mode. It does not consume the
	// work queue at all; see the renamer package for why.
	DryRun bool
	// Discovery is carried so the renamer opens a source the same way
	// discovery found it, including whether a relative subpath is allowed.
	Discovery Discovery
}

// ValidationError collects every configuration problem at once so an operator
// sees the whole list instead of fixing one variable per restart.
type ValidationError struct{ Problems []string }

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid configuration: %s", strings.Join(e.Problems, "; "))
}

type loader struct {
	problems []string
	// fileDefaults holds values supplied by the configuration file. They sit
	// beneath the environment: lookup consults them only when neither VAR nor
	// VAR_FILE is set. See file.go for the full precedence contract.
	fileDefaults map[string]string
}

func (l *loader) fail(format string, args ...any) {
	l.problems = append(l.problems, fmt.Sprintf(format, args...))
}

// lookup reads VAR, or the contents of the file named by VAR_FILE.
func (l *loader) lookup(key string) (string, bool) {
	if path, ok := os.LookupEnv(key + "_FILE"); ok && strings.TrimSpace(path) != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			l.fail("%s_FILE is not readable", key)
			return "", false
		}
		return strings.TrimRight(string(data), "\r\n"), true
	}
	if v, ok := os.LookupEnv(key); ok {
		return v, true
	}
	if v, ok := l.fileDefaults[key]; ok {
		return v, true
	}
	return "", false
}

func (l *loader) str(key, def string) string {
	v, ok := l.lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func (l *loader) required(key string) string {
	v, ok := l.lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		l.fail("%s is required", key)
		return ""
	}
	return v
}

func (l *loader) intVal(key string, def, min, max int) int {
	raw, ok := l.lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		l.fail("%s must be an integer", key)
		return def
	}
	if n < min || n > max {
		l.fail("%s must be between %d and %d", key, min, max)
		return def
	}
	return n
}

func (l *loader) boolVal(key string, def bool) bool {
	raw, ok := l.lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	b, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		l.fail("%s must be a boolean", key)
		return def
	}
	return b
}

func (l *loader) duration(key string, def, min, max time.Duration) time.Duration {
	raw, ok := l.lookup(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		l.fail("%s must be a Go duration such as 30s", key)
		return def
	}
	if d < min || d > max {
		l.fail("%s must be between %s and %s", key, min, max)
		return def
	}
	return d
}

func (l *loader) absPath(key, def string) string {
	v := l.str(key, def)
	if v == "" {
		l.fail("%s is required", key)
		return ""
	}
	if !strings.HasPrefix(v, "/") {
		l.fail("%s must be an absolute path", key)
	}
	return v
}

func (l *loader) common(app Application) Common {
	host, _ := os.Hostname()
	if host == "" {
		host = string(app)
	}

	c := Common{
		Application:     app,
		Instance:        l.str("FN_INSTANCE", host),
		LogLevel:        l.str("FN_LOG_LEVEL", "info"),
		HTTPAddr:        l.str("FN_HTTP_ADDR", ":8080"),
		GRPCAddr:        l.str("FN_GRPC_ADDR", ":9090"),
		ShutdownTimeout: l.duration("FN_SHUTDOWN_TIMEOUT", 20*time.Second, time.Second, 5*time.Minute),
	}

	c.Database = Database{
		PrimaryHost:     l.required("FN_DB_PRIMARY_HOST"),
		PrimaryPort:     l.intVal("FN_DB_PRIMARY_PORT", 5432, 1, 65535),
		ReplicaHost:     l.str("FN_DB_REPLICA_HOST", ""),
		ReplicaPort:     l.intVal("FN_DB_REPLICA_PORT", 5432, 1, 65535),
		ReplicaRequired: l.boolVal("FN_DB_REPLICA_REQUIRED", false),
		Name:            l.required("FN_DB_NAME"),
		User:            l.required("FN_DB_USER"),
		Password:        l.required("FN_DB_PASSWORD"),
		SSLMode:         l.str("FN_DB_SSLMODE", "disable"),
		MaxConns:        l.intVal("FN_DB_MAX_CONNS", 8, 1, 200),
		ConnectTimeout:  l.duration("FN_DB_CONNECT_TIMEOUT", 5*time.Second, time.Second, time.Minute),
		ApplyMigrations: l.boolVal("FN_DB_APPLY_MIGRATIONS", app == AppWatcher),
	}
	switch c.Database.SSLMode {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
	default:
		l.fail("FN_DB_SSLMODE is not a recognised libpq sslmode")
	}
	if c.Database.ReplicaRequired && c.Database.ReplicaHost == "" {
		l.fail("FN_DB_REPLICA_REQUIRED is set but FN_DB_REPLICA_HOST is empty")
	}

	exchange := l.str("FN_AMQP_EXCHANGE", "filename_normalizer.jobs")
	c.Broker = Broker{
		Host:           l.required("FN_AMQP_HOST"),
		Port:           l.intVal("FN_AMQP_PORT", 5672, 1, 65535),
		VHost:          l.str("FN_AMQP_VHOST", "/"),
		User:           l.required("FN_AMQP_USER"),
		Password:       l.required("FN_AMQP_PASSWORD"),
		Exchange:       exchange,
		Queue:          l.str("FN_AMQP_QUEUE", "filename_normalizer.jobs.v1"),
		RoutingKey:     l.str("FN_AMQP_ROUTING_KEY", "normalize"),
		DeadLetterX:    l.str("FN_AMQP_DLX", exchange+".dlx"),
		DeadLetterQ:    l.str("FN_AMQP_DEAD_LETTER_QUEUE", "filename_normalizer.jobs.v1.dead"),
		DeliveryLimit:  l.intVal("FN_AMQP_DELIVERY_LIMIT", 5, 1, 100),
		ConfirmTimeout: l.duration("FN_AMQP_CONFIRM_TIMEOUT", 10*time.Second, time.Second, time.Minute),
		DialTimeout:    l.duration("FN_AMQP_DIAL_TIMEOUT", 5*time.Second, time.Second, time.Minute),
		Heartbeat:      l.duration("FN_AMQP_HEARTBEAT", 10*time.Second, time.Second, time.Minute),
		ReconnectDelay: l.duration("FN_AMQP_RECONNECT_DELAY", 2*time.Second, 100*time.Millisecond, time.Minute),
	}

	c.Storage = Storage{
		Incoming: l.absPath("FN_STORAGE_INCOMING", "/var/lib/filename-normalizer/incoming"),
		Queued:   l.absPath("FN_STORAGE_QUEUED", "/var/lib/filename-normalizer/queued"),
		Staging:  l.absPath("FN_STORAGE_STAGING", "/var/lib/filename-normalizer/staging"),
		Consume:  l.absPath("FN_STORAGE_CONSUME", "/var/lib/filename-normalizer/consume"),
		Failed:   l.absPath("FN_STORAGE_FAILED", "/var/lib/filename-normalizer/failed"),
		Required: l.boolVal("FN_STORAGE_REQUIRED", true),
	}

	// Destructive cleanup is explicitly unresolved policy. Refusing to start is
	// safer than accepting a flag that has no implementation behind it.
	if l.boolVal("FN_CLEANUP_ENABLED", false) {
		l.fail("FN_CLEANUP_ENABLED is not supported: source deletion and ledger purging are unresolved policy and are not implemented")
	}

	if _, ok := levelNames[strings.ToLower(c.LogLevel)]; !ok {
		l.fail("FN_LOG_LEVEL must be one of debug, info, warn, error")
	}
	return c
}

var levelNames = map[string]struct{}{
	"debug": {}, "info": {}, "warn": {}, "warning": {}, "error": {},
}

// newLoader reads the configuration file, if one is named, and returns a
// loader primed with its values as the layer beneath the environment.
func newLoader() (*loader, FileConfig, string) {
	l := &loader{fileDefaults: map[string]string{}}
	path, ok := ConfigFilePath()
	if !ok {
		return l, FileConfig{Version: FileConfigVersion}, ""
	}
	fc, err := LoadFileConfig(path)
	if err != nil {
		// A named-but-unusable configuration file is a hard failure. Falling
		// back to defaults would run the deployment under a policy nobody
		// declared, which is exactly the silent reinterpretation the contract
		// forbids.
		l.fail("%v", err)
		return l, FileConfig{Version: FileConfigVersion}, path
	}
	applyStorageFile(fc.Storage, l.fileDefaults)
	return l, fc, path
}

// LoadWatcher reads the watcher configuration.
func LoadWatcher() (WatcherConfig, error) {
	l, fc, path := newLoader()
	cfg := WatcherConfig{Common: l.common(AppWatcher)}
	cfg.ConfigFile = path
	cfg.Policy = compilePolicy(l, fc.Normalization)
	cfg.Discovery = compileDiscovery(l, fc.Discovery)
	validateStorageRoots(l, cfg.Storage)
	cfg.Roots = identifyRoots(l, cfg.Storage)
	cfg.Faults = warnOnFaults(l)

	cfg.DispatchInterval = l.duration("FN_WATCHER_DISPATCH_INTERVAL", time.Second, 50*time.Millisecond, 5*time.Minute)
	cfg.DispatchBatch = l.intVal("FN_WATCHER_DISPATCH_BATCH", 32, 1, 1000)
	cfg.DispatchClaimMaxAge = l.duration("FN_WATCHER_DISPATCH_CLAIM_MAX_AGE", 60*time.Second, 5*time.Second, time.Hour)
	cfg.AccountingInterval = l.duration("FN_WATCHER_ACCOUNTING_INTERVAL", 5*time.Second, time.Second, 5*time.Minute)
	cfg.ReconcileOnStart = l.boolVal("FN_WATCHER_RECONCILE_ON_START", true)
	cfg.Archive = compileArchive(l, cfg.Discovery)
	if cfg.Archive.Enabled {
		cfg.Storage.ArchiveAction = cfg.Archive.Action
		if cfg.Archive.Action == ArchiveMove {
			cfg.Storage.ArchiveDir = cfg.Archive.Directory
		}
	}

	if len(l.problems) > 0 {
		sort.Strings(l.problems)
		return WatcherConfig{}, &ValidationError{Problems: dedupe(l.problems)}
	}
	return cfg, nil
}

// LoadRenamer reads the renamer configuration.
func LoadRenamer() (RenamerConfig, error) {
	l, fc, path := newLoader()
	cfg := RenamerConfig{Common: l.common(AppRenamer)}
	cfg.ConfigFile = path
	cfg.Policy = compilePolicy(l, fc.Normalization)
	cfg.Discovery = compileDiscovery(l, fc.Discovery)
	validateStorageRoots(l, cfg.Storage)
	cfg.Roots = identifyRoots(l, cfg.Storage)
	cfg.Faults = warnOnFaults(l)

	concurrency, prefetch, attempts, dryRun := 1, 0, 5, false
	if fc.Processing != nil {
		if v := checkRange(l, "processing.concurrency", fc.Processing.Concurrency, 1, 64); v != 0 {
			concurrency = v
		}
		if v := checkRange(l, "processing.prefetch", fc.Processing.Prefetch, 1, 1000); v != 0 {
			prefetch = v
		}
		if v := checkRange(l, "processing.max_delivery_attempts", fc.Processing.MaxDeliveryAttempts, 1, 100); v != 0 {
			attempts = v
		}
		if fc.Processing.DryRun != nil {
			dryRun = *fc.Processing.DryRun
		}
	}
	cfg.Concurrency = l.intVal("FN_RENAMER_CONCURRENCY", concurrency, 1, 64)
	if prefetch == 0 {
		prefetch = cfg.Concurrency
	}
	cfg.Prefetch = l.intVal("FN_RENAMER_PREFETCH", prefetch, 1, 1000)
	if cfg.Prefetch < cfg.Concurrency {
		l.fail("FN_RENAMER_PREFETCH (%d) must be at least FN_RENAMER_CONCURRENCY (%d); "+
			"they may also come from processing.prefetch and processing.concurrency",
			cfg.Prefetch, cfg.Concurrency)
	}
	cfg.MaxDeliveryAttempts = l.intVal("FN_RENAMER_MAX_DELIVERY_ATTEMPTS", attempts, 1, 100)
	cfg.PublishTakeoverAfter = l.duration("FN_PUBLISH_TAKEOVER_AFTER", cfg.ShutdownTimeout, 5*time.Second, 10*time.Minute)
	cfg.DryRun = l.boolVal("FN_RENAMER_DRY_RUN", dryRun)

	if len(l.problems) > 0 {
		sort.Strings(l.problems)
		return RenamerConfig{}, &ValidationError{Problems: dedupe(l.problems)}
	}
	return cfg, nil
}

// Source-archival defaults.
const (
	defaultArchiveDirectory = "processed"
	defaultArchiveInterval  = 10 * time.Second
	defaultArchiveBatch     = 50
)

// The two things archival can do with a delivered original. They are the
// values the ledger records, too.
const (
	ArchiveMove   = "move"
	ArchiveRemove = "remove"
)

// compileArchive reads the source-archival settings, which only the watcher
// has.
func compileArchive(l *loader, d Discovery) Archive {
	a := Archive{
		Enabled:   l.boolVal("FN_ARCHIVE_ENABLED", false),
		Action:    l.str("FN_ARCHIVE_ACTION", ArchiveMove),
		Directory: l.str("FN_ARCHIVE_DIRECTORY", defaultArchiveDirectory),
		Interval:  l.duration("FN_ARCHIVE_INTERVAL", defaultArchiveInterval, time.Second, time.Hour),
		Batch:     l.intVal("FN_ARCHIVE_BATCH", defaultArchiveBatch, 1, 1000),
	}
	switch a.Action {
	case ArchiveMove, ArchiveRemove:
	default:
		l.fail("FN_ARCHIVE_ACTION must be %q or %q", ArchiveMove, ArchiveRemove)
	}
	if !ValidArchiveDirectory(a.Directory) {
		l.fail("FN_ARCHIVE_DIRECTORY must be one directory name inside the incoming root: not a path, not hidden, not . or ..")
	}
	// A recursive scan descends into the archive directory, and every
	// original moved there would be registered and delivered again.
	if a.Enabled && d.Recursive {
		l.fail("FN_ARCHIVE_ENABLED cannot be combined with recursive discovery: " +
			"the archive directory is inside the incoming root, so a recursive scan would register every archived original again")
	}
	return a
}

// ValidArchiveDirectory reports whether name can be the archive directory:
// exactly one path component, visible, and not a reference to the root itself
// or its parent.
func ValidArchiveDirectory(name string) bool {
	if name == "" || name == "." || name == ".." || len(name) > 255 {
		return false
	}
	if strings.HasPrefix(name, ".") || strings.ContainsAny(name, "/\\\x00") {
		return false
	}
	return strings.TrimSpace(name) == name
}

// dedupe removes repeated problems, which the two policy compilations can
// otherwise produce for the same underlying mistake.
func dedupe(in []string) []string {
	out := in[:0]
	var prev string
	for i, s := range in {
		if i > 0 && s == prev {
			continue
		}
		out = append(out, s)
		prev = s
	}
	return out
}

// DSN renders the primary connection string, with every value quoted.
func (d Database) DSN() string { return d.dsn(d.PrimaryHost, d.PrimaryPort) }

// ReplicaDSN renders the standby connection string, or "" when none is set.
func (d Database) ReplicaDSN() string {
	if d.ReplicaHost == "" {
		return ""
	}
	return d.dsn(d.ReplicaHost, d.ReplicaPort)
}

func (d Database) dsn(host string, port int) string {
	// Every value is single-quoted with backslash escaping, which is the libpq
	// keyword/value quoting rule. This is not cosmetic: a perfectly valid
	// password containing a space would otherwise terminate the password
	// parameter early and turn the remainder into further parameters, so the
	// connection would either fail or — worse — be made with different
	// settings than intended. Generated alphanumeric development passwords
	// never exercise that, which is exactly why it has to be handled here
	// rather than relied upon not to happen.
	//
	// The result contains the password and must never be logged; use Summary
	// for anything that reaches an output stream.
	return strings.Join([]string{
		"host=" + quoteDSNValue(host),
		"port=" + quoteDSNValue(strconv.Itoa(port)),
		"dbname=" + quoteDSNValue(d.Name),
		"user=" + quoteDSNValue(d.User),
		"password=" + quoteDSNValue(d.Password),
		"sslmode=" + quoteDSNValue(d.SSLMode),
		"connect_timeout=" + quoteDSNValue(strconv.Itoa(int(d.ConnectTimeout.Seconds()))),
		"application_name=" + quoteDSNValue("filename-normalizer"),
	}, " ")
}

// quoteDSNValue renders one libpq keyword/value parameter value.
//
// Per the libpq connection-string rules, a value is surrounded by single
// quotes, and within them a backslash or a single quote is escaped with a
// backslash. Quoting unconditionally also covers the empty-value case.
func quoteDSNValue(v string) string {
	var b strings.Builder
	b.Grow(len(v) + 2)
	b.WriteByte('\'')
	for i := 0; i < len(v); i++ {
		if c := v[i]; c == '\\' || c == '\'' {
			b.WriteByte('\\')
		}
		b.WriteByte(v[i])
	}
	b.WriteByte('\'')
	return b.String()
}

// URI renders the AMQP endpoint, percent-encoding the userinfo so a password
// containing a colon, slash, at-sign or space cannot change which host or
// vhost is addressed. It contains the password and must never be logged; use
// Summary for anything that reaches an output stream.
func (b Broker) URI() string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d%s", urlEscape(b.User), urlEscape(b.Password), b.Host, b.Port, normalizeVHost(b.VHost))
}

func normalizeVHost(v string) string {
	if v == "" || v == "/" {
		return "/"
	}
	if strings.HasPrefix(v, "/") {
		return "/" + urlEscape(strings.TrimPrefix(v, "/"))
	}
	return "/" + urlEscape(v)
}

func urlEscape(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '-', c == '_', c == '.', c == '~':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// Summary returns the loggable view of the configuration. It deliberately
// omits every credential; a test asserts that no secret value appears in it.
func (c Common) Summary() map[string]any {
	return map[string]any{
		"application":         string(c.Application),
		"instance":            c.Instance,
		"host":                c.Database.PrimaryHost,
		"port":                c.Database.PrimaryPort,
		"database":            c.Database.Name,
		"replica_host":        c.Database.ReplicaHost,
		"replica_port":        c.Database.ReplicaPort,
		"replica_required":    c.Database.ReplicaRequired,
		"tls":                 c.Database.SSLMode,
		"exchange":            c.Broker.Exchange,
		"queue":               c.Broker.Queue,
		"routing_key":         c.Broker.RoutingKey,
		"dlx":                 c.Broker.DeadLetterX,
		"dead_letter_queue":   c.Broker.DeadLetterQ,
		"vhost":               c.Broker.VHost,
		"max_attempts":        c.Broker.DeliveryLimit,
		"shutdown_timeout_ms": c.ShutdownTimeout.Milliseconds(),
		"addr":                c.HTTPAddr,
	}
}

// ErrNoConfig is returned when a required variable set is entirely absent.
var ErrNoConfig = errors.New("no configuration present in the environment")
