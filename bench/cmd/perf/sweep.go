package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/iodesystems/gwag/v2/bench/cmd/traffic/runner"
)

// Sweep is the wire shape one `perf run` invocation produces. The
// report writer consumes Sweeps verbatim — one Sweep per scenario.
type Sweep struct {
	Schema              string      `json:"schema"`
	Specs               HostSpecs   `json:"specs"`
	Scenario            string      `json:"scenario"`
	Target              string      `json:"target"`
	StartedAt           string      `json:"started_at"`
	FinishedAt          string      `json:"finished_at"`
	DurationSec         float64     `json:"duration_seconds_per_rep"`
	RepsPerStep         int         `json:"reps_per_step"`
	Warmup              bool        `json:"warmup_rep_discarded"`
	// UpstreamLatencyUs is the artificial per-call delay the operator
	// configured on the upstream service (greeter --delay or the
	// equivalent on the OpenAPI sibling). Metadata only — the perf
	// driver does not configure the backend; the operator does.
	// The report writer surfaces this as the "gateway adds X µs on top
	// of upstream Y µs" framing. Zero when no rung was configured.
	UpstreamLatencyUs   int64       `json:"upstream_latency_us"`
	Steps               []SweepStep `json:"steps"`
	Knee                *KneeInfo   `json:"knee,omitempty"`
	// Profile is the optional CPU + allocs pprof capture taken at
	// the recommended-ceiling RPS for this scenario. Populated by
	// the perf driver when --profile is enabled (default on for the
	// idiot-proof `bin/bench perf` path). Nil when capture was
	// skipped or the pprof endpoint was unreachable.
	Profile *ProfileCapture `json:"profile,omitempty"`
}

// SweepStep is one rung of the sweep — N reps at a single target RPS.
// Aggregates are computed over the non-warmup reps; the raw rep
// outputs are kept so the report (and a human looking at the JSON)
// can drill in.
type SweepStep struct {
	TargetRPS               int          `json:"target_rps"`
	Reps                    []TrafficRep `json:"reps"`
	AchievedRPSMean         float64      `json:"achieved_rps_mean"`
	MeanUsMedian            int64        `json:"client_mean_us_median"`
	P50UsMedian             int64        `json:"client_p50_us_median"`
	P95UsMedian             int64        `json:"client_p95_us_median"`
	P99UsMedian             int64        `json:"client_p99_us_median"`
	SelfMeanUsMedian        int64        `json:"gateway_self_mean_us_median,omitempty"`
	DispatchMeanUsMedian    int64        `json:"gateway_dispatch_mean_us_median,omitempty"`
}

// TrafficRep is a thin wrapper around the JSON the traffic binary
// emits. We preserve it verbatim so a sweep file is forensically
// complete — anything the report or a human needs is in here.
type TrafficRep struct {
	RepIndex int             `json:"rep_index"`
	IsWarmup bool            `json:"is_warmup"`
	Output   json.RawMessage `json:"output"`
}

// KneeInfo records where the sweep stopped and why. Three predicates
// are evaluated after each step:
//
//   - achieved-below-80%: AchievedRPSMean < 0.8 × TargetRPS — the
//     load offered ran past what the bench client + gateway +
//     upstream could absorb; throughput collapsed.
//   - p99-cliff: P99UsMedian > 2× prior step *AND* AchievedRPSMean
//     ≤ prior step's AchievedRPSMean — i.e. latency doubled while
//     throughput stopped climbing. Catches the case where the
//     gateway saturates by going slow rather than dropping requests.
//   - latency-above-50ms: P99UsMedian > 50,000 (50ms) — absolute
//     latency-SLA fail. Catches the case where throughput keeps
//     climbing past the gateway's healthy zone but p99 has
//     deteriorated past what any operator would deploy on. Without
//     this, sweeps that push 60–100k target see throughput climb
//     to "achieved 95% of target at 100ms p99" and the predicates
//     fail to flag it. 50ms is a common production-SLA ceiling;
//     operators tuning differently override via --knee-p99-max.
//
// First-to-fire wins. KneeRPS is the *prior* (still healthy) step —
// the recommended ceiling. FailedAtRPS is the first failing step.
// Reason is a stable enum so the report writer can branch on it
// without parsing prose; Detail is the human-readable numbers.
type KneeInfo struct {
	KneeRPS     int    `json:"knee_rps"`
	FailedAtRPS int    `json:"failed_at_rps"`
	Reason      string `json:"reason"`
	Detail      string `json:"detail"`
}

// Knee predicate identifiers — exposed so tests + the report writer
// can match without literal duplication.
const (
	KneeReasonAchievedBelow80 = "achieved_below_80pct"
	KneeReasonP99Cliff        = "p99_cliff"
	KneeReasonLatencyAbove50  = "latency_above_50ms"
	KneeAchievedRatio         = 0.80
	KneeP99Multiplier         = 2.0
	KneeP99MaxUs              = int64(50_000) // 50ms
)

// SweepSchemaVersion identifies the on-disk shape so the report
// writer can refuse a snapshot it doesn't understand.
const SweepSchemaVersion = "bench-perf-sweep/v1"

// runSweep implements `perf run`. Drives bench/.run/bin/traffic at
// escalating target RPS, captures the per-rep --json sidecar, and
// emits a Sweep summary (stdout table + on-disk JSON).
func runSweep(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	target := fs.String("target", "http://localhost:18080/api/graphql", "gateway GraphQL endpoint (used for traffic + /api/metrics)")
	scenario := fs.String("scenario", "graphql", "report label for this sweep (graphql/openapi/mixed/...)")
	stepsRaw := fs.String("steps", "1000,5000,10000,30000,60000,100000", "comma-separated target RPS rungs")
	duration := fs.Duration("duration", 10*time.Second, "duration of each rep")
	reps := fs.Int("reps", 3, "reps per step; rep 1 is the warm-up and excluded from aggregates unless --no-warmup")
	noWarmup := fs.Bool("no-warmup", false, "treat every rep as data (default discards rep 1)")
	outPath := fs.String("out", "bench/.run/perf/sweep.json", "where to write the Sweep JSON; '-' for stdout")
	keepReps := fs.Bool("keep-reps", false, "preserve per-rep traffic --json sidecars under <out>.reps/; default cleans them after aggregation")
	noKnee := fs.Bool("no-knee", false, "run every step to completion even when knee predicates fire (default stops at first knee)")
	upstreamLatency := fs.Duration("upstream-latency", 0, "metadata: artificial upstream delay configured by the operator on the backend (e.g. greeter --delay 100us). Stamped into the Sweep JSON so the report can present \"gateway adds X µs on top of upstream Y µs\". The perf driver does NOT configure the backend; bring up your service with the matching --delay before running.")
	query := fs.String("query", "", "GraphQL query string; defaults to the --scenario preset's query")
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: perf run [flags]")
		fs.PrintDefaults()
		fmt.Fprintln(fs.Output())
		fmt.Fprint(fs.Output(), scenarioHelp())
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("unexpected positional args: %v", fs.Args())
	}

	// Resolve --query from the scenario preset when the caller
	// didn't pin one. A preset miss with an empty --query is an
	// error: there's nothing to fire.
	if *query == "" {
		if p := scenarioPresetByName(*scenario); p != nil {
			*query = p.query
		} else {
			return fmt.Errorf("--scenario=%q has no built-in query preset; either choose one of [%s] or pass --query explicitly",
				*scenario, strings.Join(scenarioPresetNames(), ", "))
		}
	}

	steps, err := parseSteps(*stepsRaw)
	if err != nil {
		return err
	}
	if *reps < 1 {
		return fmt.Errorf("--reps must be ≥ 1")
	}

	// Build the runner target ONCE with an HTTP-client pool sized for
	// the largest rung in the sweep. Reusing this target across every
	// step and every rep means one keep-alive pool persists for the
	// whole scenario — without it, every rep spawned a fresh process
	// (or fresh client) whose connections all dropped into TIME_WAIT
	// on exit, exhausting the ~28K ephemeral-port range by the time
	// the sweep hit the 40k–60k rungs. The "regression" you saw at
	// the upper rungs was that TIME_WAIT cliff, not gateway code.
	maxRPS := steps[0]
	for _, s := range steps {
		if s > maxRPS {
			maxRPS = s
		}
	}
	poolConcurrency := runner.ResolveConcurrency(maxRPS, 0)
	body, err := json.Marshal(map[string]any{"query": *query})
	if err != nil {
		return fmt.Errorf("marshal query: %w", err)
	}
	sharedTarget := runner.Target{
		Label:      *target,
		MetricsURL: runner.MetricsURLFromGateway(*target),
		Fire:       runner.MakeGraphQLFire(5*time.Second, poolConcurrency, *target, body),
	}

	sw := Sweep{
		Schema:            SweepSchemaVersion,
		Specs:             CollectSpecs(),
		Scenario:          *scenario,
		Target:            *target,
		StartedAt:         time.Now().UTC().Format(time.RFC3339),
		DurationSec:       duration.Seconds(),
		RepsPerStep:       *reps,
		Warmup:            !*noWarmup && *reps > 1,
		UpstreamLatencyUs: upstreamLatency.Microseconds(),
	}

	repsDir := strings.TrimSuffix(*outPath, ".json") + ".reps"
	if *outPath == "-" {
		repsDir, err = os.MkdirTemp("", "perf-reps-")
		if err != nil {
			return err
		}
	} else {
		if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil {
			return err
		}
		if err := os.MkdirAll(repsDir, 0o755); err != nil {
			return err
		}
	}
	defer func() {
		if !*keepReps {
			os.RemoveAll(repsDir)
		}
	}()

	for _, rps := range steps {
		step, err := runStep(sharedTarget, rps, *duration, *reps, sw.Warmup, repsDir)
		if err != nil {
			return fmt.Errorf("step rps=%d: %w", rps, err)
		}
		sw.Steps = append(sw.Steps, step)
		// Stream-print the step's aggregate row as it finishes so a
		// long sweep gives the operator feedback without buffering.
		printStepRow(os.Stdout, step)
		if !*noKnee {
			if knee, hit := detectKnee(sw.Steps); hit {
				sw.Knee = &knee
				fmt.Fprintf(os.Stderr, "knee detected at rps=%d (%s); stopping sweep. Pass --no-knee to run remaining steps.\n",
					knee.FailedAtRPS, knee.Reason)
				break
			}
		}
	}

	sw.FinishedAt = time.Now().UTC().Format(time.RFC3339)

	if err := writeSweep(*outPath, sw); err != nil {
		return err
	}
	if *outPath != "-" {
		fmt.Fprintf(os.Stderr, "wrote %s\n", *outPath)
		if *keepReps {
			fmt.Fprintf(os.Stderr, "per-rep sidecars kept at %s\n", repsDir)
		}
	}
	return nil
}

// runStep drives one (target-rps) rung — runs `reps` runner.Run
// passes against the shared Target, aggregates the non-warmup reps,
// and persists each rep's JSON shape under repsDir.
//
// The sharedTarget is the single source of HTTP keep-alive pool for
// the whole sweep; do not rebuild it per step.
func runStep(sharedTarget runner.Target, rps int, dur time.Duration, reps int, warmup bool, repsDir string) (SweepStep, error) {
	step := SweepStep{TargetRPS: rps}
	type aggInputs struct {
		achievedRPS []float64
		meanUs      []int64
		p50, p95, p99 []int64
		selfMeanUs  []int64
		dispMeanUs  []int64
	}
	var inputs aggInputs
	opts := runner.Options{
		RPS:           rps,
		Duration:      dur,
		ServerMetrics: true,
	}
	for r := 1; r <= reps; r++ {
		res, err := runner.Run(opts, "", []runner.Target{sharedTarget})
		if err != nil {
			return step, fmt.Errorf("rep %d/%d run: %w", r, reps, err)
		}
		var buf bytes.Buffer
		if err := runner.WriteJSON(&buf, opts, res); err != nil {
			return step, fmt.Errorf("rep %d/%d encode: %w", r, reps, err)
		}
		raw := buf.Bytes()
		repPath := filepath.Join(repsDir, fmt.Sprintf("rps-%d-rep-%d.json", rps, r))
		if err := os.WriteFile(repPath, raw, 0o644); err != nil {
			return step, fmt.Errorf("rep %d/%d write sidecar: %w", r, reps, err)
		}
		isWarmup := warmup && r == 1
		step.Reps = append(step.Reps, TrafficRep{
			RepIndex: r,
			IsWarmup: isWarmup,
			Output:   raw,
		})
		if isWarmup {
			continue
		}
		td, err := parseTrafficOutput(raw)
		if err != nil {
			return step, fmt.Errorf("rep %d/%d parse: %w", r, reps, err)
		}
		inputs.achievedRPS = append(inputs.achievedRPS, td.achievedRPS)
		inputs.meanUs = append(inputs.meanUs, td.meanUs)
		inputs.p50 = append(inputs.p50, td.p50)
		inputs.p95 = append(inputs.p95, td.p95)
		inputs.p99 = append(inputs.p99, td.p99)
		if td.selfMeanUs > 0 {
			inputs.selfMeanUs = append(inputs.selfMeanUs, td.selfMeanUs)
		}
		if td.dispMeanUs > 0 {
			inputs.dispMeanUs = append(inputs.dispMeanUs, td.dispMeanUs)
		}
	}
	step.AchievedRPSMean = mean(inputs.achievedRPS)
	step.MeanUsMedian = medianInt64(inputs.meanUs)
	step.P50UsMedian = medianInt64(inputs.p50)
	step.P95UsMedian = medianInt64(inputs.p95)
	step.P99UsMedian = medianInt64(inputs.p99)
	step.SelfMeanUsMedian = medianInt64(inputs.selfMeanUs)
	step.DispatchMeanUsMedian = medianInt64(inputs.dispMeanUs)
	return step, nil
}

// trafficData is the subset of bench-traffic/v1 the sweep driver
// aggregates per step. Decoupled from the runner.JSONOutput type so
// a future bench-traffic/v2 can introduce additive fields without
// the sweep driver caring.
type trafficData struct {
	achievedRPS float64
	meanUs      int64
	p50, p95, p99 int64
	selfMeanUs  int64
	dispMeanUs  int64
}

func parseTrafficOutput(raw []byte) (trafficData, error) {
	var v struct {
		Schema  string `json:"schema"`
		Targets []struct {
			OK          uint64  `json:"ok"`
			AchievedRPS float64 `json:"achieved_rps"`
			MeanUs      int64   `json:"mean_us"`
			P50Us       int64   `json:"p50_us"`
			P95Us       int64   `json:"p95_us"`
			P99Us       int64   `json:"p99_us"`
		} `json:"targets"`
		Gateway *struct {
			Dispatches []struct {
				Count  uint64 `json:"count"`
				MeanUs int64  `json:"mean_us"`
			} `json:"dispatches"`
			Ingress map[string]struct {
				SelfMeanUs int64 `json:"self_mean_us"`
			} `json:"ingress"`
		} `json:"gateway"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return trafficData{}, err
	}
	if v.Schema != "" && !strings.HasPrefix(v.Schema, "bench-traffic/v1") {
		return trafficData{}, fmt.Errorf("unexpected schema %q (sweep driver knows bench-traffic/v1)", v.Schema)
	}
	if len(v.Targets) == 0 {
		return trafficData{}, errors.New("no targets in traffic JSON")
	}
	t := v.Targets[0]
	td := trafficData{
		achievedRPS: t.AchievedRPS,
		meanUs:      t.MeanUs,
		p50:         t.P50Us,
		p95:         t.P95Us,
		p99:         t.P99Us,
	}
	if v.Gateway != nil {
		// Ingress mean: pool across ingresses by weighting on count.
		// Sweep driver runs only graphql today, so this collapses to
		// one entry — but the math stays right when openapi/grpc
		// scenarios join.
		var sum, count int64
		for _, ing := range v.Gateway.Ingress {
			sum += ing.SelfMeanUs
			count++
		}
		if count > 0 {
			td.selfMeanUs = sum / count
		}
		var dSum, dCount int64
		for _, d := range v.Gateway.Dispatches {
			dSum += d.MeanUs * int64(d.Count)
			dCount += int64(d.Count)
		}
		if dCount > 0 {
			td.dispMeanUs = dSum / dCount
		}
	}
	return td, nil
}

func parseSteps(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("--steps: %q is not an integer", p)
		}
		if n <= 0 {
			return nil, fmt.Errorf("--steps: %d is not > 0", n)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, errors.New("--steps cannot be empty")
	}
	return out, nil
}

// ensureTrafficBinary returns the path to bench/.run/bin/traffic,
// building it if absent. The path is resolved relative to the perf
// binary's CWD (which is the repo root when invoked via bin/bench).
// Mirrors cmd_traffic in bin/bench.
func ensureTrafficBinary() (string, error) {
	bin := filepath.Join("bench", ".run", "bin", "traffic")
	if _, err := os.Stat(bin); err == nil {
		return bin, nil
	}
	fmt.Fprintln(os.Stderr, "==> building traffic generator (bench/.run/bin/traffic)")
	if err := os.MkdirAll(filepath.Dir(bin), 0o755); err != nil {
		return "", err
	}
	cmd := exec.Command("go", "build", "-o", bin, "./bench/cmd/traffic")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("go build traffic: %w", err)
	}
	return bin, nil
}

func writeSweep(path string, sw Sweep) error {
	var w *os.File
	if path == "-" {
		w = os.Stdout
	} else {
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		defer f.Close()
		w = f
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(sw)
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var sum float64
	for _, x := range xs {
		sum += x
	}
	return sum / float64(len(xs))
}

func medianInt64(xs []int64) int64 {
	if len(xs) == 0 {
		return 0
	}
	cp := slices.Clone(xs)
	slices.Sort(cp)
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2
}

// printStepRow appends one fixed-width row to stdout. tabwriter
// resets column widths per Flush so streaming rows with tabwriter
// only align within one call; fixed widths keep the table readable
// as the sweep progresses without buffering rows to end-of-sweep.
var stepHeaderPrinted bool

const stepRowFormat = "%-10s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s\n"

func printStepRow(w *os.File, step SweepStep) {
	if !stepHeaderPrinted {
		fmt.Fprintf(w, stepRowFormat,
			"TARGET_RPS", "ACHIEVED", "MEAN", "P50", "P95", "P99", "SELF_MEAN", "DISPATCH_MEAN")
		stepHeaderPrinted = true
	}
	fmt.Fprintf(w, stepRowFormat,
		strconv.Itoa(step.TargetRPS),
		fmt.Sprintf("%.0f", step.AchievedRPSMean),
		usToStr(step.MeanUsMedian),
		usToStr(step.P50UsMedian),
		usToStr(step.P95UsMedian),
		usToStr(step.P99UsMedian),
		usToStr(step.SelfMeanUsMedian),
		usToStr(step.DispatchMeanUsMedian),
	)
}

// detectKnee evaluates the two stop predicates against the latest
// step and returns (kneeInfo, true) when one fires. Called after
// each step lands; first-firing predicate wins.
//
// KneeRPS is the *prior* step's TargetRPS — the highest rung that
// still passed both rules. When the very first step fails, KneeRPS
// is 0 to signal "every rung tested was beyond the knee".
func detectKnee(steps []SweepStep) (KneeInfo, bool) {
	if len(steps) == 0 {
		return KneeInfo{}, false
	}
	cur := steps[len(steps)-1]
	prevRPS := 0
	var prevP99 int64
	var prevAchieved float64
	if len(steps) >= 2 {
		prev := steps[len(steps)-2]
		prevRPS = prev.TargetRPS
		prevP99 = prev.P99UsMedian
		prevAchieved = prev.AchievedRPSMean
	}
	// Predicate 1: achieved-below-80% (target ratio).
	if cur.AchievedRPSMean < KneeAchievedRatio*float64(cur.TargetRPS) {
		ratio := 0.0
		if cur.TargetRPS > 0 {
			ratio = cur.AchievedRPSMean / float64(cur.TargetRPS)
		}
		return KneeInfo{
			KneeRPS:     prevRPS,
			FailedAtRPS: cur.TargetRPS,
			Reason:      KneeReasonAchievedBelow80,
			Detail: fmt.Sprintf("achieved %.0f / %d target (%.0f%% < %.0f%% threshold)",
				cur.AchievedRPSMean, cur.TargetRPS, ratio*100, KneeAchievedRatio*100),
		}, true
	}
	// Predicate 2: p99 cliff — p99 doubled *and* achieved RPS no
	// longer climbs vs the prior step. Both conditions matter:
	// latency naturally climbs as load nears capacity (queueing),
	// so latency-doubled-alone fires too eagerly on healthy ramps
	// (e.g. 1k → 5k where p99 doubles but throughput tracks target
	// perfectly). Saturation-via-latency only matters when
	// throughput also stops scaling.
	if prevP99 > 0 && prevAchieved > 0 &&
		float64(cur.P99UsMedian) > KneeP99Multiplier*float64(prevP99) &&
		cur.AchievedRPSMean <= prevAchieved {
		return KneeInfo{
			KneeRPS:     prevRPS,
			FailedAtRPS: cur.TargetRPS,
			Reason:      KneeReasonP99Cliff,
			Detail: fmt.Sprintf("p99 %dµs vs prior %dµs (×%.1f, >×%.1f threshold) AND achieved %.0f did not exceed prior %.0f",
				cur.P99UsMedian, prevP99, float64(cur.P99UsMedian)/float64(prevP99), KneeP99Multiplier,
				cur.AchievedRPSMean, prevAchieved),
		}, true
	}
	// Predicate 3: absolute p99 above the SLA ceiling. Catches the
	// "throughput keeps climbing but only at 100ms+ p99" case the
	// first two predicates miss — the bench keeps absorbing
	// requests, just slowly. Default 50ms reflects a common
	// production p99 SLA.
	if cur.P99UsMedian > KneeP99MaxUs {
		return KneeInfo{
			KneeRPS:     prevRPS,
			FailedAtRPS: cur.TargetRPS,
			Reason:      KneeReasonLatencyAbove50,
			Detail: fmt.Sprintf("p99 %dµs (%.1fms) exceeds %dms SLA ceiling",
				cur.P99UsMedian, float64(cur.P99UsMedian)/1000, KneeP99MaxUs/1000),
		}, true
	}
	return KneeInfo{}, false
}

func usToStr(us int64) string {
	if us <= 0 {
		return "-"
	}
	if us < 1000 {
		return fmt.Sprintf("%dµs", us)
	}
	return fmt.Sprintf("%.2fms", float64(us)/1000)
}
