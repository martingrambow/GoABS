package bench

import (
	"context"
	"encoding/csv"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sealuzh/goabs/data"
	"github.com/sealuzh/goabs/utils/executil"
)

const (
	cmdName           = "go"
	cmdArgsTest       = "test"
	cmdArgsBench      = "-bench=^%s$"
	cmdArgsBenchTime  = "-benchtime=%s"
	cmdArgsCount      = "-count=%d"
	cmdArgsNoTests    = "-run=^$"
	cmdArgsTimeout    = "-timeout=%s"
	cmdArgsMem        = "-benchmem"
	cmdArgsProfileOut = "-outputdir=%s"
	cmdArgsCPUProfile = "-cpuprofile=%s"
	cmdArgsMemProfile = "-memprofile=%s"
	benchRuntime      = 1
	benchTimeoutMsg   = "*** Test killed with quit: ran too long"
)

type Runner interface {
	Run(ctx context.Context, run int, test string) (int, error)
}

// NewRunner creates a new benchmark runner.
// By default it returns a penalised runner that in consecutive runs only executes successful benchmark executions.
func NewRunner(goRoot, projectRoot string, project2Root string, benchs data.PackageMap, wi int, mi int, timeout, benchTime, benchDuration, runDuration time.Duration, benchMem bool, profile data.Profile, profileDir string, out csv.Writer) (Runner, error) {
	// if benchmark gets executed over time period, do not do warm-up iterations
	if benchDuration > 0 {
		wi = 0
	}

	// if no measurement iterations are provided, always do at least one measurement
	if mi == 0 {
		mi = 1
	}

	cmdCount := fmt.Sprintf(cmdArgsCount, (wi + mi))
	cmdArgs := []string{cmdArgsTest, fmt.Sprintf(cmdArgsBenchTime, benchTime), fmt.Sprintf(cmdArgsTimeout, timeout), cmdCount, cmdArgsNoTests}

	var rp resultParser = rtResultParser{}
	if benchMem {
		cmdArgs = append(cmdArgs, cmdArgsMem)
		rp = memResultParser{}
	}

	return &runnerWithPenalty{
		defaultRunner: defaultRunner{
			projectRoot:    projectRoot,
			project2Root:   project2Root,
			wi:             wi,
			mi:             mi,
			benchDuration:  benchDuration,
			runDuration:    runDuration,
			benchMem:       benchMem,
			resultParser:   rp,
			out:            out,
			benchs:         benchs,
			profile:        profile,
			profileDir:     profileDir,
			env:            executil.Env(goRoot, executil.GoPath(projectRoot)),
			env2:           executil.Env(goRoot, executil.GoPath(project2Root)),
			cmdCount:       cmdCount,
			cmdArgs:        cmdArgs,
		},
		penalisedBenchs: make(map[string]struct{}),
		timeout:         timeout,
	}, nil
}

type defaultRunner struct {
	projectRoot    string
	project2Root   string
	wi             int
	mi             int
	benchDuration  time.Duration
	benchMem       bool
	runDuration    time.Duration
	resultParser   resultParser
	out            csv.Writer
	benchs         data.PackageMap
	profile        data.Profile
	profileDir     string
	env            []string
	env2           []string
	cmdCount       string
	cmdArgs        []string
}

type runnerWithPenalty struct {
	defaultRunner
	timeout         time.Duration
	penalisedBenchs map[string]struct{}
}

type rmitBenchmark struct {
	dir1   string
	dir2   string
	bench  data.Function
}

func (r *runnerWithPenalty) RunBenchmark(ctx context.Context, bench rmitBenchmark, run int, suiteExec int, test string) (int, error) {
	if r.benchDuration != 0 {
		startBench := time.Now()
		benchCount := 0
		for time.Since(startBench).Seconds() < r.benchDuration.Seconds() {
			exec, err := r.RunBenchmarkOnce(ctx, bench, run, suiteExec, benchCount, test)
			if err != nil || !exec {
				return benchCount, err
			}
			benchCount++

			select {
			case <-ctx.Done():
				return benchCount, ctx.Err()
			default:
			}
		}
		return benchCount, nil
	}

	// no benchmark duration supplied -> only one benchmark execution
	exec, err := r.RunBenchmarkOnce(ctx, bench, run, suiteExec, 0, test)
	if exec {
		fmt.Printf("Benchmark run succesful\n")
		return 1, err
	}
	fmt.Printf("Benchmark run failed\n")
	return 0, err

}

func (r *runnerWithPenalty) RunBenchmarkOnce(ctx context.Context, bench rmitBenchmark, run int, suiteExec int, benchExec int, test string) (bool, error) {
	fmt.Printf("### Execute Benchmark: %s/%s::%s\n", bench.bench.Pkg, bench.bench.File, bench.bench.Name)
	args := append(r.cmdArgs, fmt.Sprintf(cmdArgsBench, bench.bench.Name))
	// add profile if necessary
	if r.profile != data.NoProfile {
		args = r.profileCmdArgs(args, bench.bench, run, suiteExec, benchExec, test)
	}

	if bench.dir2 == "" {

		fmt.Printf("Changing directory to dir1: %s\n", bench.dir1)
		err := os.Chdir(bench.dir1)
		if err != nil {
			fmt.Printf("Error while changing directory to dir1: %s\n %s\n", bench.dir1, err)
		}
		r.RunOneVersion(ctx, bench.bench, run, suiteExec, benchExec, test, 1, r.env)

	} else {

		x := rand.Intn(2) //x is 0 or 1
		fmt.Printf("x is %d\n", x)

		if x == 0 {
			fmt.Printf("Changing directory to dir1: %s\n", bench.dir1)
			err := os.Chdir(bench.dir1)
			if err != nil {
				fmt.Printf("Error while changing directory to dir1: %s\n %s\n", bench.dir1, err)
			}
			r.RunOneVersion(ctx, bench.bench, run, suiteExec, benchExec, test, 1, r.env)
		}

		fmt.Printf("Changing directory to dir2: %s\n", bench.dir2)
		err := os.Chdir(bench.dir2)
		if err != nil {
			fmt.Printf("Error while changing directory to dir2: %s\n %s\n", bench.dir2, err)
		}
		r.RunOneVersion(ctx, bench.bench, run, suiteExec, benchExec, test, 2, r.env2)

		if x > 0 {
			fmt.Printf("Changing directory to dir1: %s\n", bench.dir1)
			err := os.Chdir(bench.dir1)
			if err != nil {
				fmt.Printf("Error while changing directory to dir1: %s\n %s\n", bench.dir1, err)
			}
			r.RunOneVersion(ctx, bench.bench, run, suiteExec, benchExec, test, 1, r.env)
		}
	}

	return true, nil
}

func (r *runnerWithPenalty) RunOneVersion(ctx context.Context, bench data.Function, run int, suiteExec int, benchExec int, test string, version int, env []string) (bool, error) {
	relBenchName := fmt.Sprintf("%s/%s::%s", bench.Pkg, bench.File, bench.Name)
	args := append(r.cmdArgs, fmt.Sprintf(cmdArgsBench, bench.Name))

	c := exec.Command(executil.GoCommand(env), args...)
	c.Env = env

	res, err := c.CombinedOutput()
	resStr := string(res)
	if err != nil {
		fmt.Printf("Error while executing command '%s\n", c.Args)
		if strings.Contains(resStr, benchTimeoutMsg) {
			fmt.Printf("%s timed out after %s\n", relBenchName, r.timeout)
			r.penalisedBenchs[relBenchName] = struct{}{}
			err = nil
			return false, nil
		}
		fmt.Printf("Cmd Error:\n%s\n", resStr)
		return false, err
	}

	fmt.Printf("Benchmark %d done: %s\n", version, resStr)

	result, err := r.resultParser.parse(resStr)
	if err != nil {
		if _, ok := err.(resultNotParsable); ok {
			fmt.Printf("%s result could not be parsed\n", relBenchName)
			r.penalisedBenchs[relBenchName] = struct{}{}
			return false, nil
		}
		fmt.Printf("Parse Error:\n%s\n", resStr)
		return false, err
	}
	fmt.Printf("Result %d parsed\n", version)

	saveBenchOut(test, run, suiteExec, benchExec, bench, result, r.out, r.benchMem, version)
	return true, nil
}

func (r *runnerWithPenalty) RunOnce(ctx context.Context, run int, test string) (int, error) {
	benchCount := 0

	//Collect benchmarks
	var benchs []rmitBenchmark

	for pkgName, pkg := range r.benchs {
		fmt.Printf("# Collect Benchmarks in Dir: %s\n", pkgName)

		dir1 := filepath.Join(r.projectRoot, pkgName)
		dir2 := ""
		if r.project2Root != "" {
			dir2 = filepath.Join(r.project2Root, pkgName)
		}

		for fileName, file := range pkg {
			fmt.Printf("## Collect Benchmarks in File: %s\n", fileName)
			for _, bench := range file{
				benchs = append(benchs, rmitBenchmark{dir1, dir2, bench})
			}
		}
	}

	//Shuffle benchmarks
	fmt.Printf("#####################\n")
	fmt.Printf("## Shuffle Benchmarks\n")
	rand.Seed(time.Now().Unix())
	rand.Shuffle(len(benchs), func(i, j int) {
		benchs[i], benchs[j] = benchs[j], benchs[i]
	})
	fmt.Printf("#####################\n")

	//run benchmarks
	for _, b := range benchs {
		executed, err := r.RunBenchmark(ctx, b, run, 0, test)
		if err != nil {
			return benchCount, err
		}
		benchCount += executed
		fmt.Printf("%d / %d done\n", benchCount, len(benchs))

		select {
		case <-ctx.Done():
			return benchCount, ctx.Err()
		default:
		}
	}

	return benchCount, nil
}

func (r *runnerWithPenalty) Run(ctx context.Context, run int, test string) (int, error) {
	return r.RunOnce(ctx, run, test)
}

func (r *runnerWithPenalty) profileCmdArgs(args []string, bench data.Function, run int, suiteExec int, benchExec int, test string) []string {
	cmdProfileOut := fmt.Sprintf(cmdArgsProfileOut, r.profileDir)
	cpuPath := profileName(bench, run, suiteExec, benchExec, test, "cpu.pprof")
	cpuArg := fmt.Sprintf(cmdArgsCPUProfile, cpuPath)
	memPath := profileName(bench, run, suiteExec, benchExec, test, "mem.pprof")
	memArg := fmt.Sprintf(cmdArgsMemProfile, memPath)
	switch r.profile {
	case data.AllProfiles:
		args = append(args, cmdProfileOut, cpuArg, memArg)
	case data.CPUProfile:
		args = append(args, cmdProfileOut, cpuArg)
	case data.MemProfile:
		args = append(args, cmdProfileOut, memArg)
	}
	return args
}

func TimedRun(ctx context.Context, r Runner, run int, test string) (int, error, time.Duration) {
	now := time.Now()
	execBenchs, err := r.Run(ctx, run, test)
	dur := time.Since(now)
	return execBenchs, err, dur
}

func profileName(bench data.Function, run int, suiteExec int, benchExec int, test string, t string) string {
	return fmt.Sprintf("%d-%d-%d_%s_%s_%s_%s", run, suiteExec, benchExec, replaceSlashes(test), replaceSlashes(bench.Pkg), bench.Name, t)
}

func replaceSlashes(p string) string {
	if len(p) == 0 {
		return ""
	}

	// remove leading /
	if strings.HasPrefix(p, "/") {
		p = p[1:]
	}
	if strings.HasSuffix(p, "/") {
		p = p[:len(p)-1]
	}
	return strings.Replace(p, "/", "-", -1)
}

func saveBenchOut(test string, run int, suiteExec int, benchExec int, b data.Function, res []result, out csv.Writer, benchMem bool, version int) {
	outSize := 7
	if benchMem {
		outSize += 2
	}

	for _, result := range res {
		rec := make([]string, 0, outSize)
		rec = append(rec, fmt.Sprintf("%d-%d-%d", run, suiteExec, benchExec))
		rec = append(rec, test)
		rec = append(rec, filepath.Join(b.Pkg, b.File, b.Name))
		rec = append(rec, result.benchName)
		rec = append(rec, strconv.Itoa(version))
		rec = append(rec, strconv.Itoa(result.Invocations))
		rec = append(rec, strconv.FormatFloat(float64(result.Runtime), 'f', -1, 32))

		if benchMem {
			rec = append(rec, strconv.FormatInt(int64(result.Memory), 10))
			rec = append(rec, strconv.FormatInt(int64(result.Allocations), 10))
		}

		out.Write(rec)
		out.Flush()
	}
}
