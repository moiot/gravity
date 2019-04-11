package logutil

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"strconv"

	"runtime/pprof"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

var PipelineName = "unknown"

const (
	defaultLogTimeFormat = time.RFC3339
	defaultLogMaxSize    = 300 // MB
	defaultLogFormat     = "json"
	defaultLogLevel      = log.InfoLevel
)

// FileLogConfig serializes file log related config in toml/json.
type FileLogConfig struct {
	// Filename Log filename, leave empty to disable file log.
	Filename string `toml:"filename" json:"filename"`
	// LogRotate Is log rotate enabled. TODO.
	LogRotate bool `toml:"log-rotate" json:"log-rotate"`
	// MaxSize size for a single file, in MB.
	MaxSize int `toml:"max-size" json:"max-size"`
	// MaxDays log keep days, default is never deleting.
	MaxDays int `toml:"max-days" json:"max-days"`
	// MaxBackups Maximum number of old log files to retain.
	MaxBackups int `toml:"max-backups" json:"max-backups"`
	// Compress determines if the rotated log files should be compressed
	// using gzip. The default is not to perform compression.
	Compress bool `toml:"compress" json:"compress"`
}

// LogConfig serializes log related config in toml/json.
type LogConfig struct {
	// Level Log level.
	Level string `toml:"level" json:"level"`
	// Format Log format. one of json, text, or console.
	Format string `toml:"format" json:"format"`
	// DisableTimestamp Disable automatic timestamps in output.
	DisableTimestamp bool `toml:"disable-timestamp" json:"disable-timestamp"`
	// File log config.
	File FileLogConfig `toml:"file" json:"file"`
	// SlowQueryFile filename, default to File log config on empty.
	SlowQueryFile string
}

// isSkippedPackageName tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	return strings.Contains(name, "github.com/sirupsen/logrus") ||
		strings.Contains(name, "github.com/coreos/pkg/capnslog")
}

type contextHook struct{}

// Fire implements logrus.Hook interface
// https://github.com/sirupsen/logrus/issues/63
func (hook *contextHook) Fire(entry *log.Entry) error {
	pc := make([]uintptr, 3)
	cnt := runtime.Callers(6, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = path.Base(file)
			entry.Data["line"] = line
			if PipelineName != "" {
				entry.Data["pipeline"] = PipelineName
			}
			break
		}
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (hook *contextHook) Levels() []log.Level {
	return log.AllLevels
}

type TestHook struct{}

// Fire implements logrus.Hook interface
// https://github.com/sirupsen/logrus/issues/63
func (hook *TestHook) Fire(entry *log.Entry) error {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idField := strings.Fields(strings.TrimPrefix(string(buf[:n]), "goroutine "))[0]
	id, err := strconv.Atoi(idField)
	if err != nil {
		return err
	}
	entry.Data["goroutine"] = id
	if entry.Level <= log.FatalLevel {
		pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (hook *TestHook) Levels() []log.Level {
	return log.AllLevels
}

func stringToLogLevel(level string) log.Level {
	switch strings.ToLower(level) {
	case "fatal":
		return log.FatalLevel
	case "error":
		return log.ErrorLevel
	case "warn", "warning":
		return log.WarnLevel
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	}
	return defaultLogLevel
}

// logTypeToColor converts the Level to a color string.
func logTypeToColor(level log.Level) string {
	switch level {
	case log.DebugLevel:
		return "[0;37"
	case log.InfoLevel:
		return "[0;36"
	case log.WarnLevel:
		return "[0;33"
	case log.ErrorLevel:
		return "[0;31"
	case log.FatalLevel:
		return "[0;31"
	case log.PanicLevel:
		return "[0;31"
	}

	return "[0;37"
}

// textFormatter is for compatability with ngaut/log
type textFormatter struct {
	DisableTimestamp bool
	EnableColors     bool
}

// Format implements logrus.Formatter
func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}

	if f.EnableColors {
		colorStr := logTypeToColor(entry.Level)
		fmt.Fprintf(b, "\033%sm ", colorStr)
	}

	if !f.DisableTimestamp {
		fmt.Fprintf(b, "%s ", entry.Time.Format(defaultLogTimeFormat))
	}
	if file, ok := entry.Data["file"]; ok {
		fmt.Fprintf(b, "%s:%v:", file, entry.Data["line"])
	}
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)
	for k, v := range entry.Data {
		if k != "file" && k != "line" {
			fmt.Fprintf(b, " %v=%v", k, v)
		}
	}
	b.WriteByte('\n')

	if f.EnableColors {
		b.WriteString("\033[0m")
	}
	return b.Bytes(), nil
}

func stringToLogFormatter(format string, disableTimestamp bool) log.Formatter {
	switch strings.ToLower(format) {
	case "text":
		return &textFormatter{
			DisableTimestamp: disableTimestamp,
		}
	case "json":
		return &log.JSONFormatter{
			TimestampFormat:  defaultLogTimeFormat,
			DisableTimestamp: disableTimestamp,
		}
	case "console":
		return &log.TextFormatter{
			FullTimestamp:    true,
			TimestampFormat:  defaultLogTimeFormat,
			DisableTimestamp: disableTimestamp,
		}
	case "highlight":
		return &textFormatter{
			DisableTimestamp: disableTimestamp,
			EnableColors:     true,
		}
	default:
		return &textFormatter{}
	}
}

// initFileLog initializes file based logging options.
func initFileLog(cfg *FileLogConfig, logger *log.Logger) error {
	if st, err := os.Stat(cfg.Filename); err == nil {
		if st.IsDir() {
			return errors.New("can't use directory as log file name")
		}
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}

	// use lumberjack to logrotate
	output := &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}

	if logger == nil {
		log.SetOutput(output)
	} else {
		logger.Out = output
	}
	return nil
}

// SlowQueryLogger is used to log slow query, InitLogger will modify it according to config file.
var SlowQueryLogger = log.StandardLogger()

func InitLogger(cfg *LogConfig) error {
	l := GetLogLevelFromEnv()
	if l == "" && cfg.Level == "" {
		l = "info"
	} else if l == "" && cfg.Level != "" {
		l = cfg.Level
	}

	log.SetLevel(stringToLogLevel(l))
	log.AddHook(&contextHook{})

	if cfg.Format == "" {
		cfg.Format = defaultLogFormat
	}
	formatter := stringToLogFormatter(cfg.Format, cfg.DisableTimestamp)
	log.SetFormatter(formatter)

	if len(cfg.File.Filename) != 0 {
		if err := initFileLog(&cfg.File, nil); err != nil {
			return errors.Trace(err)
		}
	}

	if len(cfg.SlowQueryFile) != 0 {
		SlowQueryLogger = log.New()
		tmp := cfg.File
		tmp.Filename = cfg.SlowQueryFile
		if err := initFileLog(&tmp, SlowQueryLogger); err != nil {
			return errors.Trace(err)
		}
		hooks := make(log.LevelHooks)
		hooks.Add(&contextHook{})
		SlowQueryLogger.Hooks = hooks
		SlowQueryLogger.Formatter = formatter
	}

	return nil
}

func MustInitLogger(cfg *LogConfig) {
	if err := InitLogger(cfg); err != nil {
		log.Fatalf("failed to init logger")
	}
}

func NewLogger(cfg *LogConfig) (*log.Logger, error) {
	var err error
	r := log.New()

	r.SetLevel(stringToLogLevel(cfg.Level))
	r.Formatter = stringToLogFormatter(cfg.Format, cfg.DisableTimestamp)

	if len(cfg.File.Filename) == 0 {
		return r, err
	}

	output, err := genFileLog(&cfg.File)

	if err != nil {
		return r, errors.Trace(err)
	}
	r.Out = output

	return r, nil
}

func genFileLog(cfg *FileLogConfig) (*lumberjack.Logger, error) {
	if st, err := os.Stat(cfg.Filename); err == nil {
		if st.IsDir() {
			return nil, errors.New("can't use directory as log file name")
		}
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}

	// use lumberjack to logrotate
	output := &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		Compress:   cfg.Compress,
		LocalTime:  true,
	}
	return output, nil
}

func GetLogLevelFromEnv() string {
	return os.Getenv("LOG_LEVEL")
}

func SetLogLevelFromEnv() {
	l := GetLogLevelFromEnv()
	if l == "" {
		l = "info"
	}
	level, _ := log.ParseLevel(l)
	log.SetLevel(level)
}
