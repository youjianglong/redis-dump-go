package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"github.com/yannh/redis-dump-go/pkg/config"
	"github.com/yannh/redis-dump-go/pkg/redisdump"
)

type progressLogger struct {
	stats map[uint8]int
}

func newProgressLogger() *progressLogger {
	return &progressLogger{
		stats: map[uint8]int{},
	}
}

func (p *progressLogger) drawProgress(to io.Writer, db uint8, nDumped int) {
	if _, ok := p.stats[db]; !ok && len(p.stats) > 0 {
		// We switched database, write to a new line
		fmt.Fprintf(to, "\n")
	}

	p.stats[db] = nDumped
	if nDumped == 0 {
		return
	}

	fmt.Fprintf(to, "\rDatabase %d: %d element dumped", db, nDumped)
}

type syncWriter struct {
	writer io.Writer
	mu     *sync.Mutex
}

func NewSyncWriter(writer io.Writer) *syncWriter {
	w := &syncWriter{}
	w.writer = writer
	w.mu = &sync.Mutex{}

	return w
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writer.Write(p)
}

func realMain() int {
	var err error

	c, outBuf, err := config.FromFlags(os.Args[0], os.Args[1:])
	if outBuf != "" {
		out := os.Stderr
		errCode := 1
		if c.Help {
			out = os.Stdout
			errCode = 0
		}
		fmt.Fprintln(out, outBuf)
		return errCode
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed parsing command line: %s\n", err.Error())
		return 1
	}

	var tlshandler *redisdump.TlsHandler = nil
	if c.Tls == true {
		tlshandler = redisdump.NewTlsHandler(c.CaCert, c.Cert, c.Key)
	}

	var serializer func([]string) []byte
	switch c.Format {
	case "resp":
		serializer = redisdump.RESPSerializer

	case "commands":
		serializer = redisdump.RedisCmdSerializer

	default:
		log.Fatalf("Failed parsing parameter flag: can only be resp or json")
	}

	var writer io.Writer
	if c.Output != "" {
		file, err := os.OpenFile(c.Output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
		if err != nil {
			log.Fatalf("Failed open file %s : %s", c.Output, err.Error())
		}
		defer file.Close()
		writer = file
	} else {
		writer = os.Stdout
	}

	progressNotifs := make(chan redisdump.ProgressNotification)
	var wg sync.WaitGroup
	wg.Add(1)

	defer func() {
		close(progressNotifs)
		wg.Wait()
		if !(c.Silent) {
			fmt.Fprint(os.Stderr, "\n")
		}
	}()

	pl := newProgressLogger()
	go func() {
		for n := range progressNotifs {
			if !(c.Silent) {
				pl.drawProgress(os.Stderr, n.Db, n.Done)
			}
		}
		wg.Done()
	}()

	var db = new(uint8)
	// If the user passed a db as parameter, we only dump that db
	if c.Db >= 0 {
		*db = uint8(c.Db)
	} else {
		db = redisdump.AllDBs
	}

	s := redisdump.Host{
		Host:       c.Host,
		Port:       c.Port,
		User:       c.User,
		Password:   c.Password,
		TlsHandler: tlshandler,
	}

	if err = redisdump.DumpServer(s, db, c.Filter, c.NWorkers, c.WithTTL, c.BatchSize, c.Noscan, NewSyncWriter(writer), serializer, progressNotifs); err != nil {
		fmt.Fprintf(os.Stderr, "%s", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(realMain())
}
