package config

import (
	"bytes"
	"flag"
	"fmt"
	"os"
)

type Config struct {
	Host      string
	Port      int
	Password  string
	Db        int
	Filter    string
	Noscan    bool
	BatchSize int
	NWorkers  int
	WithTTL   bool
	Output    string
	Format    string
	Silent    bool
	Tls       bool
	CaCert    string
	Cert      string
	Key       string
	Help      bool
}

func isFlagPassed(flags *flag.FlagSet, name string) bool {
	found := false
	flags.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func FromFlags(progName string, args []string) (Config, string, error) {
	c := Config{}

	flags := flag.NewFlagSet(progName, flag.ContinueOnError)
	var outBuf bytes.Buffer
	flags.SetOutput(&outBuf)

	flags.StringVar(&c.Host, "host", "127.0.0.1", "Server host")
	flags.IntVar(&c.Port, "port", 6379, "Server port")
	flags.StringVar(&c.Password, "password", "", "Auth password")
	flags.IntVar(&c.Db, "db", -1, "only dump this database (default: all databases)")
	flags.StringVar(&c.Filter, "filter", "*", "Key filter to use")
	flags.BoolVar(&c.Noscan, "noscan", false, "Use KEYS * instead of SCAN - for Redis <=2.8")
	flags.IntVar(&c.BatchSize, "batchSize", 1000, "HSET/RPUSH/SADD/ZADD only add 'batchSize' items at a time")
	flags.IntVar(&c.NWorkers, "n", 10, "Parallel workers")
	flags.BoolVar(&c.WithTTL, "ttl", true, "Preserve Keys TTL")
	flags.StringVar(&c.Format, "fmt", "resp", "Output format - can be resp or commands")
	flags.StringVar(&c.Output, "out", "resp", "Output file path")
	flags.BoolVar(&c.Silent, "s", false, "Silent mode (disable logging of progress / stats)")
	flags.BoolVar(&c.Tls, "tls", false, "Establish a secure TLS connection")
	flags.StringVar(&c.CaCert, "cacert", "", "CA Certificate file to verify with")
	flags.StringVar(&c.Cert, "cert", "", "Private key file to authenticate with")
	flags.StringVar(&c.Key, "key", "", "SSL private key file path")
	flags.BoolVar(&c.Help, "h", false, "show help information")
	flags.Usage = func() {
		fmt.Fprintf(&outBuf, "Usage: %s [OPTION]...\n", progName)
		flags.PrintDefaults()
	}

	err := flags.Parse(args)

	if c.Help {
		flags.Usage()
	}

	if c.Password == "" {
		c.Password = os.Getenv("REDISDUMPGO_AUTH")
	}

	return c, outBuf.String(), err
}
