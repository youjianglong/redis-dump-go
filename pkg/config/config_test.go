package config

import (
	"reflect"
	"testing"
)

func TestFromFlags(t *testing.T) {
	testCases := []struct {
		args []string
		conf Config
	}{
		{
			[]string{},
			Config{
				Db:        -1,
				Host:      "127.0.0.1",
				Port:      6379,
				Filter:    "*",
				BatchSize: 1000,
				NWorkers:  10,
				WithTTL:   true,
				Format:    "resp",
			},
		},
		{
			[]string{"-db", "2"},
			Config{
				Db:        2,
				Host:      "127.0.0.1",
				Port:      6379,
				Filter:    "*",
				BatchSize: 1000,
				NWorkers:  10,
				WithTTL:   true,
				Format:    "resp",
			},
		},
		{
			[]string{"-ttl=false"},
			Config{
				Db:        -1,
				Host:      "127.0.0.1",
				Port:      6379,
				Filter:    "*",
				BatchSize: 1000,
				NWorkers:  10,
				WithTTL:   false,
				Format:    "resp",
			},
		},
		{
			[]string{"-host", "redis", "-port", "1234", "-batchSize", "10", "-n", "5", "-fmt", "commands"},
			Config{
				Db:        -1,
				Host:      "redis",
				Port:      1234,
				Filter:    "*",
				BatchSize: 10,
				NWorkers:  5,
				WithTTL:   true,
				Format:    "commands",
			},
		},
		{
			[]string{"-db", "1"},
			Config{
				Db:        1,
				Host:      "127.0.0.1",
				Port:      6379,
				Filter:    "*",
				BatchSize: 1000,
				NWorkers:  10,
				WithTTL:   true,
				Format:    "resp",
			},
		},
		{
			[]string{"-h"},
			Config{
				Db:        -1,
				Host:      "127.0.0.1",
				Port:      6379,
				Filter:    "*",
				BatchSize: 1000,
				NWorkers:  10,
				WithTTL:   true,
				Format:    "resp",
				Help:      true,
			},
		},
	}

	for i, testCase := range testCases {
		cfg, _, _ := FromFlags("redis-dump-go", testCase.args)
		if reflect.DeepEqual(cfg, testCase.conf) != true {
			t.Errorf("test %d: failed parsing config - expected , got: \n%+v\n%+v", i, testCase.conf, cfg)
		}
	}
}
