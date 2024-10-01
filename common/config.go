package common

import (
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

/*
	config.go implements the methods to parse a config file and creates the instance structs
*/

type ClientFlags struct {
	DebugLevel int `yaml:"debugLevel"`
	KeyLength int `yaml:"keyLength"`
	ValueLength int `yaml:"valueLength"`
	ArrivalRate float64 `yaml:"arrivalRate"`
	TestDuration int `yaml:"testDuration"`
	WriteRequestRatio float64 `yaml:"writeRequestRatio"`
}

type ReplicaFlags struct {
	DebugLevel int `yaml:"debugLevel"`
	RoundTripTime int `yaml:"roundTripTime"` // in milliseconds
	BenchmarkMode int `yaml:"benchmarkMode"` // 0: resident store, 1: redis
}

type NetworkFlags struct {
	DebugLevel	int `yaml:"debugLevel"`
	ArtificialLatency int `yaml:"artificialLatency"` // in milliseconds
}

type Flags struct {
	ClientFlags ClientFlags `yaml:"client"`
	ReplicaFlags ReplicaFlags `yaml:"replica"`
	NetworkFlags NetworkFlags `yaml:"network"`
}

type Config struct {
	id int
	Replicas []Instance `yaml:"replicas"`
	Clients  []Instance `yaml:"clients"`
	Flags Flags `yaml:"flags"`
}


// Instance describes a single  instance connection information
type Instance struct {
	Id    string `yaml:"id"`
	Domain string `yaml:"domain"` // address should be in the form x.x.x.x
	Port string `yaml:"port"`
}

func ParseConfig(fname string, id int) (*Config, error) {
	var cfg Config
	data, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	err = yaml.UnmarshalStrict(data, &cfg)
	if err != nil {
		return nil, err
	}

	cfg.setId(id)

	cfg.configureSelfIP()
	return &cfg, nil
}

func (cfg *Config) getInstanceIndex(id int) (int, string) {
	for i := range cfg.Replicas {
		if cfg.Replicas[i].Id == strconv.Itoa(id) {
			return i, "replica"
		}
	}
	for i := range cfg.Clients {
		if cfg.Clients[i].Id == strconv.Itoa(id) {
			return i, "client"
		}
	}
	panic("should not happen")
}

/*
	Replace the IP of my self to 0.0.0.0
*/

func (cfg *Config) configureSelfIP() {
	index, instanceType := cfg.getInstanceIndex(cfg.id)

	if instanceType == "replica" {
		cfg.Replicas[index].Domain = "0.0.0.0"
	} else {
		cfg.Clients[index].Domain = "0.0.0.0"
	}
}

func (cfg *Config) setId (id int) {
	cfg.id = id
}

/*
	Returns the self ip:port
*/

func (cfg *Config) GetAddress(id int) string {

	index, instanceType := cfg.getInstanceIndex(id)

	if instanceType == "replica" {
		return cfg.Replicas[index].Domain + ":" + cfg.Replicas[index].Port
	} else {
		return cfg.Clients[index].Domain + ":" + cfg.Clients[index].Port
	}
}