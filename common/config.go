package common

import (
	"os"
	"strconv"

	"gopkg.in/yaml.v2"
)

/*
	config.go implements the methods to parse a config file and creates the instance structs
*/

// Instance describes a single  instance connection information
type Instance struct {
	Id    string `yaml:"id"`
	Region string `yaml:"region"`
	Domain string `yaml:"domain"` // address should be in the form x.x.x.x
	Port string `yaml:"port"`
}

// InstanceConfig describes the set of peers and clients in the system
type InstanceConfig struct {
	Replicas   []Instance `yaml:"replicas"`
	Clients []Instance `yaml:"clients"`
}

// NewInstanceConfig loads a  instance configuration from given file
func NewInstanceConfig(fname string, name int64) (*InstanceConfig, error) {
	var cfg InstanceConfig
	data, err := os.ReadFile(fname)
	if err != nil {
		return nil, err
	}
	err = yaml.UnmarshalStrict(data, &cfg)
	if err != nil {
		return nil, err
	}
	cfg = configureSelfIP(cfg, name)
	return &cfg, nil
}

/*
	Replace the IP of my self to 0.0.0.0
*/

func configureSelfIP(cfg InstanceConfig, id int64) InstanceConfig {
	for i := 0; i < len(cfg.Replicas); i++ {
		if cfg.Replicas[i].Id == strconv.Itoa(int(id)) {
			cfg.Replicas[i].Domain = "0.0.0.0"
			return cfg
		}
	}
	for i := 0; i < len(cfg.Clients); i++ {
		if cfg.Clients[i].Id == strconv.Itoa(int(id)) {
			cfg.Clients[i].Domain = "0.0.0.0"
			return cfg
		}
	}
	panic("should not happen")
}
