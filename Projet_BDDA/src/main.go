package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"malzahar-project/Projet_BDDA/config"
	"malzahar-project/Projet_BDDA/sgbd"
)

func main() {
	cfgPath := flag.String("config", "config.txt", "path to config file")
	flag.Parse()

	abs, _ := filepath.Abs(*cfgPath)
	cfg, err := config.LoadDBConfig(abs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(2)
	}
	s, err := sgbd.NewSGBD(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize SGBD: %v\n", err)
		os.Exit(2)
	}
	if err := s.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "runtime error: %v\n", err)
		os.Exit(2)
	}
}
