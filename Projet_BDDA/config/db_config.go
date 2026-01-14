package config

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
)

// DBConfig holds basic configuration for the (mini) SGBD.
type DBConfig struct {
	DBPath         string `json:"dbpath"`
	PageSize       int    `json:"pagesize"`
	DMMaxFileCount int    `json:"dm_maxfilecount"`
	BMBufferCount  int    `json:"bm_buffercount"`
	BMPolicy       string `json:"bm_policy"`
}

// PageId identifies a page inside a Data file: FileIdx is the index x in Datax.bin
// and PageIdx is the page number within that file (0-based).
type PageId struct {
	FileIdx int
	PageIdx int
}

// NewDBConfig constructs an instance from an in-memory path with default params.
// To provide explicit page size and max file count use NewDBConfigWithParams.
func NewDBConfig(dbpath string) *DBConfig {
	return &DBConfig{DBPath: dbpath, PageSize: 4096, DMMaxFileCount: 8, BMBufferCount: 16, BMPolicy: "LRU"}
}

// NewDBConfigWithParams constructs a DBConfig with explicit parameters.
func NewDBConfigWithParams(dbpath string, pageSize int, dmMaxFileCount int) *DBConfig {
	return &DBConfig{DBPath: dbpath, PageSize: pageSize, DMMaxFileCount: dmMaxFileCount, BMBufferCount: 16, BMPolicy: "LRU"}
}

// LoadDBConfig loads configuration from a text file. The loader accepts either JSON
// (e.g. {"dbpath":"./DB"}) or a simple key=value format (e.g. dbpath = '../DB').
func LoadDBConfig(filePath string) (*DBConfig, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, errors.New("empty config file")
	}

	var c DBConfig
	// try JSON first
	if err := json.Unmarshal(data, &c); err == nil && c.DBPath != "" {
		return &c, nil
	}

	// fallback to simple key=value parser
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// support dbpath = '...'
		if parts := strings.SplitN(line, "=", 2); len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			val = strings.Trim(val, `"'`)
			if key == "dbpath" {
				c.DBPath = val
				// don't return yet; we may also parse other params
			}
			if key == "pagesize" {
				// try parse int
				if v, err := strconv.Atoi(val); err == nil {
					c.PageSize = v
				}
			}
			if key == "dm_maxfilecount" || key == "dm.maxfilecount" {
				if v, err := strconv.Atoi(val); err == nil {
					c.DMMaxFileCount = v
				}
			}
			if key == "bm_buffercount" {
				if v, err := strconv.Atoi(val); err == nil {
					c.BMBufferCount = v
				}
			}
			if key == "bm_policy" {
				c.BMPolicy = val
			}
		}
		// support dbpath: ...
		if parts := strings.SplitN(line, ":", 2); len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			val = strings.Trim(val, `"'`)
			if key == "dbpath" {
				c.DBPath = val
			}
			if key == "pagesize" {
				if v, err := strconv.Atoi(val); err == nil {
					c.PageSize = v
				}
			}
			if key == "dm_maxfilecount" || key == "dm.maxfilecount" {
				if v, err := strconv.Atoi(val); err == nil {
					c.DMMaxFileCount = v
				}
			}
			if key == "bm_buffercount" {
				if v, err := strconv.Atoi(val); err == nil {
					c.BMBufferCount = v
				}
			}
			if key == "bm_policy" {
				c.BMPolicy = val
			}
		}
	}
	if c.DBPath == "" {
		return nil, errors.New("dbpath not found in config")
	}
	// set defaults if not provided
	if c.PageSize == 0 {
		c.PageSize = 4096
	}
	if c.DMMaxFileCount == 0 {
		c.DMMaxFileCount = 8
	}
	// set defaults for buffer manager if not provided
	if c.BMBufferCount == 0 {
		c.BMBufferCount = 16
	}
	if c.BMPolicy == "" {
		c.BMPolicy = "LRU"
	}
	return &c, nil
}
