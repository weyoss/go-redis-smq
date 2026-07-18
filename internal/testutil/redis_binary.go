/*
 * Copyright (c) 2026
 * Weyoss <weyoss@outlook.com>
 * https://github.com/weyoss
 *
 * This source code is licensed under the MIT license found in the LICENSE file
 * in the root directory of this source tree.
 *
 */

package testutil

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"
)

const valkeyVersion = "7.2.8"

var valkeyURLs = map[string]map[string]string{
	"linux": {
		"amd64": "https://github.com/weyoss/valkey/releases/download/v" + valkeyVersion + "-2/valkey-server-linux-x64-v" + valkeyVersion + "-2.tar.gz",
		"arm64": "https://github.com/weyoss/valkey/releases/download/v" + valkeyVersion + "-2/valkey-server-linux-arm64-v" + valkeyVersion + "-2.tar.gz",
	},
	"darwin": {
		"amd64": "https://github.com/weyoss/valkey/releases/download/v" + valkeyVersion + "-2/valkey-server-macos-x64-v" + valkeyVersion + "-2.tar.gz",
		"arm64": "https://github.com/weyoss/valkey/releases/download/v" + valkeyVersion + "-2/valkey-server-macos-arm64-v" + valkeyVersion + "-2.tar.gz",
	},
}

// findOrDownloadRedis returns the path to a Redis binary.
// Checks PATH first, then the cache directory, then downloads a pre-built binary.
func findOrDownloadRedis() (string, error) {
	//fmt.Println("testutil: checking PATH for redis-server...")
	if path, err := exec.LookPath("redis-server"); err == nil {
		//fmt.Printf("testutil: found redis-server in PATH: %s\n", path)
		return path, nil
	}

	cachePath := redisCachePath()
	//fmt.Printf("testutil: checking cache: %s\n", cachePath)
	if _, err := os.Stat(cachePath); err == nil {
		//fmt.Println("testutil: using cached binary")
		return cachePath, nil
	}

	url, ok := valkeyURLs[runtime.GOOS][runtime.GOARCH]
	if !ok {
		return "", fmt.Errorf("no pre-built binary for %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	//fmt.Printf("testutil: downloading from %s...\n", url)
	if err := downloadAndExtract(url, cachePath); err != nil {
		return "", fmt.Errorf("download redis: %w", err)
	}
	//fmt.Println("testutil: download complete")

	if err := os.Chmod(cachePath, 0o755); err != nil {
		return "", fmt.Errorf("chmod redis: %w", err)
	}

	return cachePath, nil
}

func redisCachePath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = os.TempDir()
	}
	return filepath.Join(home, ".redis-smq-cache", "valkey-server-"+valkeyVersion)
}

func downloadAndExtract(url, destPath string) error {
	lockPath := destPath + ".lock"
	if err := acquireLock(lockPath); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	defer releaseLock(lockPath)

	if _, err := os.Stat(destPath); err == nil {
		return nil
	}

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download: status %d", resp.StatusCode)
	}

	gzReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	defer gzReader.Close()

	tarReader := tar.NewReader(gzReader)

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar: %w", err)
		}

		if filepath.Base(header.Name) == "valkey-server" {
			if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
				return fmt.Errorf("mkdir: %w", err)
			}

			f, err := os.Create(destPath)
			if err != nil {
				return fmt.Errorf("create: %w", err)
			}
			defer f.Close()

			if _, err := io.Copy(f, tarReader); err != nil {
				return fmt.Errorf("copy: %w", err)
			}
			return nil
		}
	}

	return fmt.Errorf("valkey-server binary not found in archive")
}

func acquireLock(lockPath string) error {
	for i := 0; i < 120; i++ {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL, 0o644)
		if err == nil {
			f.Close()
			return nil
		}
		if !os.IsExist(err) {
			return err
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("timeout waiting for lock: %s", lockPath)
}

func releaseLock(lockPath string) {
	os.Remove(lockPath)
}
