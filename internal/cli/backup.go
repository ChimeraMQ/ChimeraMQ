package cli

import (
	"archive/tar"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RunBackupCLI runs the backup command.
func RunBackupCLI(args []string) {
	fs := flag.NewFlagSet("backup", flag.ExitOnError)
	dataDir := fs.String("data-dir", "/var/lib/chimera", "Data directory to backup")
	output := fs.String("output", "", "Output file (default: chimera-backup-YYYYMMDD-HHMMSS.tar.gz)")
	verbose := fs.Bool("v", false, "Verbose output")

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if *output == "" {
		*output = fmt.Sprintf("chimera-backup-%s.tar.gz", time.Now().Format("20060102-150405"))
	}

	if *verbose {
		fmt.Printf("Backing up %s to %s...\n", *dataDir, *output)
	}

	if err := createBackup(*dataDir, *output); err != nil {
		fmt.Fprintf(os.Stderr, "Backup failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Backup created: %s\n", *output)
}

// RunRestoreCLI runs the restore command.
func RunRestoreCLI(args []string) {
	fs := flag.NewFlagSet("restore", flag.ExitOnError)
	input := fs.String("input", "", "Backup file to restore (required)")
	dataDir := fs.String("data-dir", "/var/lib/chimera", "Data directory to restore to")
	force := fs.Bool("force", false, "Force restore even if data directory is not empty")
	verbose := fs.Bool("v", false, "Verbose output")

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	if *input == "" {
		fmt.Fprintf(os.Stderr, "Error: -input is required\n")
		fs.Usage()
		os.Exit(1)
	}

	// Check if data directory exists and is not empty
	if info, err := os.Stat(*dataDir); err == nil && info.IsDir() {
		entries, err := os.ReadDir(*dataDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading data directory: %v\n", err)
			os.Exit(1)
		}
		if len(entries) > 0 && !*force {
			fmt.Fprintf(os.Stderr, "Error: Data directory %s is not empty. Use -force to overwrite.\n", *dataDir)
			os.Exit(1)
		}
	}

	if *verbose {
		fmt.Printf("Restoring %s to %s...\n", *input, *dataDir)
	}

	if err := restoreBackup(*input, *dataDir); err != nil {
		fmt.Fprintf(os.Stderr, "Restore failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Restore complete: %s\n", *dataDir)
}

func createBackup(dataDir, output string) error {
	file, err := os.Create(output)
	if err != nil {
		return fmt.Errorf("create backup file: %w", err)
	}
	defer file.Close()

	gzWriter := gzip.NewWriter(file)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	return filepath.Walk(dataDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get relative path
		relPath, err := filepath.Rel(dataDir, path)
		if err != nil {
			return err
		}

		// Create tar header
		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}
		header.Name = relPath

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		// Write file content if it's a regular file
		if info.Mode().IsRegular() {
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			if _, err := io.Copy(tarWriter, file); err != nil {
				return err
			}
		}

		return nil
	})
}

func restoreBackup(input, dataDir string) error {
	file, err := os.Open(input)
	if err != nil {
		return fmt.Errorf("open backup file: %w", err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("decompress backup: %w", err)
	}
	defer gzReader.Close()

	tarReader := tar.NewReader(gzReader)

	// Create data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0750); err != nil {
		return fmt.Errorf("create data directory: %w", err)
	}

	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar header: %w", err)
		}

		// Security: prevent path traversal attacks
		// Clean the path and ensure it doesn't escape the data directory
		cleanName := filepath.Clean(header.Name)
		if strings.Contains(cleanName, "..") {
			return fmt.Errorf("invalid path in archive: %s", header.Name)
		}

		targetPath := filepath.Join(dataDir, cleanName)

		// Verify the target path is within data directory
		absTarget, err := filepath.Abs(targetPath)
		if err != nil {
			return fmt.Errorf("resolve target path: %w", err)
		}
		absDataDir, err := filepath.Abs(dataDir)
		if err != nil {
			return fmt.Errorf("resolve data directory: %w", err)
		}
		if !strings.HasPrefix(absTarget, absDataDir+string(filepath.Separator)) && absTarget != absDataDir {
			return fmt.Errorf("path escapes data directory: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("create directory: %w", err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0750); err != nil {
				return fmt.Errorf("create parent directory: %w", err)
			}

			outFile, err := os.Create(targetPath)
			if err != nil {
				return fmt.Errorf("create file: %w", err)
			}

			if _, err := io.Copy(outFile, tarReader); err != nil {
				outFile.Close()
				return fmt.Errorf("write file: %w", err)
			}

			outFile.Close()

			// Preserve permissions
			if err := os.Chmod(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("set permissions: %w", err)
			}
		}
	}

	return nil
}
