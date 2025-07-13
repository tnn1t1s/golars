package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/davidpalaitis/golars/benchmarks/data"
	"github.com/davidpalaitis/golars/frame"
	"github.com/davidpalaitis/golars/io/csv"
	"github.com/davidpalaitis/golars/io/parquet"
)

func main() {
	var (
		size       = flag.String("size", "medium", "Data size: small, medium, large, xlarge")
		format     = flag.String("format", "parquet", "Output format: csv or parquet")
		output     = flag.String("output", "", "Output file path (default: data/h2oai_<size>.<format>)")
		nullRatio  = flag.Float64("null", 0.05, "Null ratio (0.0 to 1.0)")
		seed       = flag.Int64("seed", 0, "Random seed")
		sort       = flag.Bool("sort", false, "Sort the data by group columns")
		showInfo   = flag.Bool("info", false, "Show data size information and exit")
	)
	flag.Parse()

	if *showInfo {
		showDataInfo()
		return
	}

	// Get config
	config, err := data.GetConfigBySize(*size)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	// Override config with command line flags
	config.NullRatio = *nullRatio
	config.Seed = *seed
	config.Sort = *sort

	// Determine output path
	outputPath := *output
	if outputPath == "" {
		outputPath = filepath.Join("data", fmt.Sprintf("h2oai_%s.%s", *size, *format))
	}

	// Ensure output directory exists
	dir := filepath.Dir(outputPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Generate data
	fmt.Printf("Generating H2O.ai benchmark data:\n")
	fmt.Printf("  Size: %s (%d rows, %d groups)\n", *size, config.NRows, config.NGroups)
	fmt.Printf("  Null ratio: %.2f%%\n", config.NullRatio*100)
	fmt.Printf("  Seed: %d\n", config.Seed)
	fmt.Printf("  Sort: %v\n", config.Sort)
	fmt.Printf("  Output: %s\n", outputPath)

	start := time.Now()
	df, err := data.GenerateH2OAIData(config)
	if err != nil {
		log.Fatalf("Failed to generate data: %v", err)
	}
	genTime := time.Since(start)
	fmt.Printf("  Generation time: %v\n", genTime)

	// Write data
	start = time.Now()
	switch *format {
	case "csv":
		err = writeCSV(df, outputPath)
	case "parquet":
		err = writeParquet(df, outputPath)
	default:
		log.Fatalf("Unknown format: %s", *format)
	}
	if err != nil {
		log.Fatalf("Failed to write data: %v", err)
	}
	writeTime := time.Since(start)
	fmt.Printf("  Write time: %v\n", writeTime)

	// Show file info
	info, err := os.Stat(outputPath)
	if err == nil {
		fmt.Printf("  File size: %.2f MB\n", float64(info.Size())/(1024*1024))
	}

	fmt.Printf("\nData generation complete!\n")
}

func writeCSV(df *frame.DataFrame, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file, csv.DefaultWriteOptions())
	return writer.Write(df)
}

func writeParquet(df *frame.DataFrame, path string) error {
	return parquet.WriteParquet(df, path)
}

func showDataInfo() {
	fmt.Println("H2O.ai Benchmark Data Sizes:")
	fmt.Println()
	fmt.Printf("%-10s %15s %15s %20s\n", "Size", "Rows", "Groups", "Approx. Size")
	fmt.Printf("%-10s %15s %15s %20s\n", "----", "----", "------", "------------")
	
	for _, info := range data.GetDataSizeInfo() {
		sizeMB := float64(info.SizeInBytes) / (1024 * 1024)
		fmt.Printf("%-10s %15s %15s %20s\n",
			info.Name,
			formatNumber(info.Rows),
			formatNumber(info.Groups),
			fmt.Sprintf("%.1f MB", sizeMB))
	}
	
	fmt.Println()
	fmt.Println("Column Schema:")
	fmt.Println("  id1, id2, id3: String grouping columns")
	fmt.Println("  id4, id5, id6: Int32 grouping columns")
	fmt.Println("  v1, v2: Int32 value columns")
	fmt.Println("  v3: Float64 value column")
}

func formatNumber(n int) string {
	if n >= 1_000_000_000 {
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	} else if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	} else if n >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}