package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
)

func main() {
	// Create data with explicit ties to test ranking functions
	df, err := golars.NewDataFrame(
		golars.NewStringSeries("name", []string{
			"A", "B", "C", "D", "E", "F", "G", "H", "I", "J",
		}),
		golars.NewInt32Series("score", []int32{
			100, 90, 90, 80, 80, 80, 70, 60, 60, 50,
		}),
		golars.NewStringSeries("group", []string{
			"X", "X", "Y", "Y", "X", "Y", "X", "Y", "X", "Y",
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Test DataFrame with Ties:")
	fmt.Println(df)
	fmt.Println()

	// Apply all ranking functions
	result := df
	
	// Row Number (no ties)
	result, err = result.WithColumn("row_num",
		golars.RowNumber().Over(
			golars.Window().OrderBy("score", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Rank (with gaps)
	result, err = result.WithColumn("rank",
		golars.Rank().Over(
			golars.Window().OrderBy("score", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Dense Rank (no gaps)
	result, err = result.WithColumn("dense_rank",
		golars.DenseRank().Over(
			golars.Window().OrderBy("score", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Percent Rank
	result, err = result.WithColumn("percent_rank",
		golars.PercentRank().Over(
			golars.Window().OrderBy("score", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Ranking Results (with ties):")
	fmt.Println(result.Select("name", "score", "row_num", "rank", "dense_rank", "percent_rank"))
	fmt.Println()
	
	// Test with partitioning
	partResult := df
	
	partResult, err = partResult.WithColumn("group_rank",
		golars.Rank().Over(
			golars.Window().
				PartitionBy("group").
				OrderBy("score", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	partResult, err = partResult.WithColumn("group_dense_rank",
		golars.DenseRank().Over(
			golars.Window().
				PartitionBy("group").
				OrderBy("score", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println("Partitioned Ranking (by group):")
	fmt.Println(partResult.Select("name", "group", "score", "group_rank", "group_dense_rank"))
}