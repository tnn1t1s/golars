package main

import (
	"fmt"
	"log"

	"github.com/davidpalaitis/golars"
)

func main() {
	// Create sample data with some ties
	df, err := golars.NewDataFrame(
		golars.NewStringSeries("product", []string{
			"Laptop", "Phone", "Tablet", "Monitor", "Keyboard",
			"Mouse", "Headphones", "Camera", "Printer", "Scanner",
		}),
		golars.NewStringSeries("category", []string{
			"Electronics", "Electronics", "Electronics", "Electronics", "Accessories",
			"Accessories", "Accessories", "Electronics", "Office", "Office",
		}),
		golars.NewInt32Series("price", []int32{
			1200, 800, 600, 400, 100,
			50, 150, 800, 300, 300,  // Note: Camera and Phone have same price, Printer and Scanner too
		}),
		golars.NewInt32Series("stock", []int32{
			5, 10, 15, 8, 25,
			30, 20, 3, 12, 7,
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Original DataFrame:")
	fmt.Println(df)
	fmt.Println()

	// Example 1: All ranking functions without partitioning
	fmt.Println("=== All Ranking Functions (ordered by price desc) ===")
	
	result := df
	
	// Row Number
	result, err = result.WithColumn("row_num",
		golars.RowNumber().Over(
			golars.Window().OrderBy("price", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Rank (with gaps for ties)
	result, err = result.WithColumn("rank",
		golars.Rank().Over(
			golars.Window().OrderBy("price", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Dense Rank (no gaps for ties)
	result, err = result.WithColumn("dense_rank",
		golars.DenseRank().Over(
			golars.Window().OrderBy("price", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Percent Rank
	result, err = result.WithColumn("percent_rank",
		golars.PercentRank().Over(
			golars.Window().OrderBy("price", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// NTile - divide into 3 buckets
	result, err = result.WithColumn("ntile_3",
		golars.NTile(3).Over(
			golars.Window().OrderBy("price", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(result.Select("product", "price", "row_num", "rank", "dense_rank", "percent_rank", "ntile_3"))
	fmt.Println()

	// Example 2: Ranking within categories
	fmt.Println("=== Ranking Within Categories ===")
	
	categoryResult := df
	
	// Row Number within category
	categoryResult, err = categoryResult.WithColumn("cat_row_num",
		golars.RowNumber().Over(
			golars.Window().
				PartitionBy("category").
				OrderBy("price", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Percent Rank within category
	categoryResult, err = categoryResult.WithColumn("cat_percent_rank",
		golars.PercentRank().Over(
			golars.Window().
				PartitionBy("category").
				OrderBy("price", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// NTile within category (2 buckets)
	categoryResult, err = categoryResult.WithColumn("cat_ntile_2",
		golars.NTile(2).Over(
			golars.Window().
				PartitionBy("category").
				OrderBy("price", false)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Sort by category and price for better visualization
	fmt.Println(categoryResult.Select("category", "product", "price", "cat_row_num", "cat_percent_rank", "cat_ntile_2"))
	fmt.Println()

	// Example 3: Stock ranking
	fmt.Println("=== Stock Level Ranking ===")
	
	stockResult := df
	
	// Rank by stock levels (ascending - low stock gets low rank)
	stockResult, err = stockResult.WithColumn("stock_rank",
		golars.Rank().Over(
			golars.Window().OrderBy("stock", true)))
	if err != nil {
		log.Fatal(err)
	}
	
	// Quartiles (4 buckets)
	stockResult, err = stockResult.WithColumn("stock_quartile",
		golars.NTile(4).Over(
			golars.Window().OrderBy("stock", true)))
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Println(stockResult.Select("product", "stock", "stock_rank", "stock_quartile"))
}