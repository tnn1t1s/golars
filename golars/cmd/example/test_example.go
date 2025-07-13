package main

import (
    "fmt"
    "log"
    "github.com/davidpalaitis/golars"
)

func main() {
    // Create a DataFrame from slices
    df, err := golars.NewDataFrame(
        golars.NewSeries("name", []string{"Alice", "Bob", "Charlie", "David"}),
        golars.NewSeries("age", []int32{25, 30, 35, 28}),
        golars.NewSeries("salary", []float64{50000, 60000, 75000, 55000}),
    )
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Println(df)
}