package main

import (
	"database/sql"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/pk-anderson/go-sqlscript/config"
	"github.com/pk-anderson/go-sqlscript/database"
	"github.com/pk-anderson/go-sqlscript/excelreader"
)

func validation(excelPath string, csvPath string, tableName string) {
	if excelPath == "" {
		log.Fatal("Error: No Excel file path provided.")
	}

	if csvPath == "" {
		log.Fatal("Error: No CSV file path provided.")
	}

	if tableName == "" {
		log.Fatal("Error: No table name provided.")
	}
}

func loadDB() *sql.DB {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Error loading configuration: %v", err)
	}

	db, err := database.ConnectDB(cfg)
	if err != nil {
		log.Fatalf("Error connecting to the database: %v", err)
	}

	log.Println("Successfully connected to the database!")

	return db
}

func handleData(excelPath string, csvPath string) (map[string][]string, []string) {
	excelData, columnOrder, err := excelreader.ReadExcel(excelPath)
	if err != nil {
		log.Fatalf("Error reading the Excel file: %v", err)
	}

	log.Println("Excel Data:")
	for _, header := range columnOrder {
		log.Printf("%s: %v", header, excelData[header])
	}

	err = excelreader.WriteCSV(excelData, columnOrder, csvPath)
	if err != nil {
		log.Fatalf("Error writing the CSV file: %v", err)
	}

	log.Println("CSV file successfully written:", csvPath)

	return excelData, columnOrder
}

func insertCSVData(db *sql.DB, csvPath string, tableName string, columnOrder []string) {
	file, err := os.Open(csvPath)
	if err != nil {
		log.Fatalf("Error opening CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	if err != nil {
		log.Fatalf("Error reading CSV header: %v", err)
	}

	placeholders := []string{}
	for i := 1; i <= len(columnOrder); i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i))
	}
	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tableName, strings.Join(columnOrder, ", "), strings.Join(placeholders, ", "))

	stmt, err := db.Prepare(insertStmt)
	if err != nil {
		log.Fatalf("Error preparing SQL insert statement: %v", err)
	}
	defer stmt.Close()

	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Fatalf("Error reading CSV line: %v", err)
		}

		args := make([]interface{}, len(record))
		for i, v := range record {
			args[i] = v
		}

		_, err = stmt.Exec(args...)
		if err != nil {
			log.Printf("Error inserting row %v: %v", record, err)
		}
	}

	log.Println("CSV data successfully inserted into the table!")
}

func main() {
	excelPath := flag.String("excelpath", "", "Path to the Excel file")
	csvPath := flag.String("csvpath", "", "Path to save the CSV file")
	tableName := flag.String("table", "", "Name of the table to insert data into")
	flag.Parse()

	validation(*excelPath, *csvPath, *tableName)

	db := loadDB()
	defer db.Close()

	_, columnOrder := handleData(*excelPath, *csvPath)

	insertCSVData(db, *csvPath, *tableName, columnOrder)
}
