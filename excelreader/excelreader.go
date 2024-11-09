package excelreader

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/xuri/excelize/v2"
)

func ReadExcel(filePath string) (map[string][]string, []string, error) {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("error opening Excel file: %v", err)
	}

	sheetNames := f.GetSheetList()
	if len(sheetNames) == 0 {
		return nil, nil, fmt.Errorf("no sheets found in the Excel file")
	}

	sheetName := sheetNames[0]
	log.Printf("Reading from sheet: %s", sheetName)

	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading rows from the sheet: %v", err)
	}

	if len(rows) < 2 {
		return nil, nil, fmt.Errorf("insufficient data in the Excel file")
	}

	headers := rows[0]
	data := make(map[string][]string)
	columnOrder := []string{}
	for _, header := range headers {
		data[header] = []string{}
		columnOrder = append(columnOrder, header)
	}

	for _, row := range rows[1:] {
		for i, header := range headers {
			if i < len(row) {
				data[header] = append(data[header], row[i])
			} else {
				data[header] = append(data[header], "")
			}
		}
	}

	return data, columnOrder, nil
}

func WriteCSV(data map[string][]string, headers []string, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("error creating the CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(headers); err != nil {
		return fmt.Errorf("error writing header to the CSV: %v", err)
	}

	numRows := len(data[headers[0]])
	for i := 0; i < numRows; i++ {
		row := []string{}
		for _, header := range headers {
			row = append(row, data[header][i])
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("error writing row to the CSV: %v", err)
		}
	}

	return nil
}
