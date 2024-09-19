package main

import (
	"fmt"
	"flag"
	"html/template"
	"io"
	"os"
	"gopkg.in/yaml.v2"
)
 
func getValues(valuesFilePath *string) (map[string]interface{}, error){
	file, err := os.Open(*valuesFilePath)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	read, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var values map[string]interface{}

	err = yaml.Unmarshal(read, &values)
	if err != nil {
		return nil, err
	}

	return values, nil

}

func run(templateFilePath *string, valuesFilePath *string, outputFilePath *string) error {
	tmpl, err := template.ParseFiles(*templateFilePath)
	if err != nil {
		return err
	}

	values, err := getValues(valuesFilePath)
	if err != nil {
		return err
	}

	output, err := os.Create(*outputFilePath)
	if err != nil {
		return err
	}
	defer output.Close()

	err = tmpl.Execute(output, values)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	templateFilePath := flag.String("template", "", "Path to the template file")
	valuesFilePath := flag.String("values", "", "Path to the values file")
	outputFilePath := flag.String("output", "", "Path to the output file location")
	flag.Parse()

	err := run(templateFilePath, valuesFilePath, outputFilePath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
