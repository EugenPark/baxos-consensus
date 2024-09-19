#!/bin/bash

cd parser
go build -o target/parser

cd ..
./parser/target/parser --template "template.yaml.tmpl" --values "values.yaml" --output "docker-compose.yaml"
