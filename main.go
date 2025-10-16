package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type MetaCommandResult = int

const (
	MetaCommandSuccess MetaCommandResult = iota
	MetaCommandUnrecogunisedCommand
)

type PrepareResult = int

const (
	PrepareSuccess PrepareResult = iota
	PrepareUnrecognisedStatement
	PrepareSyntaxError
	PrepareFailure
)

type StatementType = int

const (
	StatementInsert StatementType = iota
	StatementSelect
)

type Row struct {
	id       int
	username string
	email    string
}

type Statement struct {
	_type       StatementType
	rowToInsert Row
}

func doMetaCommand(input string) MetaCommandResult {
	s := strings.TrimSpace(input)
	if s == ".exit" {
		os.Exit(0)
	}
	return MetaCommandUnrecogunisedCommand
}

func prepareStatement(input string, stmt *Statement) PrepareResult {
	if strings.HasPrefix(input, "insert") {
		stmt._type = StatementInsert
		n, err := fmt.Sscanf(input, "insert %d %s %s", &stmt.rowToInsert.id, &stmt.rowToInsert.username, &stmt.rowToInsert.email)
		if err != nil {
			fmt.Printf("`fmt.Sscanf` failed: %s\n", err)
			return PrepareFailure
		}
		if n < 3 {
			return PrepareSyntaxError
		}
		return PrepareSuccess
	}
	if strings.HasPrefix(input, "select") {
		stmt._type = StatementSelect
		return PrepareSuccess
	}
	return PrepareUnrecognisedStatement
}

func executeStatement(stmt *Statement) {
	switch stmt._type {
	case StatementInsert:
		fmt.Println("this is where we would do an insert")
	case StatementSelect:
		fmt.Println("this is where we would do an select")
	}
}

func printPrompt() {
	fmt.Print("db > ")
}

func readInput(reader *bufio.Reader) string {
	input, err := reader.ReadString('\n')
	if err != nil {
		os.Exit(1)
	}
	return strings.TrimSpace(input)
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		printPrompt()
		input := readInput(reader)
		if strings.HasPrefix(input, ".") {
			switch doMetaCommand(input) {
			case MetaCommandSuccess:
				continue
			case MetaCommandUnrecogunisedCommand:
				fmt.Println("unrecognised command")
				continue
			}
		}

		var stmt Statement
		switch prepareStatement(input, &stmt) {
		case PrepareSuccess:
		case PrepareUnrecognisedStatement:
			fmt.Println("unrecognised keyword")
			continue
		}

		executeStatement(&stmt)
		fmt.Println("Executed")
	}
}
