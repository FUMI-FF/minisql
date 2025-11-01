package main

import (
	"bufio"
	"errors"
	"fmt"
	"minisql/backend"
	"minisql/core"
	"os"
	"strings"
)

var (
	ErrUnrecognizedMetaCmd = errors.New("unrecognized meta command")
)

func doMetaCommand(input string, db *backend.DB) error {
	s := strings.TrimSpace(input)
	if s == ".exit" {
		db.Close()
		os.Exit(0)
	}
	return ErrUnrecognizedMetaCmd
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
	if len(os.Args) < 2 {
		fmt.Println("Must apply a database filename")
		os.Exit(1)
	}

	db, err := backend.Open(os.Args[1])
	if err != nil {
		fmt.Println("Failed to open database")
		os.Exit(1)
	}
	defer db.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		printPrompt()
		input := readInput(reader)
		if strings.HasPrefix(input, ".") {
			if err := doMetaCommand(input, db); err != nil {
				fmt.Printf("Failed to execute meta command: %s\n", err)
			}
			continue
		}

		stmt, err := core.PrepareStatement(input)
		if err != nil {
			fmt.Printf("Failed to prepare statement: %s\n", err)
			continue
		}

		err = core.ExecuteStatement(stmt, db)
		if err != nil {
			fmt.Printf("`executeStatement` failed: %s\n", err)
		}
		fmt.Println("Executed")
	}
}
