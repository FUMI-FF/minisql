package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

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
		if input == ".exit" {
			os.Exit(0)
		} else {
			fmt.Printf("unrecognized command: %s\n", input)
		}
	}
}
