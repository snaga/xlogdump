package main

import (
	"fmt"
	"xlogtranslate"
)

func main() {
	entries := parseWalFile("000000010000000000000001", 17812976);
	for i := range(entries) {
		fmt.Println(entries[i])
	}
}