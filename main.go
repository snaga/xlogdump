package main

import (
	"fmt"
	"github.com/bhand-mm/xlogdump/walparse"
)

func main() {
	entries := walparse.ParseWalFile("xlogtranslate/000000010000000000000001", 17812976);
	for i := range(entries) {
		fmt.Println(entries[i])
	}
}