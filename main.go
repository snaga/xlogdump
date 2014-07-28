package main

import (
	"fmt"
	"log"
	"github.com/bhand-mm/xlogdump/walparse"
	"github.com/go-fsnotify/fsnotify"
)

func main() {
	quitChan := make(chan bool)

	entries := walparse.ParseWalFile("xlogtranslate/000000010000000000000001", 17812976);
	for i := range(entries) {
		fmt.Println(entries[i])
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
	    log.Fatal(err)
	}
	defer watcher.Close()

	go func() {
	    for {
	        select {
	        case event := <-watcher.Events:
	            log.Println("event:", event)
	            if event.Op&fsnotify.Write == fsnotify.Write {
	                log.Println("modified file:", event.Name)
	            }
	        case err := <-watcher.Errors:
	            log.Println("error:", err)
	        }
	    }
	}()

	err = watcher.Add(".")
	if err != nil {
	    log.Fatal(err)
	}

	<-quitChan
}