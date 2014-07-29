package main

import (
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"github.com/bhand-mm/xlogdump/walparse"
	"github.com/go-fsnotify/fsnotify"
)

/*
read files sequentially
retain backlog
on commit clear backlog of all committed updates
periodically drop old records based on some limit
 */

func watchWalDirectory(path string, quitChan chan bool) (<-chan string) {
	var highestLogId, highestRecOff uint32 = 0, 0

	// this channel is returned by this method to send updates to clients
	commitsChan := make(chan string)

	// this channel passes entries to be processed by the first go routine
	entryChan := make(chan walparse.WalEntry)

	// process entries as they come in and generate messages on the commitsChan
	go func() {
		for entry := range entryChan {
			fmt.Sprintf("%v", entry)
			commitsChan <- fmt.Sprintf("%v %v", entry.XLogId, entry.XRecOff)
		}
	}()

	// read current files
	files, err := filepath.Glob(path+"*")
	if err != nil {
	    log.Fatal(err)
	}

	// ... in order
	sort.Sort(sort.StringSlice(files))

	// ... and publish to entryChan
	var publishEntries = func(filename string) {
		entries := walparse.ParseWalFile(filename, 0)
		for _, e := range entries {
			if (e.XLogId > highestLogId || (e.XLogId == highestLogId && e.XRecOff > highestRecOff)) {
				entryChan <- e
				highestLogId, highestRecOff = e.XLogId, e.XRecOff
			}
		}
	}

	for _, f := range files {
		publishEntries(f)
	}

	// now begin watching for changes to files and publish to entryChan
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
	    log.Fatal(err)
	}

	go func() {
		defer watcher.Close()

	    for {
	        select {
	        case event := <-watcher.Events:
	        	publishEntries(event.Name)
	        case err := <-watcher.Errors:
	            log.Println("error:", err)
	        case <-quitChan:
	        	return
	        }
	    }
	}()

	err = watcher.Add(path)
	if err != nil {
	    log.Fatal(err)
	}

	return commitsChan
}

func main() {
	quitChan := make(chan bool)

	commitsChan := watchWalDirectory("./xlog/", quitChan)

	for commit := range commitsChan {
		log.Println(commit)
	}

	<-quitChan
}










