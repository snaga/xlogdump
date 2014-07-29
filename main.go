package main

import (
	"log"
	"path/filepath"
	"sort"
	"github.com/bhand-mm/xlogdump/walparse"
	"github.com/go-fsnotify/fsnotify"
)

/*
path.Base(string) string
*/

func watchDirectory(path string, quitChan chan bool) (<-chan walparse.WalEntry) {
	notifyChan := make(chan walparse.WalEntry)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
	    log.Fatal(err)
	}

	files, err := filepath.Glob(path+"*")
	if err != nil {
	    log.Fatal(err)
	}

	sort.Sort(sort.StringSlice(files))

	var entries = make([]walparse.WalEntry, 0)

	for i, f := range files {
		log.Printf("i: %v, file: %v", i, f)
		temp := walparse.ParseWalFile(f, 0)
		entries = append(entries, temp...)
		log.Println(len(entries))
	}

	for i := 0; i < 100; i++ {
		log.Println(entries[i])
	}

	go func() {
		defer watcher.Close()

	    for {
	        select {
	        // case event := <-watcher.Events:
	        	// log.Println(event)
	            // switch {
	            // case event.Op & fsnotify.Remove == fsnotify.Remove:
	            // 	notifyChan <- FileNotification{FileDeleted, event.Name}
	            // default:
	            // 	notifyChan <- FileNotification{FileUpdated, event.Name}
	            // }
	        // case err := <-watcher.Errors:
	            // log.Println("error:", err)
	        case <-quitChan:
	        	return
	        }
	    }
	}()

	err = watcher.Add(path)
	if err != nil {
	    log.Fatal(err)
	}

	return notifyChan
}

func main() {
	quitChan := make(chan bool)

	fileChan := watchDirectory("./xlog/", quitChan)

	<-fileChan

	// get channel for file name
	// on write send changed message
	// on delete send quit message
	// quit should trigger one final parse
	// each 'actor' writes updates to a channel 
	// an initial write message should be spoofed for all files currently in the directory

	<-quitChan
}










