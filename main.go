package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"regexp"
	"sort"

	"github.com/go-fsnotify/fsnotify"
)

func watchWalDirectory(watchPath string, quitChan chan bool) (<-chan Update) {
	if (watchPath[len(watchPath) - 1] != '/') {
		watchPath = watchPath + "/"
	}

	// this channel is returned by this method to send updates to clients
	updatesChan := make(chan Update)

	// this channel passes entries to be processed by the first go routine
	entryChan := make(chan WalEntry)

	var publishEntries = func(filename string) {
		base := filepath.Base(filename)
		matched, _ := regexp.MatchString("[0-9A-F]{24}", base)
		if matched {
			entries := ParseWalFile(filename, 0)
			for _, e := range entries {
				entryChan <- e
			}
		}
	}

	// process entries as they come in and generate messages on the updatesChan
	go func() {
		/*********
		 IMPORTANT: PostgreSQL reuses log files! We must not set highest* unless XId is unseen
		 *********/
		var highestLogId, highestRecOff, highestXId uint32 = 0, 0, 0
		var entries = make([]WalEntry, 0)

		select {
		case entry := <- entryChan:
			if entry.XId >= highestXId && (entry.XLogId > highestLogId || (entry.XLogId == highestLogId && entry.XRecOff > highestRecOff)) {
				highestLogId, highestRecOff, highestXId = entry.XLogId, entry.XRecOff, entry.XId

				switch {
				case entry.RmId == RM_XACT_ID && entry.Info == XLOG_XACT_COMMIT:
					temp := entries
					entries = make([]WalEntry, 0)
					for _, old := range temp {
						if old.XId == entry.XId {
							// publish some
							updatesChan <- entryToUpdate(old)
						} else {
							// requeue others
							entries = append(entries, old)
						}
					}
				case entry.RmId == RM_HEAP_ID:
					// heap op codes are 0x00 through 0x70
					var heapOp = entry.Info & 0x70

					// ... but we only care about these four (insert, delete, update, hot update respectively)
					if heapOp == 0x00 || heapOp == 0x10 || heapOp == 0x20 || heapOp == 0x40 {
						entries = append(entries, entry)
					}
				}
			}
        case <-quitChan:
        	return
		}
	}()

	localFilesDone := make(chan bool)
	go func() {
		// read current files
		files, err := filepath.Glob(watchPath+"*")
		if err != nil {
		    log.Fatal(err)
		}

		// ... in order
		sort.Sort(sort.StringSlice(files))

		// ... and publish to entryChan
		for _, f := range files {
			publishEntries(f)
		}
		localFilesDone <- true
	}()

	// now begin watching for changes to files and publish to entryChan
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
	    log.Fatal(err)
	}

	go func() {
		defer watcher.Close()
		<-localFilesDone

	    for {
	        select {
	        case event := <-watcher.Events:
	        	if event.Op & fsnotify.Write == fsnotify.Write || event.Op & fsnotify.Create == fsnotify.Create {
		        	publishEntries(event.Name)
		        }
	        case err := <-watcher.Errors:
	            log.Println("error:", err)
	        case <-quitChan:
	        	return
	        }
	    }
	}()

	err = watcher.Add(watchPath)
	if err != nil {
	    log.Fatal(err)
	}

	return updatesChan
}

func main() {
	path := flag.String("path", "", "full path to the pg_xlog directory")
	flag.Parse()

	if *path == "" {
		flag.PrintDefaults()
	} else {
		quitChan := make(chan bool)

		updateChan := watchWalDirectory(*path, quitChan)

		for update := range updateChan {
			fmt.Printf("%v\n", update)
		}

		<-quitChan
	}
}

