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
 fix malloc usage
 publish updates over sockets (maybe amqp)
 test using postgres user
 */

func watchWalDirectory(path string, quitChan chan bool) (<-chan string) {
	// this channel is returned by this method to send updates to clients
	commitsChan := make(chan string)

	// this channel passes entries to be processed by the first go routine
	entryChan := make(chan walparse.WalEntry)

	var publishEntries = func(filename string) {
		entries := walparse.ParseWalFile(filename, 0)
		for _, e := range entries {
			entryChan <- e
		}
	}

	// process entries as they come in and generate messages on the commitsChan
	go func() {
		var highestLogId, highestRecOff uint32 = 0, 0
		var entries = make([]walparse.WalEntry, 0)

		for entry := range entryChan {
			if (entry.XLogId > highestLogId || (entry.XLogId == highestLogId && entry.XRecOff > highestRecOff)) {
				highestLogId, highestRecOff = entry.XLogId, entry.XRecOff

				switch {
				case entry.RmId == walparse.RM_XACT_ID && entry.Info == walparse.XLOG_XACT_COMMIT:
					temp := entries
					entries = make([]walparse.WalEntry, 0)
					for _, old := range temp {
						if old.XId == entry.XId {
							commitsChan <- fmt.Sprintf("committed:%v-%v as part of XId:%v", old.XLogId, old.XRecOff, old.XId)
						} else {
							entries = append(entries, old)
						}
					}
				case entry.RmId == walparse.RM_HEAP_ID:
					var heapOp = entry.Info & 0x70
					if heapOp == 0x00 || heapOp == 0x10 || heapOp == 0x20 || heapOp == 0x40 {
						entries = append(entries, entry)
					}
				}
			}
		}
	}()

	go func() {
		// read current files
		files, err := filepath.Glob(path+"*")
		if err != nil {
		    log.Fatal(err)
		}

		// ... in order
		sort.Sort(sort.StringSlice(files))

		// ... and publish to entryChan
		for _, f := range files {
			publishEntries(f)
		}
	}()

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
	        	if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
		        	publishEntries(event.Name)
		        }
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
		fmt.Printf("%v\n", commit)
	}

	<-quitChan
}










