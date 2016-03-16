package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/go-fsnotify/fsnotify"
)

func watchWalDirectory(watchPath string) (<-chan Update) {
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

		for entry := range entryChan {
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
	        }
	    }
	}()

	err = watcher.Add(watchPath)
	if err != nil {
	    log.Fatal(err)
	}

	return updatesChan
}

func newClient(conn net.Conn, requestChan chan Request) {
	var start string
	var last string = ""

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		line := scanner.Text()

		// check for "start ####"
		_, err := fmt.Sscanf(line, "%s %s", &start, &last)
		if err == nil && strings.ToLower(start) == "start" {
			start = "start"
			last = strings.ToUpper(last)
			break
		}

		// check for just "start"
		_, err = fmt.Sscanf(line, "%s", &start)
		if err == nil && strings.ToLower(start) == "start" {
			start = "start"
			break
		}

		fmt.Fprintf(conn, "error: must send either \"start\" or \"start <logid>\"\n")
	}
	if err := scanner.Err(); err != nil {
	    log.Println("error reading from client:", err)
	}

	if start == "start" {
		for {
			responseChan := make(chan Update)
			requestChan <- Request{responseChan, last}
			for res := range responseChan {
				resBytes := updateToJson(res)
				conn.Write(resBytes)
				fmt.Fprintf(conn, "\n")
				if res.LogId > last {
					last = res.LogId
				}
			}
		}
	}
}

type Request struct {
	To		chan Update
	Last	string
}

func main() {
	path := flag.String("path", "", "full path to the pg_xlog directory (required)")
	statefile := flag.String("statefile", "", "path to file that keeps state (required)")
	address := flag.String("listen", "0.0.0.0:8989", "ip and port to listen on")
	buffer := flag.Int("buffer", 1000, "maximum amount of history to keep for clients to catch up after disconnect")
	flag.Parse()

	if *path == "" || *statefile == "" {
		flag.PrintDefaults()
	} else {
		quitChan := make(chan bool)
		state := ""
		stateBytes, err := ioutil.ReadFile(*statefile)
		if err == nil {
			state = string(stateBytes)
		}
		stateFD, err := os.OpenFile(*statefile, os.O_CREATE | os.O_WRONLY, 0644)
		if err != nil {
			log.Fatal("cannot create state file", err)
		}

		updates := make([]Update, 0)
		updatesChan := watchWalDirectory(*path)
		requestChan := make(chan Request)

		go func() {
			wakeQueue := make([]chan Update, 0)
			for {
				select {
				case update := <- updatesChan:
					if update.LogId > state {
						updates = append(updates, update)
						lenUpdates := len(updates)
						if lenUpdates > *buffer {
							updates = updates[lenUpdates - *buffer : lenUpdates]
						}

						for _, wake := range wakeQueue {
							close(wake)
						}
						wakeQueue = make([]chan Update, 0)
						state = update.LogId
						_, err := stateFD.Seek(0, 0)
						if err != nil {
							log.Println("error writing state file: ", err)
						}
						_, err = stateFD.Write([]byte(state))
						if err != nil {
							log.Println("error writing state file: ", err)
						}
						// ioutil.WriteFile(*statefile, []byte(state), 0644)
					}
				case req := <- requestChan:
					for _, update := range updates { // TODO: optimize
						if update.LogId > req.Last {
							req.To <- update
						}
					}

					wakeQueue = append(wakeQueue, req.To)
				}
			}
		}()

		ln, err := net.Listen("tcp", *address)
		if err != nil {
			log.Fatal(err)
		}
		for {
			conn, err := ln.Accept()
			if err != nil {
				log.Println(err)
			} else {
				go newClient(conn, requestChan)
			}
		}

		<-quitChan
	}
}

