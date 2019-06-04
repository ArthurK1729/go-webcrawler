package main

import (
	"bufio"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Globals
var wg sync.WaitGroup

var re = regexp.MustCompile(`https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)`)

var tr = &http.Transport{
	MaxIdleConns:    10,
	IdleConnTimeout: 10 * time.Second,
}

var client = &http.Client{Transport: tr, Timeout: 10 * time.Second}

type SafeMap struct {
	urlRepository map[string]bool
	mux           *sync.RWMutex
}

func (m *SafeMap) SafeUpdate(url string) {
	m.mux.Lock()
	m.urlRepository[url] = true
	m.mux.Unlock()

}

func (m *SafeMap) CheckExists(url string) bool {
	m.mux.RLock()
	_, ok := m.urlRepository[url]
	m.mux.RUnlock()
	return ok
}

type Fetcher interface {
	Fetch(url string) (elapsed time.Duration, urls []string, err error)
}

type URLFetcher struct{}

func (URLFetcher) Fetch(url string) (time.Duration, []string, error) {
	if strings.Contains(url, ".png") ||
		strings.Contains(url, ".jpg") ||
		strings.Contains(url, ".mp3") ||
		strings.Contains(url, ".jpeg") ||
		strings.Contains(url, ".gif") ||
		strings.Contains(url, "css") ||
		strings.Contains(url, ".mp4") ||
		strings.Contains(url, ".js") {
		return 0, []string{}, errors.New("Invalid URL type for " + url)
	}

	resp, err := client.Get(url)

	if err != nil {
		log.Println(err)
		return 0, []string{}, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Println(err)
		return 0, []string{}, err
	}

	start := time.Now()
	fetchedUrls := re.FindAllString(string(body), -1)
	elapsed := time.Since(start)

	return elapsed, fetchedUrls, nil
}

func Crawl(fetcher Fetcher, urlStream chan string, urlMap SafeMap, writerChannel chan GraphEntry, workerNum int) {
	log.Print("Worker ", workerNum, " initiated.")

	for {
		select {
		case currentLink := <-urlStream:
			log.Printf("Worker %v is working on %v\n", workerNum, currentLink)

			if !urlMap.CheckExists(currentLink) {
				urlMap.SafeUpdate(currentLink)

				elapsed, urls, err := fetcher.Fetch(currentLink)
				log.Printf("Regex took %s seconds", elapsed)

				if err != nil || urls == nil {
					log.Println("Skipping link ", currentLink)
				} else {
					writerChannel <- GraphEntry{currentLink, urls}
					for _, url := range urls {
						urlStream <- url
					}
				}
			}
			// case <-time.After(10 * time.Second):
			// 	defer wg.Done()
			// 	return

		}
	}
}

// func InitialiseLogger(quit chan bool) {
// 	f, err := os.OpenFile("crawler.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

// 	if err != nil {
// 		log.Fatalf("error opening file: %v", err)
// 	}

// 	log.SetOutput(f)

// 	<-quit
// 	f.Close()
// }

type GraphEntry struct {
	sourceNode       string
	destinationNodes []string
}

func GraphWriter(writerChannel chan GraphEntry) {
	file, err := os.OpenFile("links.csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	writer := bufio.NewWriter(file)
	defer file.Close()

	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	log.Println("Writer initialised")
	for {
		graphEntry := <-writerChannel
		writer.WriteString(strings.Join(append([]string{graphEntry.sourceNode}, graphEntry.destinationNodes...), ","))
	}
}

func main() {
	f, err := os.OpenFile("crawler.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)

	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}

	log.SetOutput(f)
	defer f.Close()

	log.Println("Initialising web crawler")
	concurrency := 500
	bufferSize := 1000000
	writerChannel := make(chan GraphEntry, bufferSize)

	wg.Add(concurrency)

	var urlStream chan string = make(chan string, bufferSize)

	urlMap := SafeMap{urlRepository: make(map[string]bool), mux: &sync.RWMutex{}}
	fetcher := URLFetcher{}

	go GraphWriter(writerChannel)

	for i := 0; i < concurrency; i++ {
		log.Printf("Launching worker %v\n", i)
		go Crawl(fetcher, urlStream, urlMap, writerChannel, i)
	}

	urlStream <- "https://moz.com/top500"
	wg.Wait()
	// loggerQuitChannel <- true
}
