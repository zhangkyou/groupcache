package main

import (
	"context"
	"encoding/json"
	"flag"
	"github.com/mailgun/groupcache/v2"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	Protocol = "http://"
	BaseUrl = "127.0.0.1"
)

func main() {
	var port string
	flag.StringVar(&port, "port", "8080", "http peer port.")
	flag.Parse()

	addr := BaseUrl + ":" + port
	host := Protocol + addr
	peers := []string{"http://127.0.0.1:8080", "http://127.0.0.1:8081", "http://127.0.0.1:8082"}
	pool := groupcache.NewHTTPPoolOpts(host, &groupcache.HTTPPoolOptions{})
	pool.Set(peers...)
	server := http.Server{
		Addr:    addr,
		Handler: pool,
	}
	//Start a HTTP server to listen for peer requests from the groupcache
	go func() {
		log.Printf("GroupCache Serving, addr: %s....\n", addr)
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
	defer server.Shutdown(context.Background())

	batchGetter := func(_ context.Context, keys []string, dests []groupcache.Sink) error {
		for i := 0; i < len(keys); i++ {
			err := dests[i].SetString("got:" + keys[i], time.Time{})
			if err != nil {
				return err
			}
		}
		return nil
	}
	// Create a new group cache with a max cache size of 3MB
	groupName := "http"
	batchGroup := groupcache.NewBatchGroup(groupName, 3000000, nil, groupcache.BatchGetterFunc(batchGetter))

	http.HandleFunc("/batchGet", func(writer http.ResponseWriter, request *http.Request) {
		query := request.URL.Query()
		keys := query.Get("keys")
		peerKey := query.Get("peer_key")
		keyList := strings.Split(keys, ",")

		gots := make([]string, len(keyList))
		dests := groupcache.BatchStringSink(gots)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := batchGroup.BatchGet(ctx, peerKey, keyList, dests)
		if err != nil {
			log.Printf("%s: error on peer_key: %s, keys %s: %v", "batchGet", peerKey, keys, err)
			writer.Write([]byte(err.Error()))
			writer.Write([]byte("\n"))
			return
		}

		result := make(map[string]string)
		for idx, key := range keyList {
			result[key] = gots[idx]
		}
		bodyStr, err := json.Marshal(result)
		if err != nil {
			writer.Write([]byte(err.Error()))
			writer.Write([]byte("\n"))
			return
		}
		writer.Header().Set("Content-Type","application/json")
		writer.Write(bodyStr)
		writer.Write([]byte("\n"))
		return
	})

	http.HandleFunc("/cacheInfo", func(writer http.ResponseWriter, request *http.Request) {
		result := make(map[string]groupcache.CacheStats)

		result["mainCache"] = batchGroup.CacheStats(groupcache.MainCache)
		result["hotCache"] = batchGroup.CacheStats(groupcache.HotCache)
		body, err := json.Marshal(result)
		if err != nil {
			writer.Write([]byte(err.Error()))
			writer.Write([]byte("\n"))
			return
		}
		writer.Header().Set("Content-Type","application/json")
		writer.Write(body)
		writer.Write([]byte("\n"))
	})

	http.HandleFunc("/groupStats", func(writer http.ResponseWriter, request *http.Request) {
		result := make(map[string]groupcache.Stats)

		result[batchGroup.Name()] = batchGroup.Stats
		body, err := json.Marshal(result)
		if err != nil {
			writer.Write([]byte(err.Error()))
			writer.Write([]byte("\n"))
			return
		}
		writer.Header().Set("Content-Type","application/json")
		writer.Write(body)
		writer.Write([]byte("\n"))
	})

	httpAddr := BaseUrl + ":1" + port
	httpServer := http.Server{Addr: httpAddr}
	log.Printf("Http Serving, addr: %s....\n", httpAddr)
	err := httpServer.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
