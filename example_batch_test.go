package groupcache_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/mailgun/groupcache/v2"
)

func ExampleBatchUsage() {
	// Keep track of peers in our cluster and add our instance to the pool `http://localhost:8080`
	pool := groupcache.NewHTTPPoolOpts("http://localhost:8080", &groupcache.HTTPPoolOptions{})

	// Add more peers to the cluster
	pool.Set("http://localhost:8081", "http://localhost:8082")

	server := http.Server{
		Addr:    "localhost:8080",
		Handler: pool,
	}

	// Start a HTTP server to listen for peer requests from the groupcache
	go func() {
		log.Printf("Serving....\n")
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
	//defer server.Shutdown(context.Background())

	localHits := 0
	batchGetter := func(_ context.Context, keys []string, dests []groupcache.Sink) error {
		localHits++
		for i := 0; i < len(keys); i++ {
			err := dests[i].SetString("got:" + keys[i], time.Time{})
			if err != nil {
				return err
			}
		}
		return nil
	}

	// Create a new group cache with a max cache size of 3MB
	name := "http-test"
	batchGroup := groupcache.NewBatchGroup(name, 3000000, nil, groupcache.BatchGetterFunc(batchGetter))
	//ctx := context.Background()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	run := func(name string) {
		fmt.Printf("%s: before main cache stats: %+v\n", name, batchGroup.CacheStats(groupcache.MainCache))
		fmt.Printf("%s: before hot cache stats: %+v\n", name, batchGroup.CacheStats(groupcache.HotCache))

		for i := 0; i < 10; i++ {
			peerKey := fmt.Sprintf("peer_key-%d", i)
			keys := make([]string, 0)
			for j := 0; j < 10; j++ {
				keys = append(keys, fmt.Sprintf("key-%d-%d", i, j))
			}

			gots := make([]string, len(keys))
			dests := groupcache.BatchStringSink(gots)

			err := batchGroup.BatchGet(ctx, peerKey, keys, dests)
			if err != nil {
				log.Printf("%s: error on peer_key: %s, keys %s: %v", name, peerKey, strings.Join(keys, ","), err)
				continue
			}
			for idx, dest := range dests {
				want := "got:" + keys[idx]
				if gots[idx] != want {
					log.Printf("%s: for peer_key: %q, key: %q, got %q; want %q, real dest: %v", name, peerKey, keys[idx], gots[idx], want, dest)
				}
			}
		}

		fmt.Printf("%s: after main cache stats: %+v\n", name, batchGroup.CacheStats(groupcache.MainCache))
		fmt.Printf("%s: after hot cache stats: %+v\n", name, batchGroup.CacheStats(groupcache.HotCache))
	}

	run("first")
	run("second")
	//time.Sleep(time.Second * 3600)

	// Output: -- User --
	// Id: 12345
	// Name: John Doe
	// Age: 40
	// IsSuper: true
}
