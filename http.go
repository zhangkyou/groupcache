/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package groupcache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/zhangkyou/groupcache/consistenthash"
	pb "github.com/zhangkyou/groupcache/groupcachepb"
)

const defaultBasePath = "/_groupcache/"

const defaultMultiPath = "/_groupcachemulti/"

const defaultReplicas = 50

// HTTPPool implements PeerPicker for a pool of HTTP peers.
type HTTPPool struct {
	// this peer's base URL, e.g. "https://example.net:8000"
	self string

	// opts specifies the options.
	opts HTTPPoolOptions

	mu          sync.Mutex // guards peers and httpGetters
	peers       *consistenthash.Map
	httpGetters map[string]*httpGetter // keyed by e.g. "http://10.0.0.2:8008"
}

// HTTPPoolOptions are the configurations of a HTTPPool.
type HTTPPoolOptions struct {
	// BasePath specifies the HTTP path that will serve groupcache requests.
	// If blank, it defaults to "/_groupcache/".
	BasePath string

	// MultiPath specifies the HTTP path that will serve multi keys' groupcache requests.
	// If blank, it defaults to "/_groupcachemulti/".
	MultiPath string

	// Replicas specifies the number of key replicas on the consistent hash.
	// If blank, it defaults to 50.
	Replicas int

	// HashFn specifies the hash function of the consistent hash.
	// If blank, it defaults to crc32.ChecksumIEEE.
	HashFn consistenthash.Hash

	// Transport optionally specifies an http.RoundTripper for the client
	// to use when it makes a request.
	// If nil, the client uses http.DefaultTransport.
	Transport func(context.Context) http.RoundTripper

	// Context optionally specifies a context for the server to use when it
	// receives a request.
	// If nil, uses the http.Request.Context()
	Context func(*http.Request) context.Context
}

// NewHTTPPool initializes an HTTP pool of peers, and registers itself as a PeerPicker.
// For convenience, it also registers itself as an http.Handler with http.DefaultServeMux.
// The self argument should be a valid base URL that points to the current server,
// for example "http://example.net:8000".
func NewHTTPPool(self string) *HTTPPool {
	p := NewHTTPPoolOpts(self, nil)
	http.Handle(p.opts.BasePath, p)
	return p
}

var httpPoolMade bool

// NewHTTPPoolOpts initializes an HTTP pool of peers with the given options.
// Unlike NewHTTPPool, this function does not register the created pool as an HTTP handler.
// The returned *HTTPPool implements http.Handler and must be registered using http.Handle.
func NewHTTPPoolOpts(self string, o *HTTPPoolOptions) *HTTPPool {
	if httpPoolMade {
		panic("groupcache: NewHTTPPool must be called only once")
	}
	httpPoolMade = true

	p := &HTTPPool{
		self:        self,
		httpGetters: make(map[string]*httpGetter),
	}
	if o != nil {
		p.opts = *o
	}
	if p.opts.BasePath == "" {
		p.opts.BasePath = defaultBasePath
	}
	if p.opts.MultiPath == "" {
		p.opts.MultiPath = defaultMultiPath
	}
	if p.opts.Replicas == 0 {
		p.opts.Replicas = defaultReplicas
	}
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)

	RegisterPeerPicker(func() PeerPicker { return p })
	return p
}

// Set updates the pool's list of peers.
// Each peer value should be a valid base URL,
// for example "http://example.net:8000".
func (p *HTTPPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(p.opts.Replicas, p.opts.HashFn)
	p.peers.Add(peers...)
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{
			getTransport: p.opts.Transport,
			baseURL:      peer + p.opts.BasePath,
			multiURL:     peer + p.opts.MultiPath,
		}
	}
}

// GetAll returns all the peers in the pool
func (p *HTTPPool) GetAll() []ProtoGetter {
	p.mu.Lock()
	defer p.mu.Unlock()

	var i int
	res := make([]ProtoGetter, len(p.httpGetters))
	for _, v := range p.httpGetters {
		res[i] = v
		i++
	}
	return res
}

func (p *HTTPPool) PickPeer(key string) (ProtoGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.peers.IsEmpty() {
		return nil, false
	}
	if peer := p.peers.Get(key); peer != p.self {
		return p.httpGetters[peer], true
	}
	return nil, false
}

func (p *HTTPPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Parse request.
	if !strings.HasPrefix(r.URL.Path, p.opts.MultiPath) && !strings.HasPrefix(r.URL.Path, p.opts.BasePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}

	//multi key request
	//todo not elegant,rebuild in future
	if strings.HasPrefix(r.URL.Path, p.opts.MultiPath) {
		parts := strings.SplitN(r.URL.Path[len(p.opts.MultiPath):], "/", 3)
		if len(parts) != 3 {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		groupName := parts[0]
		peerKey := parts[1]
		keyStr := parts[2]
		keys := strings.Split(keyStr, ",")

		// Fetch the value for this group/key.
		group := GetGroup(groupName)
		if group == nil {
			http.Error(w, "no such group: "+groupName, http.StatusNotFound)
			return
		}
		var ctx context.Context
		if p.opts.Context != nil {
			ctx = p.opts.Context(r)
		} else {
			ctx = r.Context()
		}

		group.Stats.ServerRequests.Add(1)
		// Delete the key and return 200
		if r.Method == http.MethodDelete {
			//todo batch delete
			return
		}

		bValues := make([][]byte, len(keys))
		dests := make([]Sink, len(keys))
		for i := 0; i < len(keys); i++ {
			dests[i] = AllocatingByteSliceSink(&bValues[i])
		}
		err := group.BatchGet(ctx, peerKey, keys, dests)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		valueList := make([]*pb.GetResponse, len(keys))
		for i := 0; i < len(keys); i++ {
			value := dests[i]
			view, err := value.view()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			var expireNano int64
			if !view.e.IsZero() {
				expireNano = view.Expire().UnixNano()
			}

			res := &pb.GetResponse{Value: bValues[i], Expire: &expireNano}
			valueList[i] = res
		}
		// Write the value to the response body as a proto message.
		body, err := proto.Marshal(&pb.GetMultiResponse{ValueList: valueList})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Write(body)
		return
	}

	parts := strings.SplitN(r.URL.Path[len(p.opts.BasePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	// Fetch the value for this group/key.
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	var ctx context.Context
	if p.opts.Context != nil {
		ctx = p.opts.Context(r)
	} else {
		ctx = r.Context()
	}

	group.Stats.ServerRequests.Add(1)

	// Delete the key and return 200
	if r.Method == http.MethodDelete {
		group.localRemove(key)
		return
	}

	var b []byte

	value := AllocatingByteSliceSink(&b)
	err := group.Get(ctx, key, value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	view, err := value.view()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var expireNano int64
	if !view.e.IsZero() {
		expireNano = view.Expire().UnixNano()
	}

	// Write the value to the response body as a proto message.
	body, err := proto.Marshal(&pb.GetResponse{Value: b, Expire: &expireNano})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Write(body)
}

type httpGetter struct {
	getTransport func(context.Context) http.RoundTripper
	baseURL      string
	multiURL     string
}

// GetURL
func (p *httpGetter) GetURL() string {
	return p.baseURL
}

var bufferPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

//single get url http://example.net:8000/_groupcache/group/key
//multi get url http://example.net:8000/_gruopcachemulti/group/peer_key/keys
func (h *httpGetter) makeRequest(ctx context.Context, method string, in *pb.GetRequest, out *http.Response) error {
	var u string
	if !in.GetMulti() {
		u = fmt.Sprintf(
			"%v%v/%v",
			h.baseURL,
			url.QueryEscape(in.GetGroup()),
			url.QueryEscape(in.GetKey()),
		)
	} else {
		u = fmt.Sprintf(
			"%v%v/%v/%v",
			h.multiURL,
			url.QueryEscape(in.GetGroup()),
			url.QueryEscape(in.GetPeerKey()),
			url.QueryEscape(in.GetKey()),
		)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return err
	}

	tr := http.DefaultTransport
	if h.getTransport != nil {
		tr = h.getTransport(ctx)
	}

	res, err := tr.RoundTrip(req)
	if err != nil {
		return err
	}
	*out = *res
	return nil
}

func (h *httpGetter) Get(ctx context.Context, in *pb.GetRequest, out *pb.GetResponse) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodGet, in, &res); err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err := io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

func (h *httpGetter) BatchGet(ctx context.Context, in *pb.GetRequest, out *pb.GetMultiResponse) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodGet, in, &res); err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	b := bufferPool.Get().(*bytes.Buffer)
	b.Reset()
	defer bufferPool.Put(b)
	_, err := io.Copy(b, res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	err = proto.Unmarshal(b.Bytes(), out)
	if err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

func (h *httpGetter) Remove(ctx context.Context, in *pb.GetRequest) error {
	var res http.Response
	if err := h.makeRequest(ctx, http.MethodDelete, in, &res); err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return fmt.Errorf("while reading body response: %v", res.Status)
		}
		return fmt.Errorf("server returned status %d: %s", res.StatusCode, body)
	}
	return nil
}
