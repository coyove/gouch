package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/coyove/gouch/clock"
)

var nn *Node
var addr = flag.String("l", ":8080", "node listen address")
var datadir = flag.String("d", "localdata", "data directory")
var nodename = flag.String("n", "node1", "node name")
var nodesconfig = flag.String("c", "nodes.config", "node name")

func main() {
	flag.Parse()

	buf, err := ioutil.ReadFile(*nodesconfig)
	nn, err = NewNode(*nodename, "bolt", *datadir, string(buf))
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			writeJSON(w, r, "msg", "invalid URL path: "+r.RequestURI, "error", true)
			return
		}
		m := nn.Info()
		m["node_listen"] = *addr
		writeJSON(w, r, "data", m, "ok", true)
	})
	http.HandleFunc("/put", httpPut)
	http.HandleFunc("/delete", httpDelete)
	http.HandleFunc("/get", httpGet)
	http.HandleFunc("/replicate", httpReplicate)

	log.Println("Node is listening on:", *addr)
	http.ListenAndServe(*addr, nil)
}

func httpPut(w http.ResponseWriter, r *http.Request) {
	key, value := r.FormValue("key"), r.FormValue("value")
	if key == "" {
		writeJSON(w, r, "error", true, "msg", "empty key")
		return
	}

	start := time.Now()
	if err := nn.Put(key, []byte(value)); err != nil {
		writeJSON(w, r, "error", true, "msg", err.Error())
		return
	}
	writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds())
}

func httpDelete(w http.ResponseWriter, r *http.Request) {
	key := r.FormValue("key")
	if key == "" {
		writeJSON(w, r, "error", true, "msg", "empty key")
		return
	}

	start := time.Now()
	if err := nn.Delete(key); err != nil {
		writeJSON(w, r, "error", true, "msg", err.Error())
		return
	}
	writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds())
}

func httpGet(w http.ResponseWriter, r *http.Request) {
	key := r.FormValue("key")
	if key == "" {
		writeJSON(w, r, "error", true, "msg", "empty key")
		return
	}

	start := time.Now()
	if r.FormValue("all_versions") != "" {
		ts, _ := strconv.ParseInt(r.FormValue("ts"), 10, 64)
		res, err := nn.GetAllVersions(key, ts)
		if err != nil {
			writeJSON(w, r, "error", true, "msg", err.Error())
			return
		}

		x, now := []map[string]interface{}{}, clock.Timestamp()
		for _, p := range res.Data {
			key, ts, node := p.SplitKeyInfo()
			x = append(x, map[string]interface{}{
				"key":    key,
				"ts":     ts,
				"node":   node,
				"future": ts > now,
			})
		}
		writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds(), "data", x)
	} else {
		v, err := nn.Get(key)
		if err != nil {
			writeJSON(w, r, "error", true, "not_found", err == ErrNotFound, "msg", err.Error())
			return
		}
		writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds(), "data", string(v))
	}
}

func httpReplicate(w http.ResponseWriter, r *http.Request) {
	ts, _ := strconv.ParseInt(r.FormValue("ts"), 10, 64)
	n, _ := strconv.Atoi(r.FormValue("n"))
	if n == 0 {
		n = 100
	}

	res, err := nn.GetChangedKeysSince(ts, n)
	if err != nil {
		w.Header().Add("X-Error", "true")
		w.Header().Add("X-Msg", err.Error())
		writeProtobuf(w, r, nil)
		return
	}

	writeProtobuf(w, r, res)
}
