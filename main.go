package main

import (
	"flag"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/coyove/gouch/clock"
)

var nn *Node
var addr = flag.String("l", ":8080", "listen address")
var datadir = flag.String("d", "localdata", "data directory")

func main() {
	flag.Parse()

	var err error
	nn, err = NewNode("test", "bolt", *datadir)
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
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
		writeJSON(w, r, "error", true, "msg", err.Error())
		return
	}

	writeProtobuf(w, r, res)
}
