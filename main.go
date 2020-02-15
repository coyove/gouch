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

var (
	addr        = flag.String("l", ":8080", "node listen address")
	datadir     = flag.String("d", "localdata", "data directory")
	nodename    = flag.String("n", "node1", "node name")
	nodesconfig = flag.String("c", "nodes.config", "node name")
)

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
	ts, err := nn.Put(key, []byte(value))
	if err != nil {
		writeJSON(w, r, "error", true, "msg", err.Error())
		return
	}
	writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds(), "ver", ts)
}

func httpDelete(w http.ResponseWriter, r *http.Request) {
	key := r.FormValue("key")
	if key == "" {
		writeJSON(w, r, "error", true, "msg", "empty key")
		return
	}

	start := time.Now()
	ts, err := nn.Delete(key)
	if err != nil {
		writeJSON(w, r, "error", true, "msg", err.Error())
		return
	}
	writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds(), "ver", ts)
}

func httpGet(w http.ResponseWriter, r *http.Request) {
	key := r.FormValue("key")
	if key == "" {
		writeJSON(w, r, "error", true, "msg", "empty key")
		return
	}

	ver, err := strconv.ParseInt(r.FormValue("ver"), 10, 64)
	start := time.Now()

	if r.FormValue("all_versions") != "" {
		res, err := nn.GetAllVersions(key, ver, r.FormValue("key_only") != "")
		if err != nil {
			writeJSON(w, r, "error", true, "msg", err.Error())
			return
		}

		x, now := []map[string]interface{}{}, clock.Timestamp()
		for i := len(res.Data) - 1; i >= 0; i-- {
			p := res.Data[i]
			ts := p.Version()
			x = append(x, map[string]interface{}{
				"value":     jsonBytes(p.Value),
				"ver":       ts,
				"value_len": p.ValueLen,
				"unix_ts":   time.Unix(clock.UnixSecFromTimestamp(ts), 0).Format(time.RFC3339),
				"node":      p.Node(),
				"future":    ts > now,
			})
		}
		writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds(), "key", key, "data", x)
	} else {
		var v []byte
		if ver > 0 {
			v, err = nn.GetVersion(key, ver)
		} else {
			v, ver, err = nn.Get(key)
		}
		if err != nil {
			writeJSON(w, r, "error", true, "not_found", err == ErrNotFound, "msg", err.Error())
			return
		}
		if r.FormValue("binary") != "" {
			w.Header().Add("X-Binary", "true")
			w.Header().Add("X-Version", strconv.FormatInt(ver, 10))
			w.Header().Add("Content-Type", "application/octet-stream")
			w.Write(v)
		} else {
			writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds(), "data", string(v), "ver", ver)
		}
	}
}

func httpReplicate(w http.ResponseWriter, r *http.Request) {
	ver, _ := strconv.ParseInt(r.FormValue("ver"), 10, 64)
	n, _ := strconv.Atoi(r.FormValue("n"))
	if n == 0 {
		n = 100
	}

	res, err := nn.GetChangedKeysSince(ver, n)
	if err != nil {
		w.Header().Add("X-Error", "true")
		w.Header().Add("X-Msg", err.Error())
		writeProtobuf(w, r, nil)
		return
	}

	writeProtobuf(w, r, res)
}
