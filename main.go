package main

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/coyove/gouch/clock"
)

var nn *Node

func main() {
	var err error

	nn, err = NewNode("bolt", "testlog")
	if err != nil {
		panic(err)
	}

	http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
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
		writeJSON(w, r, "msg", "ok", "cost", time.Since(start).Seconds())
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
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
			writeJSON(w, r, "msg", "ok", "cost", time.Since(start).Seconds(), "data", x)
		} else {
			v, err := nn.Get(key)
			if err != nil {
				writeJSON(w, r, "error", true, "msg", err.Error())
				return
			}
			writeJSON(w, r, "msg", "ok", "cost", time.Since(start).Seconds(), "data", string(v))
		}
	})

	log.Println("ok")
	http.ListenAndServe(":8080", nil)
}
