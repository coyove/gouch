package main

import (
	"flag"
	"io/ioutil"
	"log"
	"net/http"
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
	http.HandleFunc("/get/", httpGet)
	http.HandleFunc("/range", httpRange)
	http.HandleFunc("/replicate", httpReplicate)

	log.Println("Node is listening on:", *addr)
	http.ListenAndServe(*addr, nil)
}
