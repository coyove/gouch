package main

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

func getKey(r *http.Request) string {
	p := r.URL.Path
	if strings.HasSuffix(p, "/") {
		p = p[:len(p)-1]
	}
	idx := strings.LastIndex(p, "/")
	if idx == 0 {
		return ""
	}
	return p[idx+1:]
}

func httpPut(w http.ResponseWriter, r *http.Request) {
	key, value := r.FormValue("key"), r.FormValue("value")
	if key == "" {
		writeJSON(w, r, "error", true, "msg", "empty key")
		return
	}

	start := time.Now()

	ts, err := nn.Put(key, []byte(value), r.FormValue("append") != "")
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
	key := getKey(r)
	if key == "" {
		writeJSON(w, r, "error", true, "msg", "empty key")
		return
	}

	ver, err := strconv.ParseInt(r.FormValue("ver"), 10, 64)
	n, err := strconv.ParseInt(r.FormValue("n"), 10, 64)
	start := time.Now()

	if r.FormValue("all_versions") != "" {
		if n == 0 {
			n = 100
		}
		res, next, err := nn.GetAllVersions(key, ver, int(n), r.FormValue("key_only") != "")
		if err != nil {
			writeJSON(w, r, "error", true, "msg", err.Error())
			return
		}
		writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds(), "data", res, "next", next)
	} else {
		var v Entry
		if ver > 0 {
			v, err = nn.GetVersion(key, ver)
		} else {
			v, err = nn.Get(key)
		}
		if err != nil {
			writeJSON(w, r, "error", true, "not_found", err == ErrNotFound, "msg", err.Error())
			return
		}
		if r.FormValue("binary") != "" {
			w.Header().Add("X-Binary", "true")
			w.Header().Add("X-Version", strconv.FormatInt(ver, 10))
			w.Header().Add("Content-Type", "application/octet-stream")
			w.Write(v.ValueBytes())
		} else {
			writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds(), "data", v)
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

	if nodename := r.FormValue("me"); nodename != "" {
		f := nn.friends.states[nodename]
		if f != nil {
			f.RevCheckpoint = f.RevCheckpointTmp
			f.RevCheckpointTmp = ver
		}
	}

	writeProtobuf(w, r, res)
}

func httpRange(w http.ResponseWriter, r *http.Request) {
	key := r.FormValue("key")
	n, _ := strconv.Atoi(r.FormValue("n"))
	if n <= 0 {
		writeJSON(w, r, "error", true, "msg", "missing 'n'")
		return
	}

	start := time.Now()
	res, next, err := nn.Range(key, r.FormValue("end_key"), n,
		r.FormValue("key_only") != "",
		r.FormValue("include_deleted") != "",
		r.FormValue("desc") != "")
	if err != nil {
		writeJSON(w, r, "error", true, "msg", err.Error())
		return
	}

	writeJSON(w, r, "ok", true, "cost", time.Since(start).Seconds(), "next", next, "data", res)
}
