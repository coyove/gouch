package main

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coyove/gouch/clock"
	"github.com/coyove/gouch/driver"
	"github.com/gogo/protobuf/proto"
)

var httpClient = &http.Client{Timeout: time.Second}

type repState struct {
	NodeName         string    `json:"node_name"`
	Checkpoint       int64     `json:"checkpoint"`
	Progress         float64   `json:"progress"`
	LastJobAt        time.Time `json:"last_job_at"`
	LastJobTimestamp int64     `json:"last_job_at_ts"`
	// Alive            bool      `json:"alive"`
	LastError string `json:"last_error"`
}

func (n *Node) readRepState(friends string) {
	n.friends.contacts = map[string]string{}
	n.friends.states = map[string]*repState{}

	flag := false
	for _, f := range strings.Split(friends, ";") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		fu, err := url.Parse(f)
		if err != nil || fu.User == nil {
			log.Println("WARN: omit invalid friend:", strings.TrimSpace(f), err)
			continue
		}
		name := fu.User.String()
		if name == n.Name {
			flag = true
			continue
		}
		n.friends.contacts[name] = fu.Scheme + "://" + fu.Host
	}

	if !flag {
		log.Println("WARN: yourself (node:", n.Name, ") is not found in the friend list")
	}

	fn := filepath.Join(n.path, "replication")
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		// Not exist, create
	} else {
		buf, err := ioutil.ReadFile(fn)
		if err != nil {
			log.Println("WARN: read rep state error:", err)
			return
		}
		if err := json.Unmarshal(buf, &n.friends.states); err != nil {
			log.Println("WARN: read rep unmarshal error:", err)
			return
		}
	}

	for k := range n.friends.contacts {
		if n.friends.states[k] == nil {
			n.friends.states[k] = &repState{
				NodeName: k,
			}
		}
	}
}

func (n *Node) writeRepState(name string) {
	n.friends.Lock()
	defer n.friends.Unlock()
	if n.friends.contacts[name] == "" {
		return
	}
	buf, _ := json.Marshal(n.friends.states)
	fn := filepath.Join(n.path, "replication")
	if err := ioutil.WriteFile(fn, buf, 0777); err != nil {
		log.Println("WARN: write rep state error:", err)
	}
}

func (n *Node) replicationWorker(f *repState) {
	for {
		resp, err := httpClient.Get(n.friends.contacts[f.NodeName] + "/replicate?ver=" + strconv.FormatInt(f.Checkpoint, 10))
		if err != nil {
			f.LastError = err.Error() + "/" + time.Now().String()
		} else {
			buf, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()

			p := &Pairs{}
			if err := proto.Unmarshal(buf, p); err != nil {
				f.LastError = err.Error() + "/" + resp.Header.Get("X-Msg")
			} else {
				if err := n.PutKeyParis(p.Data); err != nil {
					f.LastError = err.Error()
				} else {
					if p.Next > f.Checkpoint {
						f.Checkpoint = p.Next
						f.Progress = float64(f.Checkpoint-n.log.Genesis()) / float64(clock.Timestamp()-n.log.Genesis())
					} else {
						f.Progress = 1
					}
					f.LastError = ""
					f.LastJobAt = time.Now()
					f.LastJobTimestamp = clock.Timestamp()
					n.writeRepState(f.NodeName)
				}
			}
		}
		time.Sleep(time.Second)
	}
}

func (n *Node) GetChangedKeysSince(startTimestamp int64, count int) (*Pairs, error) {
	c, err := n.log.GetCursor(startTimestamp)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	res := &Pairs{}

	for len(res.Data) < count {
		ts, key, err := c.Data()
		if err != nil {
			return nil, err
		}

		dbkey := n.combineKeyVer(string(key), ts)
		k, v, err := n.db.Get(dbkey)
		if err != nil {
			return nil, err
		}

		if bytes.Equal(k, dbkey) {
			res.Data = append(res.Data, driver.Entry{k, v, 0})
		}

		if !c.Next() {
			break
		}
	}

	if len(res.Data) > 0 {
		res.Next = res.Data[len(res.Data)-1].Version() + 1
	}
	return res, nil
}

func (n *Node) PutKeyParis(pairs []driver.Entry) error {
	sort.Slice(pairs, func(i, j int) bool {
		return bytes.Compare(pairs[i].Key, pairs[j].Key) == -1
	})
	return n.db.Put(pairs...)
}
