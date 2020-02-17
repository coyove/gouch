package main

import (
	"fmt"
	"net"
	"time"

	"github.com/coyove/gouch/clock"
)

func (n *Node) Info() map[string]interface{} {
	start := time.Unix(clock.UnixSecFromTimestamp(n.startAt), 0)
	m := map[string]interface{}{
		"node_start_at":      start,
		"node_start_at_ts":   n.startAt,
		"node_lives":         time.Since(start).Seconds(),
		"node_internal_name": n.InternalName(),
		"node_name":          n.Name,
		"node_db_driver":     n.driver,
		"node_genesis":       n.log.Genesis(),
		"log_size":           n.log.Size(),
		"log_size_human":     fmt.Sprintf("%.3fG", float64(n.log.Size())/1024/1024/1024),
		"db_stat":            n.db.Info(),
	}

	ifaces, _ := net.Interfaces()
	localip := ""
	for _, i := range ifaces {
		addrs, _ := i.Addrs()

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			localip += ip.String() + ";"
		}
	}

	m["node_ip"] = localip
	m["friends_states"] = n.friends.states
	m["friends_contacts"] = n.friends.contacts

	return m
}
