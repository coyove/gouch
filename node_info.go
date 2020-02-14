package main

import (
	"fmt"
	"net"
	"time"
)

func (n *Node) Info() map[string]interface{} {
	m := map[string]interface{}{
		"node_start_at":      n.startAt,
		"node_start_at_ts":   n.startAtTimestamp,
		"node_lives":         time.Since(n.startAt).Seconds(),
		"node_internal_name": n.InternalName(),
		"node_name":          n.Name,
		"node_db_driver":     n.driver,
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

	n.friends.Lock()
	m["friends"] = n.friends.states
	n.friends.Unlock()

	return m
}
