package stores

import (
	"fmt"
	"strings"

	"github.com/ozontech/seq-db/buildinfo"
	"github.com/ozontech/seq-db/logger"
)

type Stores struct {
	Shards [][]string
	Vers   []string
}

func NewStoresFromString(str string, replicas int) *Stores {
	if str == "" {
		// this will give empty slice for replica sets instead of
		// unwanted slice with one empty slice
		return &Stores{
			Shards: [][]string{},
			Vers:   []string{},
		}
	}
	hostList := strings.Split(str, ",")
	if len(hostList)%(replicas) != 0 {
		logger.Fatal("number of hosts must be multiple of replica count")
	}
	c := len(hostList) / replicas
	hosts := make([][]string, c)
	vers := make([]string, c)
	for x, host := range hostList {
		pos := strings.IndexByte(host, '|')
		ver := buildinfo.Version
		if pos > 0 {
			ver = host[pos+1:]
			host = host[:pos]
		}
		index := x / replicas
		if len(hosts) < index {
			hosts = append(hosts, make([]string, 0))
		}
		hosts[index] = append(hosts[index], host)
		vers[index] = ver
	}

	return &Stores{
		Shards: hosts,
		Vers:   vers,
	}
}

func (s *Stores) String() string {
	stores := ""

	for rsIndex, rs := range s.Shards {
		stores += fmt.Sprintf("replica set index=%d, ver=%s: %s\n", rsIndex, s.Vers[rsIndex], strings.Join(rs, ","))
	}

	return stores
}
