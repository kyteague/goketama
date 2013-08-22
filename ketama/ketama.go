package ketama

import (
	"crypto/md5"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

var (
	ErrNoServers       = errors.New("No valid server definitions found")
	ErrMalformedServer = errors.New("One of the servers is in an invalid format")
)

type mcs struct {
	point uint
	addr  interface{}
}

type mcsArray []mcs

type ServerInfo struct {
	Addr   interface{}
	Memory uint64
}

type Continuum struct {
	numpoints int
	modtime   time.Time
	array     mcsArray
}

func (s mcsArray) Less(i, j int) bool { return s[i].point < s[j].point }
func (s mcsArray) Len() int           { return len(s) }
func (s mcsArray) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s mcsArray) Sort()              { sort.Sort(s) }

func md5Digest(in []byte) []byte {
	h := md5.New()
	h.Write(in)
	return h.Sum(nil)
}

func GetHash(in string) uint {
	digest := md5Digest([]byte(in))
	return ((uint(digest[3]) << 24) |
		(uint(digest[2]) << 16) |
		(uint(digest[1]) << 8) |
		uint(digest[0]))
}

func New(serverList []ServerInfo) *Continuum {
	numServers := len(serverList)
	if numServers == 0 {
		panic(ErrNoServers)
	}

	var totalMemory uint64
	for i := range serverList {
		totalMemory += serverList[i].Memory
	}

	continuum := &Continuum{
		array: make([]mcs, numServers*160),
	}

	cont := 0

	for _, server := range serverList {
		pct := float64(server.Memory) / float64(totalMemory)
		ks := int(math.Floor(pct * 40.0 * float64(numServers)))

		for k := 0; k < ks; k++ {
			ss := fmt.Sprintf("ketama: %s-%s", server.Addr, k)
			digest := md5Digest([]byte(ss))

			for h := 0; h < 4; h++ {
				continuum.array[cont].point = ((uint(digest[3+h*4]) << 24) |
					(uint(digest[2+h*4]) << 16) |
					(uint(digest[1+h*4]) << 8) |
					uint(digest[h*4]))
				continuum.array[cont].addr = server.Addr
				cont++
			}
		}
	}

	continuum.array.Sort()
	continuum.numpoints = cont

	return continuum
}

func (cont *Continuum) PickServer(key string) interface{} {

	if len(cont.array) == 0 {
		panic(ErrNoServers)
	}

	h := GetHash(key)
	i := sort.Search(len(cont.array), func(i int) bool { return cont.array[i].point >= h })
	if i >= len(cont.array) {
		i = 0
	}
	return cont.array[i].addr
}
