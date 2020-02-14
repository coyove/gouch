package filelog

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coyove/common/clock"
)

func TestOpen(t *testing.T) {
	// os.Remove("testlog")
	h, _ := Open("testlog")

	now := time.Now()
	rand.Seed(now.Unix())

	randKey := func() string {
		x := strconv.Itoa(rand.Int())
		x = x[:len(x)-rand.Intn(len(x)/4)]
		x = strings.Repeat(x, rand.Intn(3)+1)
		return x
	}

	x := [][2]interface{}{}

	for i := 0; time.Since(now) < 2*time.Second; i++ {
		k := randKey()

		n := rand.Intn(4) + 1
		for i := 0; i < n; i++ {
			clock.Timestamp()
		}

		ts, _ := h.GetTimestampForKey([]byte(k))
		x = append(x, [2]interface{}{k, ts})
	}

	start := rand.Intn(len(x))
	c, _ := h.GetCursor(x[start][1].(int64) - 1)

	for {
		ts, key, _ := c.Data()
		y := x[start]

		if y[0].(string) != string(key) {
			t.Fatal(y, string(key), len(key)%3)
		}

		if y[1].(int64) != ts {
			t.Fatal(y, ts)
		}

		if !c.Next() {
			break
		}
		start++
	}
}

func BenchmarkCursor(b *testing.B) {
	h, _ := Open("testlog")
	for i := 0; i < b.N; i++ {
		h.GetCursor(clock.Timestamp() - 3600<<24)
	}
}
