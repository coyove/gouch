package filelog

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/coyove/common/clock"
)

func TestOpen(t *testing.T) {
	h, _ := Open("testlog")
	start, _ := h.GetTimestampForKey([]byte("zzz"))

	now := time.Now()
	rand.Seed(now.Unix())

	for i := 0; time.Since(now) < time.Second; i++ {
		ts, _ := h.GetTimestampForKey([]byte("zzz" + strconv.Itoa(i)))
		// time.Sleep(time.Millisecond * 10)

		if rand.Intn(10) == 0 {
			start = ts
			t.Log("new start", i)
		}
	}

	end, _ := h.GetTimestampForKey([]byte("abc"))
	h.GetTimestampForKey([]byte("zzz3"))

	c, _ := h.GetCursor(start)
	t.Log(start, end)
	t.Log(c.Data())
}

func BenchmarkCursor(b *testing.B) {
	h, _ := Open("testlog")
	for i := 0; i < b.N; i++ {
		h.GetCursor(clock.Timestamp() - 3600<<24)
	}
}
