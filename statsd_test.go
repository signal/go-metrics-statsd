package statsd

import (
	"bytes"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rcrowley/go-metrics"
)

func ExampleStatsD() {
	addr, _ := net.ResolveUDPAddr("net", ":2003")
	go StatsD(metrics.DefaultRegistry, 1*time.Second, "some.prefix", addr)
}

func ExampleStatsDWithConfig() {
	addr, _ := net.ResolveUDPAddr("net", ":2003")
	go StatsDWithConfig(StatsDConfig{
		Addr:          addr,
		Registry:      metrics.DefaultRegistry,
		FlushInterval: 1 * time.Second,
		DurationUnit:  time.Millisecond,
		Percentiles:   []float64{0.5, 0.75, 0.99, 0.999},
	})
}

type server struct {
	addr *net.UDPAddr
	conn net.Conn
	mu   sync.RWMutex
	b    bytes.Buffer
}

func (s *server) Close() error {
	if s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

func (s *server) Bytes() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.b.Bytes()
}

func (s *server) Lines() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return strings.Split(string(s.b.Bytes()), "\n")
}

// testServer spawns a new UDP test server on 127.0.0.1:12345 and
// collects all data into a buffer
func testServer() (s *server, err error) {
	s = &server{}
	s.addr = &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}
	s.conn, err = net.ListenUDP("udp", s.addr)
	if err != nil {
		return nil, err
	}

	go func() {
		defer s.conn.Close()
		buf := make([]byte, 1024)
		for {
			n, err := s.conn.Read(buf)
			if err != nil {
				fmt.Println("server: read: ", err)
				return
			}

			s.mu.Lock()
			_, err = s.b.Write(buf[:n])
			s.mu.Unlock()

			if err != nil {
				fmt.Println("server: write: ", err)
				return
			}
		}
	}()
	return s, nil
}

func TestWrites(t *testing.T) {
	// start a test server
	srv, err := testServer()
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	// generate some metrics
	r := metrics.NewRegistry()

	metrics.GetOrRegisterCounter("foo", r).Inc(2)

	// TODO: Use a mock meter rather than wasting 10s to get a QPS.
	for i := 0; i < 20; i++ {
		metrics.GetOrRegisterMeter("bar", r).Mark(1)
		time.Sleep(250 * time.Millisecond)
	}

	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 5)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 4)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 3)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 2)
	metrics.GetOrRegisterTimer("baz", r).Update(time.Second * 1)

	// report the metrics to the test server
	c := StatsDConfig{
		Addr:          srv.addr,
		Registry:      r,
		FlushInterval: 10 * time.Millisecond,
		DurationUnit:  time.Millisecond,
		Percentiles:   []float64{0.5, 0.75, 0.99, 0.999},
		Prefix:        "foobar",
	}
	statsd(&c)

	// wait for data to arrive
	time.Sleep(250 * time.Millisecond)

	gotLines := srv.Lines()
	wantLines := []string{
		"foobarbar.count:20|g",
		"foobarbar.fifteen-minute:4.00|g",
		"foobarbar.five-minute:4.00|g",
		"foobarbar.mean:4.00|g",
		"foobarbar.one-minute:4.00|g",
		"foobarbaz.50-percentile:3000.00|g",
		"foobarbaz.75-percentile:4500.00|g",
		"foobarbaz.99-percentile:5000.00|g",
		"foobarbaz.999-percentile:5000.00|g",
		"foobarbaz.count:5|g",
		"foobarbaz.fifteen-minute:0.00|g",
		"foobarbaz.five-minute:0.00|g",
		"foobarbaz.max:5000|g",
		"foobarbaz.mean-rate:138454.30|g",
		"foobarbaz.mean:3000.00|g",
		"foobarbaz.min:1000|g",
		"foobarbaz.one-minute:0.00|g",
		"foobarbaz.std-dev:1414.21|g",
		"foobarfoo.count:2|g",
		"",
	}

	// check number of lines
	if got, want := len(gotLines), len(wantLines); got != want {
		t.Fatalf("got %d lines want %d", got, want)
	}

	// check that last line is blank
	if got, want := gotLines[len(gotLines)-1], ""; got != want {
		t.Fatalf("got %q as last line want %q", got, want)
	}

	// compare results
	parse := func(s string) (key string, val float64, typ string, err error) {
		a, b := strings.IndexRune(s, ':'), strings.IndexRune(s, '|')
		key = s[:a]
		typ = s[b+1:]
		val, err = strconv.ParseFloat(s[a+1:b], 64)
		return
	}

	cmp := func(a, b string, delta float64) bool {
		akey, aval, atyp, aerr := parse(a)
		bkey, bval, btyp, berr := parse(b)
		return aerr == nil && berr == nil && akey == bkey && atyp == btyp && math.Abs(aval-bval) < delta
	}

	sort.Strings(gotLines)
	sort.Strings(wantLines)
	for i := range gotLines {
		got, want := gotLines[i], wantLines[i]
		if got == "" {
			continue
		}

		// TODO: mean-rate is a bit strange
		delta := 0.00001
		if strings.Contains(got, ".mean-rate:") {
			delta = math.MaxFloat64
		}

		if !cmp(got, want, delta) {
			t.Errorf("got line %q want %q", got, want)
		}
	}
}
