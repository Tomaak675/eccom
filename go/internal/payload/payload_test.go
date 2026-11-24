package payload

import (
	"bytes"
	"sync"
	"testing"

	"eccom/internal/config"
)

func TestZerosSharesOneBufferForTheConfiguredSize(t *testing.T) {
	g := New(config.Payload{SizeBytes: 32, Content: "zeros"})
	first, second := g.Generate(0), g.Generate(32)
	if len(first) != 32 || &first[0] != &second[0] || !bytes.Equal(first, make([]byte, 32)) {
		t.Fatalf("expected one shared 32-byte zero buffer, got %d and %d bytes", len(first), len(second))
	}
	other := g.Generate(8)
	if len(other) != 8 || !bytes.Equal(other, make([]byte, 8)) {
		t.Fatalf("other size: %v", other)
	}
}

func TestRandomIsReproduciblePerSeedAndDiffersBetweenSeeds(t *testing.T) {
	a := New(config.Payload{SizeBytes: 16, Content: "random", Seed: 42})
	b := New(config.Payload{SizeBytes: 16, Content: "random", Seed: 42})
	c := New(config.Payload{SizeBytes: 16, Content: "random", Seed: 43})
	first := a.Generate(0)
	if len(first) != 16 || !bytes.Equal(first, b.Generate(0)) {
		t.Fatal("the same seed must yield the same stream")
	}
	if bytes.Equal(a.Generate(0), first) {
		t.Fatal("consecutive payloads must differ")
	}
	if bytes.Equal(c.Generate(0), first) {
		t.Fatal("different seeds must yield different streams")
	}
	if got := a.Generate(5); len(got) != 5 {
		t.Fatalf("per-message size: %d", len(got))
	}
}

func TestRandomIsSafeForConcurrentUse(t *testing.T) {
	g := New(config.Payload{SizeBytes: 64, Content: "random", Seed: 1})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if len(g.Generate(0)) != 64 {
					t.Error("wrong size")
				}
			}
		}()
	}
	wg.Wait()
}
