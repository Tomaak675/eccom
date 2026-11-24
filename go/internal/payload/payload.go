// Package payload is eccom/generators/payload.py: the message bytes, built
// before the protocol adapter is involved so what the adapter transmits is
// exactly what was generated. The random stream is reproducible per seed
// but not byte-identical to Python's (a different PRNG); payload entropy
// is not a measured variable, nothing on the path compresses.
package payload

import (
	"encoding/binary"
	"math/rand/v2"
	"sync"

	"eccom/internal/config"
)

// Generator produces payloads of the configured size and content; safe for
// concurrent use.
type Generator struct {
	size  int
	zeros []byte // content zeros: one shared buffer, zero per-message cost
	mu    sync.Mutex
	rng   *rand.ChaCha8
}

// New builds the generator for cfg.
func New(cfg config.Payload) *Generator {
	g := &Generator{size: cfg.SizeBytes}
	if cfg.Content == "zeros" {
		g.zeros = make([]byte, cfg.SizeBytes)
		return g
	}
	var seed [32]byte
	binary.LittleEndian.PutUint64(seed[:], uint64(cfg.Seed))
	g.rng = rand.NewChaCha8(seed)
	return g
}

// Generate returns one payload of size bytes (the configured size for 0).
// The zeros payload is a shared buffer that callers must not modify.
func (g *Generator) Generate(size int) []byte {
	if size == 0 {
		size = g.size
	}
	if g.zeros != nil {
		if size == len(g.zeros) {
			return g.zeros
		}
		return make([]byte, size)
	}
	buf := make([]byte, size)
	g.mu.Lock()
	g.rng.Read(buf)
	g.mu.Unlock()
	return buf
}
