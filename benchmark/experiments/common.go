// Common utility functions shared across all benchmark experiments.
package experiments

import (
	"math/rand/v2"

	"github.com/go-faker/faker/v4"
)

// Generate random payloads with a fixed size in bytes.
func GeneratePayloads(count, size int) [][]byte {
	payloads := make([][]byte, count)
	for i := range payloads {
		text := faker.Paragraph()
		b := make([]byte, size)
		copy(b, text)
		// Fill remaining bytes with random data if text is shorter than size
		if len(text) < size {
			for j := len(text); j < size; j++ {
				b[j] = byte(rand.IntN(256))
			}
		}
		// Truncate if text is larger than requested size (rare for large sizes but possible)
		if len(text) > size {
			b = b[:size]
		}
		payloads[i] = b
	}
	return payloads
}
