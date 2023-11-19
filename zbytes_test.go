package zbytes

import (
	"bytes"
	"fmt"
	"testing"
)

func generateMessages(n int64, out chan<- string) {
	for i := int64(0); i < n; i += 1 {
		// out <- fmt.Sprintf("This is message number %v. %v+%v=%v. And this is more text which is static over and over again.", i, i, i, 2*i)
		out <- fmt.Sprintf("This is message number %v. And this is more text which is static over and over again.", i)
	}
	close(out)
}

func TestSimple(t *testing.T) {
	// The wrapper will build an 8k dictionary when it has 100k of input data.
	// Until then, data will be held uncompressed
	wrapper := CreateWrapper(8192, 100_000)
	messages := make(chan string, 1)

	go generateMessages(1000000000000, messages)
	for message := range messages {
		b := []byte(message)
		wrapped := wrapper.Wrap(b)
		unwrapped := wrapper.Unwrap(wrapped)
		if !bytes.Equal(b, unwrapped) {
			t.Error("unequal after conversion back")
		}
		if wrapped.IsCompressed() {
			// ensure at least a 3:1 compression ratio is seen on the first compressed message
			if 3*len(wrapped.raw) > len(b) {
				t.Errorf("%v; compressed size: %v; uncompressed size: %v", message, len(wrapped.raw), len(b))
			}
			return
		}
	}
}
