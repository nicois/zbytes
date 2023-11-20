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
	var wrapper Wrapper
	wrapper = CreateWrapper(8192, 100_000, 0)
	fixedMessage := []byte("this is a fixed message. nothing changes. Is the same every time, meaning the dictionary will only be optimised for this exact byte string")
	for {
		wrapper.Wrap(fixedMessage)
		if wrapper.GetStats().CompressedMessages > 0 {
			break
		}
	}
	t.Logf("Uncompressed length of the message: %v", len(fixedMessage))
	t.Logf("Compressed length of the message: %v", len(wrapper.Wrap(fixedMessage).raw))
	if 6*len(wrapper.Wrap(fixedMessage).raw) > len(fixedMessage) {
		t.Error("Expected at least a 6:1 compression ratio")
	}
	differentMessage := []byte("this is a different message. It is not the same as the one the dictionary was trained on.")
	t.Logf("Uncompressed length of the message: %v", len(differentMessage))
	t.Logf("Compressed length of the message: %v", len(wrapper.Wrap(differentMessage).raw))
	if len(wrapper.Wrap(differentMessage).raw) >= len(differentMessage) {
		t.Error("Expected at least some compression")
	}
}

func TestE2e(t *testing.T) {
	// The wrapper will build an 8k dictionary when it has 100k of input data.
	// Until then, data will be held uncompressed
	var wrapper Wrapper
	wrapper = CreateWrapper(32768, 1000_000, 5)
	messages := make(chan string, 1)

	go generateMessages(1000000000000, messages)
	for message := range messages {
		b := []byte(message)
		wrapped := wrapper.Wrap(b)
		unwrapped := wrapper.Unwrap(wrapped)
		if !bytes.Equal(b, unwrapped) {
			t.Error("unequal after conversion back")
		}
		if wrapper.GetStats().CompressedMessages > 0 {
			t.Logf("%+v", wrapper.GetStats())
			// ensure at least a 2:1 compression ratio is seen on the first compressed message
			if 2*len(wrapped.raw) > len(b) {
				t.Errorf("%v; compressed size: %v; uncompressed size: %v", message, len(wrapped.raw), len(b))
			}
			return
		}
	}
}
