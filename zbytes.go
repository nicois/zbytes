// Store byte arrays using compression where possible,
// using zstd dictionaries.
//
// In some situations a go application needs to store/cache
// a large number of byte arrays where each byte array is
// relatively small. These are usually not considered worth
// compressing as the overheads are too great. Using zstandard's
// dictionary capability, it is possible to train a compressor
// to be much more efficient with related patterns of data.

package zbytes

import (
	"bytes"
	"log"
	"sync"

	"github.com/valyala/gozstd"
)

// A Wrapper manages the wrapping and unwrapping
// of []byte objects, transparently handling the
// creation and application of a compression dictionary
// which is shared between subsequent Wrap operations.
// This is safe for use by multiple goroutines.
type Wrapper interface {
	Unwrap(*Wrapped) []byte
	Wrap([]byte) *Wrapped
}

type Wrapped struct {
	isCompressed bool
	raw          []byte
}

// Returns whether the wrapped data is compressed.
func (w *Wrapped) IsCompressed() bool {
	return w.isCompressed
}

type stats struct {
	messages         int64
	uncompressedSize int64
	compressedSize   int64
	mutex            *sync.Mutex
}

type wrapper struct {
	cdict             *gozstd.CDict
	ddict             *gozstd.DDict
	dictionaryBuilder chan<- []byte
	Stats             stats
	// mutex             *sync.RWMutex
}

func (w *wrapper) buildDictionary(db chan []byte, dictionarySize int, minimumSize int) {
	currentSize := 0
	inputs := make([][]byte, 0)
	for message := range db {
		inputs = append(inputs, message)
		currentSize += len(message)
		if currentSize >= minimumSize {
			break
		}
	}
	dictionary := gozstd.BuildDict(inputs, dictionarySize)
	// ensure decoder is in place before the encoder
	if ddict, err := gozstd.NewDDict(dictionary); err == nil {
		w.ddict = ddict
	} else {
		log.Fatal(err)
	}
	if cdict, err := gozstd.NewCDict(dictionary); err == nil {
		w.cdict = cdict
	} else {
		log.Fatal(err)
	}
}

// Creates a Wrapper, capable of wrapping byte arrays.
// A zstandard compression dictionary
// will be constructed from the first sourceDataSize bytes of input;
// prior to this, data will be stored internally, uncompressed.
// The dictionary's size when built will be dictionarySize bytes.
func CreateWrapper(dictionarySize int, sourceDataSize int) Wrapper {
	dictionaryBuilder := make(chan []byte, 0)
	wrapper := &wrapper{dictionaryBuilder: dictionaryBuilder, Stats: stats{mutex: new(sync.Mutex)}}
	go wrapper.buildDictionary(dictionaryBuilder, dictionarySize, sourceDataSize)
	return wrapper
}

// Create a wrapped byte array, which may or may not
// be compressed.
func (w *wrapper) Wrap(b []byte) *Wrapped {
	if w.cdict == nil {
		// try to send a copy to the builder, if we can
		select {
		case w.dictionaryBuilder <- b:
			break
		default:
			break
		}
		return &Wrapped{isCompressed: false, raw: b}
	} else {
		// don't try to reuse the writers for now
		var bb bytes.Buffer

		encoder := gozstd.NewWriterDict(&bb, w.cdict)
		defer encoder.Release()
		if _, err := encoder.Write(b); err != nil {
			log.Fatal(err)
		}

		// Flush the compressed data to bb.
		if err := encoder.Flush(); err != nil {
			log.Fatalf("cannot flush compressed data: %s", err)
		}
		return &Wrapped{isCompressed: true, raw: bb.Bytes()}
	}
}

// Returns a copy of the input byte array, which may or may not
// have be decompressed
func (w *wrapper) Unwrap(wr *Wrapped) []byte {
	if !wr.isCompressed {
		return wr.raw
	}

	if w.ddict == nil {
		panic("Trying to decompress without having a bulk processor defined!")
	}
	decoder := gozstd.NewReaderDict(bytes.NewBuffer(wr.raw), w.ddict)
	defer decoder.Release()
	var buf bytes.Buffer
	if _, err := decoder.WriteTo(&buf); err != nil {
		log.Fatal(err)
	}
	return buf.Bytes()
}
