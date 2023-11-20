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
	"fmt"
	"log"
	"sync"
	"time"

	statsd "github.com/smira/go-statsd"
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
	PeriodicStatsCollector(prefix string, stats *statsd.Client, period time.Duration, tags ...statsd.Tag)
	GetStats() Stats
}

type Wrapped struct {
	isCompressed bool
	raw          []byte
}

// Returns whether the wrapped data is compressed.
func (w *Wrapped) IsCompressed() bool {
	return w.isCompressed
}

type Stats struct {
	UncompressedMessages int64
	CompressedMessages   int64
	UncompressedBytes    int64 // total size of messages prior to compression
	CompressedBytes      int64 // total size of messages after compression (or if not compressed, the original size)
	mutex                *sync.Mutex
}

type wrapper struct {
	cdict             *gozstd.CDict
	ddict             *gozstd.DDict
	dictionaryBuilder chan<- []byte
	compressionLevel  int
	Stats             Stats
}

func (w *wrapper) GetStats() Stats {
	return w.Stats
}

// At the specified period, generate a number of Gauge statistics
// relating to the volume of messages and level of compression.
func (w *wrapper) PeriodicStatsCollector(prefix string, stats *statsd.Client, period time.Duration, tags ...statsd.Tag) {
	if stats == nil {
		return
	}
	ticker := time.NewTicker(period)
	for {
		<-ticker.C
		w.Stats.CollectStats(prefix, stats, tags...)
	}
}

// Generate a number of Gauge statistics relating to the
// volume of messages and level of compression since the last call.
func (s *Stats) CollectStats(prefix string, stats *statsd.Client, tags ...statsd.Tag) {
	if stats == nil {
		return
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	stats.Gauge(fmt.Sprintf("%vuncompressed-messages", prefix), s.UncompressedMessages, tags...)
	stats.Gauge(fmt.Sprintf("%vcompressed-messages", prefix), s.CompressedMessages, tags...)
	stats.Gauge(fmt.Sprintf("%vuncompressed-bytes", prefix), s.UncompressedBytes, tags...)
	stats.Gauge(fmt.Sprintf("%vcompressed-bytes", prefix), s.CompressedBytes, tags...)
	if s.CompressedBytes > 0 {
		// ratio of 10 implies the uncompressed size is 10 times the compressed size
		// on average, during this sampling period
		stats.FGauge(fmt.Sprintf("%vcompression-ratio", prefix), float64(s.UncompressedBytes)/float64(s.CompressedBytes), tags...)
	}
	s.UncompressedMessages = 0
	s.CompressedMessages = 0
	s.UncompressedBytes = 0
	s.CompressedBytes = 0
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
func CreateWrapper(dictionarySize int, sourceDataSize int, compressionLevel int) *wrapper {
	dictionaryBuilder := make(chan []byte, 0)
	wrapper := &wrapper{dictionaryBuilder: dictionaryBuilder, compressionLevel: compressionLevel, Stats: Stats{mutex: new(sync.Mutex)}}
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
		w.Stats.mutex.Lock()
		defer w.Stats.mutex.Unlock()
		w.Stats.UncompressedMessages += 1
		w.Stats.UncompressedBytes += int64(len(b))
		w.Stats.CompressedBytes += int64(len(b))
		return &Wrapped{isCompressed: false, raw: b}
	} else {
		// don't try to reuse the writers for now
		var bb bytes.Buffer

		encoder := gozstd.NewWriterParams(&bb, &gozstd.WriterParams{CompressionLevel: w.compressionLevel, Dict: w.cdict})
		defer encoder.Release()
		if _, err := encoder.Write(b); err != nil {
			log.Fatal(err)
		}

		// Flush the compressed data to bb.
		if err := encoder.Flush(); err != nil {
			log.Fatalf("cannot flush compressed data: %s", err)
		}
		compressed := bb.Bytes()
		w.Stats.mutex.Lock()
		defer w.Stats.mutex.Unlock()
		w.Stats.CompressedMessages += 1
		w.Stats.UncompressedBytes += int64(len(b))
		w.Stats.CompressedBytes += int64(len(compressed))
		return &Wrapped{isCompressed: true, raw: compressed}
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
