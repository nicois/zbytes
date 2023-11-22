# zbytes

A go library to transparently harness the power of zstandard custom-dictionary compression.

```go
    /* create a wrapper. It will collect the first 100000 bytes of wrapped
       content, creating an 8k custom compression dictionary from it,
       and use the default (0) compression level.
       Before the 100k is collected, bytes will be stored uncompressed.
       Afterwards, the custom dictionary will be used to achieve very good
       compression on even very short individual messages
     */
    wrapper := CreateWrapper(8192, 100_000, 0)

    // this opaque object holds the possibly-compressed data
    wrappedBytes := wrapper.Wrap([]byte("A sample message"))

    // a copy of the original byte array can easily be retrieved
    originalBytes := wrapper.Unwrap(wrappedBytes)
```

## why?

If you hold a large number of uncompressed objects in memory, this is for you.

## considerations

- the initial wrapped data (100k in the above example) will not be compressed. Even after the compression dictionary is formed,
  the data will not be retrospectively modified; only new wrapped data will see the memory usage improvements.
- during the initial phase, additional memory is used to retain copies of the wrapped bytes. Once sufficient data has been
  collected to build the dictionary, this memory is freed.
- care need to be taken if the wrapped data is to be persisted to disk. It will require the original compression dictionary to be
  available. The `Wrapper` object includes a `Backup` method, allowing the same compression dictionary to be used across restarts.
