# nats-asio

## Overview
This is a high-performance [NATS](https://nats.io/) client written in C++20 using [ASIO](https://think-async.com/Asio/) coroutines. This fork includes extensive performance optimizations and feature additions for production use.

**Original project**: [antlad/nats_asio](https://github.com/antlad/nats_asio)
**This fork**: Adds JetStream support, HTTP streaming, batch publishing, performance optimizations, and a feature-rich CLI tool.

## Key Features

### Core Library
- **Header-only C++20 NATS client** using standalone ASIO coroutines
- **JetStream support** with publish, subscribe, ACK handling, and KV operations
- **High-performance optimizations**: simdjson parsing, SIMD string operations, zero-copy buffers
- **Compression support**: zstd compression for messages
- **HTTP/HTTPS streaming**: Read from HTTP endpoints with SSL/TLS
- **Reconnection logic**: Automatic reconnection with jitter and circuit breaker
- **Connection pooling**: Multiple connections with load balancing

### nats_tool CLI

A comprehensive command-line tool for NATS operations with extensive JetStream support.

#### Publishing Modes
- **pub**: Standard NATS publish
- **bench**: High-throughput benchmark mode with pipelined publishing
- **JetStream publish**: With ACK handling, sliding window batching, and retry logic
- **Fire-and-forget**: Maximum throughput JetStream publishing (~155k msgs/sec)
- **Batch publishing**: Multi-threaded batch publishing for extreme throughput

#### Subscription Modes
- **grub**: Standard NATS subscribe
- **js_grub**: JetStream pull consumer with auto-ACK
- **js_fetch**: JetStream fetch with batch control
- **kvwatch**: Watch JetStream KV bucket changes

#### Request-Reply
- **req**: Send request and wait for reply
- **reply**: Respond to requests

#### JetStream Operations
- **Stream management**: Auto-create/update streams
- **Consumer management**: Durable consumers, pull consumers
- **KV operations**: create, update, get, keys, history, purge, revert

#### Input Sources
- **stdin**: Default input
- **File**: Read from single file with optional follow mode (`--file`, `--follow`)
- **Multiple files**: Glob patterns with wildcard support (`--file "*.log"`, repeatable)
- **Watch mode**: Detect new files matching patterns in real-time (`--file "*.log" --follow`)
- **Compressed files**: Automatic decompression of gzip (.gz) and zstd (.zst) files
- **ZIP archives**: Automatic extraction of all files from .zip archives
- **HTTP/HTTPS**: Stream data from HTTP endpoints (`--http`, `--http_header`, `--http_body`)

#### Input Formats
- **line**: Line-delimited text (default)
- **json**: JSON objects with field templating (`--subject_template`, `--payload_fields`)
- **csv**: CSV with header support (`--csv_headers`)

#### Performance Features
- **JetStream sliding window**: Batch ACKs with configurable window size (`--js_window`)
- **Automatic retry**: Retry timed-out messages (`--js_max_retries`)
- **Fire-and-forget**: Skip ACK waiting for max throughput (`--no_ack`)
- **Per-stream metrics**: Track acked, failed, timeouts, retries per stream
- **Pipeline publishing**: Reduce latency with pipelined requests
- **Compression**: zstd compression (`--compress`)

#### Output Options
- **Raw payload**: Output message payload only (`--raw`)
- **JSON output**: Structured JSON output (`--json`)
- **Dump to file**: Save messages to file (`--dump`)
- **Transform**: Pipe payload through external command (`--translate`)

## Requirements

### Core Library
```
asio (standalone)
fmt
spdlog
openssl
nlohmann_json
simdjson
zlib (for gzip decompression)
zstd (for zstd decompression)
libzip (for ZIP archive extraction)
```

### nats_tool
```
cxxopts
mimalloc
```

### Tests
```
gtest
```

## Building

```bash
# Configure with vcpkg
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=[path-to-vcpkg]/scripts/buildsystems/vcpkg.cmake

# Build
cmake --build build

# Install (optional)
cmake --install build
```

The `nats_tool` binary will be in `build/bin/nats_tool`.

## Usage Examples

### Basic Publishing
```bash
# Publish to NATS
echo "hello world" | ./nats_tool pub --topic test.subject

# Publish with count and sleep
./nats_tool pub --topic test --count 100 --sleep 10
```

### JetStream Publishing
```bash
# JetStream with ACK and sliding window
cat data.txt | ./nats_tool pub --js --topic events --js_window 1000

# Fire-and-forget for max throughput (~155k msgs/sec)
cat data.txt | ./nats_tool pub --js --topic events --no_ack

# Auto-create stream and enable retry
./nats_tool pub --js --topic mydata.events --stream mystream --create_stream --js_max_retries 5
```

### HTTP Streaming Input
```bash
# Stream from HTTP endpoint
./nats_tool pub --http https://api.example.com/stream --topic events --js

# POST with custom headers
./nats_tool pub --http https://api.example.com/query \
  --http_method POST \
  --http_body '{"query":"SELECT * FROM data"}' \
  --http_header "Authorization:Bearer token" \
  --topic results --js
```

### Structured Input (JSON/CSV)
```bash
# JSON input with field templating
echo '{"symbol":"AAPL","price":150.5}' | ./nats_tool pub \
  --input_format json \
  --subject_template "quotes.{{symbol}}" \
  --payload_fields "price"

# CSV with headers
cat data.csv | ./nats_tool pub \
  --input_format csv \
  --csv_headers "symbol,price,volume" \
  --subject_template "quotes.{{symbol}}"
```

### File Input with Follow Mode
```bash
# Read from single file
./nats_tool pub --js --topic logs --file /var/log/app.log

# Follow mode (like tail -f)
./nats_tool pub --js --topic logs --file /var/log/app.log --follow

# Read from multiple files with glob pattern
./nats_tool pub --js --topic logs --file "/var/log/app*.log"

# Multiple patterns (multiple --file options)
./nats_tool pub --js --topic logs \
  --file "/var/log/app/*.log" \
  --file "/var/log/nginx/*.log"

# Watch for new files matching pattern (log shipper mode)
./nats_tool pub --js --topic logs --file "/var/log/*.log" --follow
```

### Compressed File Input
```bash
# Automatic decompression - detects .gz files
./nats_tool pub --js --topic logs --file /var/log/app.log.gz

# Automatic decompression - detects .zst files
./nats_tool pub --js --topic logs --file /var/log/app.log.zst

# Mixed compressed and uncompressed files
./nats_tool pub --js --topic logs --file "/var/log/*.log*"

# Glob pattern matching compressed archives
./nats_tool pub --js --topic archive --file "/var/log/archive/*.gz"

# Detection by magic bytes (works even without .gz/.zst extension)
./nats_tool pub --js --topic data --file /data/compressed_file

# Multiple patterns with compression
./nats_tool pub --js --topic logs \
  --file "/var/log/current/*.log" \
  --file "/var/log/archive/*.gz" \
  --file "/var/log/archive/*.zst"

# ZIP archives - extracts all files automatically
./nats_tool pub --js --topic data --file /path/to/archive.zip

# Multiple ZIP archives
./nats_tool pub --js --topic data --file "/archives/*.zip"

# Mixed: ZIP archives + compressed files
./nats_tool pub --js --topic logs \
  --file "/var/log/*.zip" \
  --file "/var/log/*.gz" \
  --file "/var/log/*.log"
```

### Subscribing
```bash
# Standard subscribe
./nats_tool grub --topic "events.>"

# JetStream pull consumer
./nats_tool js_grub --stream mystream --consumer myconsumer --auto_ack

# Output as JSON
./nats_tool grub --topic "events.*" --json

# Dump to file
./nats_tool grub --topic "logs.>" --dump /tmp/logs.txt
```

### Request-Reply
```bash
# Send request
./nats_tool req --topic service.request --data "payload"

# Respond to requests
./nats_tool reply --topic service.request --data "response"
```

### JetStream KV Operations
```bash
# Create KV bucket
./nats_tool kvcreate --bucket config

# Put key-value
./nats_tool kvupdate --bucket config --key app.setting --data "value"

# Get value
./nats_tool kvget --bucket config --key app.setting

# List keys
./nats_tool kvkeys --bucket config

# Watch for changes
./nats_tool kvwatch --bucket config
```

## Performance Benchmarks

On local NATS server:
- **JetStream with ACK + sliding window**: ~40k msgs/sec
- **JetStream fire-and-forget**: ~155k msgs/sec (4x improvement)
- **Standard pub**: ~200k+ msgs/sec

## Performance Optimizations

This fork includes numerous performance improvements:
- **simdjson**: 10x faster JSON parsing
- **SIMD string operations**: StringZilla for accelerated string ops
- **Zero-copy buffers**: Eliminate allocations on message receive
- **Write coalescing**: Batch writes to reduce syscalls
- **Lock-free queues**: moodycamel::ConcurrentQueue
- **mimalloc**: High-performance allocator
- **Template caching**: GTL LRU cache for subject templates
- **Fast parsing**: std::from_chars instead of stol/stoull
- **Lazy header parsing**: Defer parsing until accessed

## Architecture

### Core Components
- `nats_asio::connection`: ASIO-based NATS connection with coroutine support
- `nats_asio::js_*`: JetStream publish/subscribe/ACK APIs
- `nats_asio::kv_*`: JetStream KV operations

### nats_tool Utilities
- `async_input_reader`: Async file/stdin reader with follow mode
- `async_multi_file_reader`: Multi-file reader with glob patterns and watch mode
- `decompression_reader`: Streaming decompressor for gzip/zstd files
- `zip_extractor`: ZIP archive extraction to temporary directory
- `async_http_reader`: HTTP/HTTPS streaming client
- `js_sliding_window`: JetStream ACK batching with retry logic
- `js_ack_processor`: Background ACK processing and timeout handling
- `batch_publisher`: Multi-threaded batch publishing

## Contributing

This is a personal fork with production-focused enhancements. Pull requests welcome for bug fixes and performance improvements.

## License

MIT License - see LICENSE file for details.

Original work Copyright (c) 2019 antlad
Modified work Copyright (c) 2024-2026 mrayva

## Links

- [NATS Documentation](https://docs.nats.io/)
- [JetStream Documentation](https://docs.nats.io/nats-concepts/jetstream)
- [ASIO Documentation](https://think-async.com/Asio/)
