# CQRS Kafka Performance Testing Tool

A comprehensive latency measurement tool for testing CQRS command processing through Kafka.

## Features

- **End-to-end latency measurement**: Tracks time from command send to response received
- **Detailed statistics**: Min, max, mean, standard deviation, and percentiles (p50, p75, p90, p95, p99, p99.9, p99.99)
- **Histogram-based analysis**: Uses HdrHistogram for accurate latency distribution
- **Configurable load**: Control number of requests, concurrency, and warmup
- **Colored output**: Easy-to-read results with visual indicators
- **Throughput measurement**: Requests per second calculation

## Prerequisites

1. **Kafka server running**: Make sure you have a Kafka instance accessible
2. **Command server running**: The CQRS command server must be running to process commands
3. **Topics created**: Ensure the command and command-response topics exist

## Usage

### Quick Start

```bash
# Run with default settings (1000 requests, 10 workers)
cargo run --release --bin cqrs-performance -- --bootstrap-server localhost:9092

# Custom test configuration
cargo run --release --bin cqrs-performance -- \
  --count 10000 \
  --workers 50 \
  --warmup 500 \
  --bootstrap-server localhost:9092 \
  --command-topic commands
```

### Command-Line Options

```
Options:
  -c, --count <COUNT>
          Number of commands to send [default: 1000]

  -w, --workers <WORKERS>
          Number of concurrent workers [default: 10]

      --warmup <WARMUP>
          Number of warmup requests (not counted in statistics) [default: 100]

  -b, --bootstrap-server <BOOTSTRAP_SERVER>
          Bootstrap server (overrides config)

  -t, --command-topic <COMMAND_TOPIC>
          Command topic (overrides config)

  -v, --verbose
          Enable detailed output

  -h, --help
          Print help

  -V, --version
          Print version
```

### Using Configuration File

Create a `Settings.toml` file in the `cqrs-performance` directory:

```toml
bootstrap_server = "localhost:9092"
command_topic = "commands"
command_response_topic = "command-responses"
service_id = "perf-test-client"
log_level = "info"
```

Then run without arguments:

```bash
cargo run --release --bin cqrs-performance
```

## Example Output

```
═══════════════════════════════════════════════════
      CQRS Kafka Performance Testing Tool
═══════════════════════════════════════════════════

Configuration:
  Commands:            1000
  Workers:             10
  Warmup:              100
  Bootstrap Server:    localhost:9092
  Command Topic:       commands
  Service ID:          perf-test-client

Running warmup...
Warmup complete!

Starting performance test...

═══════════════════════════════════════════════════
         KAFKA CQRS LATENCY TEST RESULTS
═══════════════════════════════════════════════════

Summary:
  Total Requests:      1000
  Successful:          1000 (100.0%)
  Failed:              0
  Total Duration:      5.42s
  Throughput:          184.50 req/s

Latency Distribution (milliseconds):
  Min:                 15.23 ms
  Mean:                52.45 ms
  Max:                 245.67 ms
  Std Dev:             18.32 ms

Percentiles:
  p50 (median):        48.12 ms
  p75:                 62.34 ms
  p90:                 78.56 ms
  p95:                 89.23 ms ✓
  p99:                 156.78 ms ✓
  p99.9:               234.12 ms
  p99.99:              245.67 ms

═══════════════════════════════════════════════════
```

## Test Scenarios

### 1. Baseline Latency Test
Measure typical latency with moderate load:

```bash
cargo run --release --bin cqrs-performance -- \
  --count 1000 \
  --workers 10 \
  --bootstrap-server localhost:9092
```

### 2. High Throughput Test
Test system behavior under heavy load:

```bash
cargo run --release --bin cqrs-performance -- \
  --count 10000 \
  --workers 100 \
  --warmup 500 \
  --bootstrap-server localhost:9092
```

### 3. Low Concurrency Test
Measure latency with minimal contention:

```bash
cargo run --release --bin cqrs-performance -- \
  --count 1000 \
  --workers 1 \
  --bootstrap-server localhost:9092
```

### 4. Stress Test
Push the system to its limits:

```bash
cargo run --release --bin cqrs-performance -- \
  --count 50000 \
  --workers 500 \
  --warmup 1000 \
  --bootstrap-server localhost:9092
```

## Interpreting Results

### Latency Metrics

- **Min/Max**: Range of observed latencies
- **Mean**: Average latency (can be skewed by outliers)
- **Std Dev**: Variability in latencies (lower is more consistent)

### Percentiles

- **p50 (median)**: 50% of requests completed faster than this
- **p95**: 95% of requests completed faster than this (common SLA target)
- **p99**: 99% of requests completed faster than this (tail latency)
- **p99.9/p99.99**: Extreme tail latency (important for user experience)

### Checkmarks (✓)

- ✓ indicates acceptable latency (p95 < 100ms, p99 < 200ms)
- ! indicates high latency that may need investigation

### Throughput

- Requests per second successfully processed
- Consider in relation to your system's capacity

## Troubleshooting

### High Latency

1. **Check Kafka broker performance**: Use Kafka monitoring tools
2. **Network latency**: Test with `ping` to Kafka broker
3. **Command server load**: Monitor CPU/memory on the server
4. **Kafka topic configuration**: Check replication factor and partition count

### Failed Requests

1. **Check server logs**: Look for errors in the command server
2. **Verify topics exist**: Ensure command and response topics are created
3. **Check Kafka connectivity**: Verify network access to Kafka broker
4. **Increase timeout**: Commands might be timing out

### Low Throughput

1. **Increase workers**: More concurrency can improve throughput
2. **Check bottlenecks**: Monitor Kafka producer/consumer metrics
3. **Tune Kafka**: Adjust batch size, linger.ms, compression
4. **Scale command server**: Add more server instances

## Tips for Accurate Testing

1. **Always use --release**: Debug builds are much slower
2. **Run warmup**: Allows JIT compilation and cache warming
3. **Consistent environment**: Run tests on same hardware/network
4. **Isolate traffic**: Avoid running other Kafka consumers/producers during tests
5. **Multiple runs**: Run tests multiple times and compare results
6. **Monitor resources**: Watch CPU, memory, and network during tests

## Advanced Usage

### Custom Kafka Configuration

For advanced Kafka tuning, modify the code in `src/main.rs` to pass additional configuration to `KafkaOutboundChannel`.

### Adding Custom Commands

Modify the `CreateUserCommand` struct and implement the `Command` trait for your custom command types.

### Integration with CI/CD

```bash
# Run as part of CI pipeline
cargo run --release --bin cqrs-performance -- \
  --count 100 \
  --workers 10 \
  --bootstrap-server $KAFKA_URL \
  > perf-results.txt

# Check if p95 latency is acceptable (example threshold: 100ms)
# Parse output and fail build if threshold exceeded
```
