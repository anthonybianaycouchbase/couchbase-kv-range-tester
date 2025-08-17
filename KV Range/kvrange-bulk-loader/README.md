# Couchbase Bulk Loader

High-performance Java application for bulk loading documents to Couchbase with two main components:

## 📦 Components

### 1. **BulkLoader** - Document Writer
- **Purpose**: Import millions of JSON client documents
- **Format**: JSON documents with fake client data (800 bytes default)
- **Performance**: ~57k docs/s with optimized settings

### 2. **KeyRangeReader** - Document Reader & Performance Tester
- **Purpose**: Read documents within a key range + comprehensive performance benchmarking
- **Features**: 
  - Single range reading with timing
  - Multi-scenario performance benchmarking
  - Different range sizes (1K to 1M documents)
  - High Concurrency configuration (8,192 ops, 32 connections)
- **Use case**: Verify data, measure read performance, optimize cluster settings

## 🚀 Quick Start

### Build
```bash
mvn -q -DskipTests package
```

### Write 100M Documents (JSON Clients)
```bash
# Environment variables
export CB_HOSTS="ec2-15-237-220-120.eu-west-3.compute.amazonaws.com,ec2-15-237-93-147.eu-west-3.compute.amazonaws.com,ec2-35-181-7-225.eu-west-3.compute.amazonaws.com"
export CB_USER="Administrator"
export CB_PASS="password"
export CB_BUCKET="KVRANGE"

# Run bulk loader (100k docs/s target)
java -cp target/kvrange-bulk-loader-1.0.0.jar com.example.bulkloader.BulkLoader
```

### Read Single Range
```bash
# Read range client::10000 to client::11000
export START_KEY="client::10000"
export END_KEY="client::11000"
java -cp target/kvrange-bulk-loader-1.0.0.jar com.example.bulkloader.KeyRangeReader
```

### Run Performance Benchmark
```bash
# Comprehensive performance testing with optimal configuration
export MODE="PERFORMANCE"
java -cp target/kvrange-bulk-loader-1.0.0.jar com.example.bulkloader.KeyRangeReader
```

## ⚙️ Configuration

### BulkLoader Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CB_HOSTS` | `ec2-15-237-220-120.eu-west-3.compute.amazonaws.com,ec2-15-237-93-147.eu-west-3.compute.amazonaws.com,ec2-35-181-7-225.eu-west-3.compute.amazonaws.com` | Couchbase cluster nodes |
| `CB_USER` | `Administrator` | Username |
| `CB_PASS` | `password` | Password |
| `CB_BUCKET` | `KVRANGE` | Bucket name |
| `DOC_COUNT` | `100000000` | Number of documents to insert |
| `DOC_SIZE` | `800` | Target document size in bytes |
| `CONCURRENCY` | `16384` | In-flight operations |
| `KV_CONNECTIONS` | `64` | Connections per KV node |
| `TARGET_RPS` | `100000` | Target requests per second (0 = no limit) |
| `DURABILITY` | `NONE` | Durability level (NONE, MAJORITY, PERSIST_TO_MAJORITY) |
| `COMPRESSION_ENABLED` | `false` | Enable compression |
| `KEY_PREFIX` | `client::` | Key prefix for documents |

### KeyRangeReader Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MODE` | `SINGLE` | Mode: `SINGLE` or `PERFORMANCE` |
| `START_KEY` | - | Start key (e.g., `client::10000`) |
| `END_KEY` | - | End key (e.g., `client::11000`) |
| `KEY_PREFIX` | `client::` | Key prefix for numeric ranges |
| `START` | `10000` | Start number for numeric range |
| `END` | `11000` | End number for numeric range |

**Note**: KeyRangeReader uses High Concurrency configuration (8,192 ops, 32 connections) for all operations.

## 📊 Performance

### Current Performance (3-node cluster)
- **Write**: ~57,000 docs/s (JSON clients, 800 bytes)
- **Read**: Variable based on range size with optimal configuration
- **Durability**: NONE (RAM-only writes for maximum IOPS)

### Performance Benchmark Results
Latest benchmark results with High Concurrency configuration:

| Scenario | Range Size | Performance | Time per Document |
|----------|------------|-------------|-------------------|
| Small (1K) | 1,000 | 15,834 docs/s | 1.894 ms |
| Medium (10K) | 10,000 | 68,556 docs/s | 0.072 ms |
| Large (100K) | 100,000 | 85,124 docs/s | 0.017 ms |
| Very Large (1M) | 1,000,000 | 97,175 docs/s | 0.011 ms |

### Performance Benchmark Scenarios
When running `MODE=PERFORMANCE`, the following scenarios are tested with High Concurrency:

1. **Small Range (1K)** - 1,000 documents
2. **Medium Range (10K)** - 10,000 documents  
3. **Large Range (100K)** - 100,000 documents
4. **Very Large Range (1M)** - 1,000,000 documents

### Key Performance Insights
- **Optimal Read Performance**: ~97,175 docs/s for 1M documents
- **Best Time per Document**: 0.011 ms (11.1 μs) for large ranges
- **Configuration**: High Concurrency (8,192 ops, 32 connections) for all scenarios
- **Zero Errors**: All scenarios completed without errors
- **Scale Efficiency**: Performance improves with range size due to economies of scale

### Time Estimation for Range Scans
Based on High Concurrency configuration:

| Range Size | Estimated Time | Time per Document |
|------------|----------------|-------------------|
| 1,000 docs | ~1.9 seconds | 1.9 ms |
| 10,000 docs | ~0.7 seconds | 0.07 ms |
| 100,000 docs | ~1.7 seconds | 0.017 ms |
| 1,000,000 docs | ~11.1 seconds | 0.011 ms |

**Formula**: `Time = Number of Documents × 0.011 ms` (for ranges > 100K)

## 📝 Document Format

JSON documents with fake client data:
```json
{
  "id": 1234567,
  "firstName": "Jean",
  "lastName": "Marie",
  "email": "jean123@gmail.com",
  "phone": "0123456789",
  "dateOfBirth": "1985-03-15",
  "address": "Rue de la Paix 42",
  "city": "Paris",
  "postalCode": "75001",
  "country": "France",
  "registrationDate": "2023-01-15T10:30:00Z",
  "lastLogin": "2024-01-20T14:45:00Z",
  "status": "active",
  "premium": true,
  "points": 1250,
  "randomField1": "randomValue123",
  "randomField2": "anotherRandomValue456"
}
```

## 🔧 Requirements

- **Java**: 17+
- **Maven**: 3.6+
- **Couchbase**: 7.0+ cluster # couchbase-kv-range-tester
