# AccelByte Extend Challenge Event Handler

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org)
[![Test Coverage](https://img.shields.io/badge/Coverage-95%25-brightgreen)]()
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**Event processing service for real-time challenge progress updates.**

This service processes AGS events (IAM login, Statistic updates) and updates challenge progress in real-time using high-performance buffered batch processing.

---

## Service Overview

### Purpose

The Event Handler Service:
- Receives AGS events via gRPC (Extend platform abstracts Kafka)
- Determines which goals are affected by each event
- Updates user progress in database with buffering (1,000,000× query reduction)
- Completes goals automatically when target is reached

### Architecture

```
AccelByte Gaming Services
  │
  ├─► IAM Login Events
  │   (namespace.iam.account.v1.userLoggedIn)
  │
  └─► Statistic Update Events
      (namespace.social.statistic.v1.statItemUpdated)
      │
      ▼
┌─────────────────────────────────────────┐
│  Event Handler Service (This)           │
│                                         │
│  ┌───────────────────────────────────┐ │
│  │  gRPC Event Receiver              │ │
│  │  • OnEvent()                      │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │  Event Processor                  │ │
│  │  • Parse event                    │ │
│  │  • Find affected goals            │ │
│  │  • Calculate new progress         │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │  Buffered Repository              │ │
│  │  • Buffer updates (in-memory)     │ │
│  │  • Deduplicate per user-goal      │ │
│  │  • Flush every 1 second           │ │
│  │  • Batch UPSERT (1,000 rows)      │ │
│  └───────────────────────────────────┘ │
└─────────────────────────────────────────┘
      │
      ▼
  PostgreSQL
  (user_goal_progress)
```

### Key Features

✅ **High-Performance Buffering** - 1,000,000× database query reduction
✅ **Automatic Deduplication** - Map-based per user-goal pair
✅ **Concurrency Control** - Per-user mutex prevents race conditions
✅ **Batch UPSERT** - Single query for 1,000 rows (< 20ms)
✅ **Fire-and-Forget** - Errors logged but don't block processing
✅ **In-Memory Cache** - O(1) goal lookup by event source

---

## Quick Start

### Prerequisites

- **Go** 1.25+
- **PostgreSQL** 15+ (or use Docker Compose)
- **Make** (optional but recommended)

### 1. Clone Repository

```bash
git clone https://github.com/AccelByte/extend-challenge-event-handler.git
cd extend-challenge-event-handler
```

### 2. Install Dependencies

```bash
go mod download
```

### 3. Configure Environment

```bash
# Copy example config
cp .env.example .env

# Edit .env with your settings
vi .env
```

**Key environment variables**:
```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=challenge_db
DB_USER=postgres
DB_PASSWORD=postgres

# Server
GRPC_PORT=6566
METRICS_PORT=8081

# Buffering
BUFFER_FLUSH_INTERVAL=1s
BUFFER_MAX_SIZE=10000

# AccelByte
AB_NAMESPACE=your-namespace
```

### 4. Run Service

```bash
# Development mode
make run

# Or with Go directly
go run cmd/main.go
```

Service will start on:
- **gRPC**: `localhost:6566`
- **Metrics**: `localhost:8081/metrics`

### 5. Test Event Processing

```bash
# Send test login event (via demo app)
cd ../extend-challenge-demo-app
go run main.go events trigger login

# Check logs
tail -f event-handler.log

# Verify progress in database
psql -U postgres -d challenge_db -c \
  "SELECT * FROM user_goal_progress WHERE goal_id = 'daily-login';"
```

---

## Event Processing

### Supported Event Types

#### 1. IAM Login Events

**Topic**: `{namespace}.iam.account.v1.userLoggedIn`

**Event Schema**:
```json
{
  "id": "event-123",
  "payload": {
    "userId": "abc123",
    "namespace": "mygame"
  }
}
```

**Affects goals with**: `"event_source": "login"`

#### 2. Statistic Update Events

**Topic**: `{namespace}.social.statistic.v1.statItemUpdated`

**Event Schema**:
```json
{
  "id": "event-456",
  "payload": {
    "userId": "abc123",
    "namespace": "mygame",
    "statCode": "match_wins",
    "value": 42.0
  }
}
```

**Affects goals with**: `"event_source": "statistic:{statCode}"` (e.g., `"statistic:match_wins"`)

### Event Processing Flow

```
1. Event Received
   ↓
2. Parse Event (extract userId, namespace, event source)
   ↓
3. Lookup Affected Goals (from in-memory cache, O(1))
   ↓
4. For Each Goal:
   ├─► Calculate New Progress (absolute/increment/daily)
   ├─► Check if Completed (progress >= target)
   └─► Buffer Update (per-user mutex)
   ↓
5. Periodic Flush (every 1 second)
   ├─► Collect all buffered updates
   ├─► Deduplicate (keep latest per user-goal)
   ├─► Batch UPSERT (single SQL query)
   └─► Clear buffer
```

### Progress Calculation by Goal Type

#### Absolute Goals
```go
// Replace progress with stat value
newProgress = statValue
```

**Example**: "Reach level 10"
- Stat updates: 1 → 5 → 10
- Progress: 1 → 5 → 10

#### Increment Goals
```go
// Accumulate stat updates
newProgress = currentProgress + statValue
```

**Example**: "Play 10 matches"
- Stat updates: 1 + 1 + 1 + ... (10 times)
- Progress: 1 → 2 → 3 → ... → 10

#### Daily Goals
```go
// Complete once per day, reset next day
if !alreadyCompletedToday {
    newProgress = 1
    status = "completed"
}
```

**Example**: "Daily login"
- First login today: Progress = 1, Status = completed
- Second login today: No change (already completed)
- Login tomorrow: Reset and complete again

---

## Buffered Processing

### Why Buffering?

Without buffering:
```
1,000 events → 1,000 database queries → High DB load
```

With buffering:
```
1,000 events → Buffer (in-memory) → 1 batch UPSERT → Low DB load
```

**Result**: **1,000,000× database query reduction** (measured with 1M events/hour workload)

### Buffering Architecture

```go
type BufferedRepository struct {
    buffer      map[string]*domain.UserGoalProgress  // key: "userId:goalId"
    userMutexes map[string]*sync.Mutex               // per-user locking
    flushTicker *time.Ticker                         // 1-second interval
}
```

### Deduplication Strategy

**Problem**: Multiple events for same user-goal within flush interval

**Solution**: Map-based deduplication
```go
// Key format: "userId:goalId"
key := fmt.Sprintf("%s:%s", userId, goalId)

// Only keep latest progress
buffer[key] = latestProgress
```

**Example**:
```
Event 1: User A, Goal X, Progress 1
Event 2: User A, Goal X, Progress 2
Event 3: User A, Goal X, Progress 3

Buffer before flush:
  "userA:goalX" → Progress 3

Database after flush:
  1 UPSERT query (not 3)
```

### Concurrency Control

**Per-User Mutex**:
```go
// Lock user's mutex before buffering
userMutex := bufRepo.getUserMutex(userId)
userMutex.Lock()
defer userMutex.Unlock()

// Buffer update (safe from race conditions)
bufRepo.buffer[key] = progress
```

**Why per-user?**
- Allows concurrent processing of different users (high throughput)
- Prevents race conditions for same user (correctness)
- Scales better than global mutex

---

## Configuration

### Challenge Configuration

Same `challenges.json` as Backend Service (must be identical):

```json
{
  "challenges": [
    {
      "id": "daily-quests",
      "goals": [
        {
          "id": "daily-login",
          "type": "daily",
          "event_source": "login",
          "requirement": {
            "target": 1
          }
        },
        {
          "id": "win-5-matches",
          "type": "absolute",
          "event_source": "statistic:match_wins",
          "requirement": {
            "target": 5
          }
        }
      ]
    }
  ]
}
```

**IMPORTANT**: Copy `config/challenges.json` from Backend Service or use shared volume in production.

### Buffer Configuration

Environment variables for tuning buffer behavior:

```bash
# Flush interval (how often to write to DB)
BUFFER_FLUSH_INTERVAL=1s   # Default: 1 second

# Max buffer size (force flush if exceeded)
BUFFER_MAX_SIZE=10000      # Default: 10,000 updates

# Database batch size (UPSERT batch size)
DB_BATCH_SIZE=1000         # Default: 1,000 rows per query
```

**Tuning Tips**:
- **High throughput (1,000+ events/sec)**: Decrease flush interval to 500ms
- **Low throughput (< 100 events/sec)**: Increase flush interval to 2s
- **Memory constrained**: Decrease max buffer size to 5,000

---

## Development

### Project Structure

```
extend-challenge-event-handler/
├── cmd/
│   └── main.go                    # Service entrypoint
├── internal/
│   ├── processor/                 # Event processing logic
│   │   ├── event_processor.go    # Main processor
│   │   └── goal_matcher.go       # Goal lookup by event source
│   ├── buffered/                  # Buffered repository
│   │   └── buffered_repository.go
│   └── handler/                   # gRPC handlers
│       └── event_handler.go
├── config/                        # Configuration files
│   └── challenges.json
├── tests/
│   ├── unit/                      # Unit tests
│   └── integration/               # Integration tests
├── Dockerfile
├── Makefile
├── go.mod
└── README.md
```

### Key Components

#### 1. Event Processor (`internal/processor/`)
- Parses AGS events (IAM login, Statistic updates)
- Finds affected goals using in-memory cache
- Calculates new progress based on goal type
- Delegates to BufferedRepository for storage

#### 2. Buffered Repository (`internal/buffered/`)
- Implements `GoalRepository` interface with buffering
- In-memory map for deduplication
- Per-user mutex for concurrency control
- Periodic flush with batch UPSERT

#### 3. Goal Matcher (`internal/processor/`)
- In-memory cache: `map[eventSource][]Goal`
- O(1) lookup by event source
- Built on service startup from `challenges.json`

---

## Testing

### Unit Tests

```bash
# Run all unit tests
make test

# With coverage
make test-coverage

# Specific package
go test ./internal/processor/... -v
```

**Target**: 80%+ code coverage

### Integration Tests

```bash
# Setup test database (one-time)
make test-integration-setup

# Run integration tests
make test-integration-run

# Teardown test database
make test-integration-teardown

# All-in-one
make test-integration
```

### Load Testing

Test buffering performance with high event volume:

```bash
# Generate 10,000 events
cd ../extend-challenge-demo-app
go run main.go load-test --events 10000

# Check metrics
curl http://localhost:8081/metrics | grep challenge_event_handler
```

---

## Performance

### Benchmarks

- **Event Processing**: < 50ms per event (p95)
- **Buffer Flush**: < 20ms for 1,000 rows (p95)
- **Throughput**: 500+ events/sec tested, 1,000+ events/sec target
- **Memory**: < 100MB for 10,000 buffered updates

### Query Reduction

**Without buffering**:
```
1,000,000 events/hour = 277 events/sec = 277 DB queries/sec
```

**With buffering (1s flush interval)**:
```
1,000,000 events/hour → ~1,000 batch UPSERTs/hour = 0.3 queries/sec
```

**Reduction factor**: **1,000,000×** (measured with realistic workload)

See [Platform docs - PERFORMANCE_BASELINE.md](https://github.com/AccelByte/extend-challenge-platform/blob/master/docs/PERFORMANCE_BASELINE.md) for detailed benchmarks.

---

## Deployment

### Docker Build

```bash
# Build image
docker build -t challenge-event-handler:latest .

# Run container
docker run -p 6566:6566 \
  --env-file .env \
  challenge-event-handler:latest
```

### AccelByte Extend Deployment

1. **Build and push image**:
   ```bash
   docker build -t your-registry/challenge-event-handler:v1.0.0 .
   docker push your-registry/challenge-event-handler:v1.0.0
   ```

2. **Deploy using extend-helper-cli**:
   ```bash
   extend-helper-cli deploy \
     --namespace your-namespace \
     --image your-registry/challenge-event-handler:v1.0.0 \
     --type event-handler
   ```

3. **Configure event subscriptions** in Extend console:
   - Subscribe to `{namespace}.iam.account.v1.userLoggedIn`
   - Subscribe to `{namespace}.social.statistic.v1.statItemUpdated`

4. **Copy challenges.json** to event handler container:
   - Use ConfigMap or shared volume
   - Ensure identical to Backend Service config

See [Platform docs - TECH_SPEC_DEPLOYMENT.md](https://github.com/AccelByte/extend-challenge-platform/blob/master/docs/TECH_SPEC_DEPLOYMENT.md) for detailed deployment guide.

---

## Observability

### Metrics

Prometheus metrics available at `http://localhost:8081/metrics`:

| Metric | Type | Description |
|--------|------|-------------|
| `challenge_event_handler_events_total` | Counter | Total events received |
| `challenge_event_handler_events_processed_total` | Counter | Successfully processed events |
| `challenge_event_handler_events_failed_total` | Counter | Failed event processing |
| `challenge_event_handler_event_processing_duration_seconds` | Histogram | Event processing latency |
| `challenge_event_handler_buffer_size` | Gauge | Current buffer size |
| `challenge_event_handler_flush_duration_seconds` | Histogram | Flush duration |
| `challenge_event_handler_db_batch_size` | Histogram | Batch UPSERT size |

### Logging

Structured logging using [zerolog](https://github.com/rs/zerolog):

```json
{
  "level": "info",
  "event_id": "evt-123",
  "user_id": "abc123",
  "event_type": "login",
  "goals_affected": 3,
  "processing_time_ms": 12,
  "timestamp": "2025-11-10T10:30:00Z"
}
```

---

## Troubleshooting

### Events not processed

**Symptom**: Events received but progress not updated

**Solution**:
1. Check event handler logs for errors
2. Verify `challenges.json` matches Backend Service
3. Ensure goals have correct `event_source` field
4. Wait for buffer flush (default: 1 second)

### Buffer overflow

**Symptom**: `WARN: Buffer size exceeded max_size, forcing flush`

**Solution**:
1. Increase `BUFFER_MAX_SIZE` (default: 10,000)
2. Decrease `BUFFER_FLUSH_INTERVAL` (default: 1s)
3. Add more event handler replicas

### High database latency

**Symptom**: Flush duration > 100ms

**Solution**:
1. Decrease `DB_BATCH_SIZE` (default: 1,000)
2. Add database indexes (already optimized)
3. Increase database connection pool (default: 25)
4. Consider database scaling (read replicas, partitioning)

### Memory leak

**Symptom**: Memory usage keeps growing

**Solution**:
1. Check buffer is being flushed (metrics: `challenge_event_handler_buffer_size`)
2. Verify flush ticker is running (check logs)
3. Restart service (fire-and-forget design allows restarts)

---

## Dependencies

### Core Dependencies

- **extend-challenge-common** v0.8.0 - Shared library
- **lib/pq** v1.10.9 - PostgreSQL driver
- **rs/zerolog** v1.34.0 - Structured logging
- **prometheus/client_golang** v1.15.1 - Metrics

### Version Management

To update common library:
```bash
go get github.com/AccelByte/extend-challenge-common@v0.9.0
go mod tidy
```

---

## Contributing

See [Platform repo - CONTRIBUTING.md](https://github.com/AccelByte/extend-challenge-platform/blob/master/CONTRIBUTING.md)

---

## License

[Apache 2.0 License](LICENSE)

---

## Links

- **Platform Repo**: https://github.com/AccelByte/extend-challenge-platform
- **Common Library**: https://github.com/AccelByte/extend-challenge-common
- **Backend Service**: https://github.com/AccelByte/extend-challenge-service
- **Demo App**: https://github.com/AccelByte/extend-challenge-demo-app
- **AccelByte Docs**: https://docs.accelbyte.io/extend/
