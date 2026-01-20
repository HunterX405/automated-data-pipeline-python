# Automated Data Pipeline

An extensible, production-ready data pipeline framework for automatically fetching, transforming, and loading data from various APIs. Currently supports NFT collection data with plans to extend to stocks/crypto prices, weather data, and other datasets.

## Project Goals

- **Multi-domain data ingestion**: Support NFTs, financial markets, weather, and other API-based datasets
- **Automated scheduling**: Run pipelines on schedules without manual intervention
- **Efficient caching**: Redis-backed HTTP response caching with intelligent cache validation
- **Robust error handling**: Automatic retries with exponential backoff and sleep timeout for transient failures
- **Scalable architecture**: Async patterns with configurable concurrency limits
- **Future web interface**: Interactive web app to explore and visualize collected datasets

## Architecture

The pipeline follows an **ETL (Extract, Transform, Load)** pattern:

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌─────────────┐
│   Extract   │ --> │  Transform   │ --> │    Load     │ --> │   Storage   │
│  (Collect)  │     │ (Normalize)  │     │  (PyArrows) │     │  (Parquet)  │
└─────────────┘     └──────────────┘     └─────────────┘     └─────────────┘
```

### Key Components

- **`CacheAPI`**: Async HTTP client with Redis caching, retry logic, and HTTP/2 support
- **Collectors**: Domain-specific data fetching modules (currently NFTs)
- **Transformers**: Normalization logic to convert raw API responses into structured Arrow tables
- **Loaders**: Parquet file writers with configurable compression and partitioning

## Project Structure

```
automated-data-pipeline/
├── main.py                    # Entry point for running pipelines
├── pipeline/
│   ├── collectors/            # Data fetching modules
│   │   ├── api_clients.py    # API client factory functions
│   │   └── nft.py            # NFT collection logic
│   ├── transform/             # Data normalization
│   │   └── normalize.py      # NFT normalization to Arrow tables
│   ├── load/                  # Storage layer
│   │   └── store.py          # Parquet file writer
│   └── utils/                 # Shared utilities
│       ├── api.py            # CacheAPI HTTP client
│       ├── cache.py          # Redis cache implementation
│       └── logs.py           # Logging configuration
├── output/                    # Generated Parquet files
├── logs/                      # Application logs
├── docker-compose.yml         # Docker services (Redis, etc.)
├── Dockerfile                 # Container image definition
└── pyproject.toml            # Python dependencies
```

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- Docker and Docker Compose (for Redis)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd automated-data-pipeline
   ```

2. **Install dependencies**:
   ```bash
   uv sync
   ```

3. **Set up environment variables**:
   Create a `.env` file in the project root:
   ```env
   OPENSEA_API_KEY=your_opensea_api_key_here
   REDIS_URL=redis://localhost:6379/0
   ```

4. **Start Redis** (using Docker Compose):
   ```bash
   docker-compose up -d redis
   ```

### Running the NFT Pipeline

Run the pipeline to collect NFT data:

```bash
# Basic run
uv run main.py

# With file logging
uv run main.py --logfile
```

The pipeline will:
1. Fetch NFT collection metadata from OpenSea API
2. Collect all NFTs from the collection
3. Enrich NFTs with trait metadata
4. Normalize data into structured Arrow tables
5. Write Parquet files to `output/` directory

**Output files**:
- `output/{collection_slug}_nfts.parquet` - Main NFT records
- `output/{collection_slug}_traits.parquet` - Flattened trait data

### Using Docker

Run the entire stack with Docker Compose:

```bash
docker-compose up --build
```

## Current Status

### Implemented Features

- **NFT Pipeline**:
  - OpenSea API integration with pagination support
  - Async producer-consumer pattern for concurrent metadata fetching
  - Trait enrichment from metadata URLs
  - PyArrow schema-based normalization
  - Parquet output with proper data types

- **HTTP Client (`CacheAPI`)**:
  - HTTP/2 support with connection pooling
  - Redis-backed response caching
  - Cache-Control header compliance (ETag, Last-Modified, stale-while-revalidate)
  - Automatic retries with exponential backoff (via Stamina)
  - Configurable concurrency limits
  - Real-time statistics tracking

- **Infrastructure**:
  - Docker containerization
  - Redis for caching
  - Structured logging with file rotation
  - Environment-based configuration

### Planned Features

- **Workflow Orchestration**: Integrate Prefect/Dagster for scheduling and dependency management
- **Additional Data Domains**:
  - Stock/crypto price data
  - Weather observations and forecasts
  - Extensible framework for new domains
- **Data Quality**: Schema validation, data quality checks, and monitoring
- **Web Application**: FastAPI backend + interactive frontend for data exploration
- **Enhanced Storage**: Optional database layer (DuckDB/PostgreSQL) for complex queries
- **Monitoring**: Metrics collection, alerting, and observability improvements

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `OPENSEA_API_KEY` | OpenSea API key for NFT data | Yes (for NFTs) |
| `REDIS_URL` | Redis connection URL | Yes |

### Pipeline Parameters

Currently configured in `main.py`:
- `nft_slug`: Collection identifier (default: `'dxterminal'`)
- Concurrency limits: Configurable per API client in `api_clients.py`

## Performance

The pipeline is optimized for efficiency:

- **Concurrent fetching**: Uses asyncio with configurable worker pools
- **Smart caching**: Redis cache reduces redundant API calls
- **HTTP/2**: Multiplexed connections for better throughput
- **Connection pooling**: Reuses connections to minimize overhead

**Measured performance** (NFT collection processing ~36,784 API calls):

- **Cold start** (no cache): ~25 minutes
  - All API calls fetched from server
  - Full data collection and enrichment
  
- **Cached run**: ~1.7 minutes
  - ~93% reduction in execution time
  - Responses served from Redis cache
  - Cache validation and updates handled automatically

**Performance characteristics**:
- Memory efficient: Streaming processing with bounded queues
- Scalable: Handles large collections with configurable concurrency
- Resilient: Automatic retries handle transient API failures

### Code Style

The project follows Python best practices:
- Type hints throughout
- Async/await for I/O operations
- Dataclasses for configuration
- Structured logging

### Adding a New Data Domain

To add support for a new data source:

1. **Create collector** (`pipeline/collectors/{domain}.py`):
   - Implement fetch functions using `CacheAPI`
   - Handle pagination/rate limiting as needed

2. **Create transformer** (`pipeline/transform/{domain}.py`):
   - Define PyArrow schema
   - Implement normalization function

3. **Update main pipeline** (`main.py`):
   - Add domain-specific pipeline execution
   - Configure output paths

4. **Add to orchestrator** (when implemented):
   - Create domain-specific flow
   - Add to master schedule

## Logging

Logs are written to:
- **Console**: Real-time progress and statistics
- **File** (optional): `logs/project.log` with rotation

Log levels:
- `INFO`: Pipeline progress, statistics
- `DEBUG`: HTTP requests, cache hits/misses, detailed operations
- `WARNING`: Non-fatal issues (missing metadata, etc.)
- `ERROR`: Failures requiring attention

---

**Built with**: Python 3.12, httpx, PyArrow, Redis, Stamina, Docker
