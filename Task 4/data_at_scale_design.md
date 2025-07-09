# Exercise 4: Data at Scale - Advanced Design Document

## Executive Summary

This document outlines a comprehensive solution for processing millions of product records from multiple sources in a scalable, cost-effective manner. The solution addresses high-volume data ingestion, multi-source reconciliation, deduplication, and real-time availability for downstream systems.

## Problem Statement

**Current Challenges:**

- Scale: Millions of rows instead of hundreds
- Multiple data sources with overlapping/different schemas
- Different update frequencies across sources
- Need for data reconciliation and deduplication
- Performance requirement: minutes, not hours
- Cost optimization is critical
- High availability requirements

## Solution Architecture

### 1. Data Management and Ingestion

#### 1.1 Multi-Source Data Ingestion Strategy

```python
# Example configuration for multiple data sources
DATA_SOURCES = {
    "api_primary": {
        "type": "rest_api",
        "url": "https://api.primary.com/products",
        "frequency": "hourly",
        "schema": "products_v1",
        "priority": 1
    },
    "api_secondary": {
        "type": "rest_api",
        "url": "https://api.secondary.com/items",
        "frequency": "daily",
        "schema": "items_v2",
        "priority": 2
    },
    "file_feed": {
        "type": "s3_csv",
        "location": "s3://data-feeds/products/",
        "frequency": "weekly",
        "schema": "legacy_products",
        "priority": 3
    }
}
```

**Ingestion Architecture:**

- **Apache Kafka** or **Amazon Kinesis** for real-time streaming
- **Apache Airflow** for batch orchestration
- **AWS Lambda** or **Google Cloud Functions** for event-driven processing
- **Schema Registry** (Confluent) for schema evolution management

#### 1.2 Deduplication Strategy

**Multi-Level Deduplication:**

1. **Source-Level Deduplication:**

   - Primary key matching within each source
   - Timestamp-based conflict resolution

2. **Cross-Source Deduplication:**
   - Fuzzy matching algorithms (Levenshtein distance, Jaro-Winkler)
   - Machine learning-based entity resolution
   - Probabilistic matching using record linkage

```python
# Example deduplication logic
class ProductDeduplicator:
    def __init__(self):
        self.similarity_threshold = 0.85
        self.ml_model = load_entity_resolution_model()

    def deduplicate_products(self, products_batch):
        """
        Multi-stage deduplication process
        """
        # Stage 1: Exact match on identifiers
        exact_matches = self.find_exact_matches(products_batch)

        # Stage 2: Fuzzy matching on product attributes
        fuzzy_matches = self.find_fuzzy_matches(products_batch)

        # Stage 3: ML-based entity resolution
        ml_matches = self.ml_entity_resolution(products_batch)

        # Stage 4: Create master records
        return self.create_master_records(exact_matches, fuzzy_matches, ml_matches)
```

#### 1.3 Schema Management and Data Harmonization

**Schema Evolution Strategy:**

- **Apache Avro** or **Protocol Buffers** for schema definition
- **Schema Registry** for centralized schema management
- **Data Contracts** between teams for schema changes

**Data Harmonization:**

```python
# Example schema mapping configuration
SCHEMA_MAPPINGS = {
    "products_v1": {
        "product_id": "id",
        "product_name": "title",
        "cost": "price",
        "product_category": "category"
    },
    "items_v2": {
        "item_id": "id",
        "item_title": "title",
        "item_price": "price",
        "item_cat": "category"
    }
}

class SchemaHarmonizer:
    def harmonize_record(self, record, source_schema):
        """Transform record to unified schema"""
        mapping = SCHEMA_MAPPINGS.get(source_schema, {})
        harmonized = {}

        for target_field, source_field in mapping.items():
            if source_field in record:
                harmonized[target_field] = self.transform_value(
                    record[source_field], target_field
                )

        return harmonized
```

### 2. Pipeline Orchestration and Processing

#### 2.1 Distributed Processing Architecture

**Technology Stack:**

- **Apache Spark** for large-scale data processing
- **Kubernetes** for container orchestration
- **Apache Kafka** for stream processing
- **Redis** for caching and state management

**Processing Pipeline:**

```python
# Spark-based processing pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

class DistributedProductProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("ProductDataProcessor") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

    def process_batch(self, input_paths, output_path):
        """
        Process large batch of product data
        """
        # Read from multiple sources
        df_primary = self.spark.read.parquet(input_paths["primary"])
        df_secondary = self.spark.read.parquet(input_paths["secondary"])

        # Harmonize schemas
        df_primary_harmonized = self.harmonize_schema(df_primary, "primary")
        df_secondary_harmonized = self.harmonize_schema(df_secondary, "secondary")

        # Union and deduplicate
        df_combined = df_primary_harmonized.union(df_secondary_harmonized)
        df_deduplicated = self.deduplicate_distributed(df_combined)

        # Write to output
        df_deduplicated.write.mode("overwrite").parquet(output_path)
```

#### 2.2 Idempotency and Reliability

**Idempotency Strategies:**

- **Exactly-once processing** using Kafka transactions
- **Idempotent keys** for duplicate request handling
- **Checkpointing** for recovery from failures

```python
class IdempotentProcessor:
    def __init__(self, checkpoint_storage):
        self.checkpoint_storage = checkpoint_storage
        self.processed_batches = set()

    def process_batch_idempotent(self, batch_id, batch_data):
        """
        Process batch with idempotency guarantee
        """
        if batch_id in self.processed_batches:
            return self.get_cached_result(batch_id)

        # Process batch
        result = self.process_batch(batch_data)

        # Store checkpoint
        self.checkpoint_storage.save_checkpoint(batch_id, result)
        self.processed_batches.add(batch_id)

        return result
```

#### 2.3 Real-time Stream Processing

**Stream Processing Architecture:**

```python
# Kafka Streams / Apache Flink example
class RealTimeProductProcessor:
    def __init__(self):
        self.kafka_config = {
            'bootstrap.servers': 'kafka-cluster:9092',
            'group.id': 'product-processor',
            'auto.offset.reset': 'earliest'
        }

    def process_stream(self):
        """
        Process real-time product updates
        """
        from kafka import KafkaConsumer
        import json

        consumer = KafkaConsumer(
            'product-updates',
            **self.kafka_config,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

        for message in consumer:
            product_data = message.value

            # Real-time deduplication
            if not self.is_duplicate(product_data):
                # Process and forward to downstream
                processed = self.process_product(product_data)
                self.send_to_downstream(processed)
```

### 3. Data Storage and Observability

#### 3.1 Data Lake Architecture

**Storage Strategy:**

- **Raw Layer**: Store original data in S3/GCS in Parquet format
- **Processed Layer**: Cleaned, deduplicated data
- **Curated Layer**: Business-ready, aggregated data
- **Serving Layer**: Optimized for query performance

```python
# Data Lake organization
DATA_LAKE_STRUCTURE = {
    "raw": {
        "location": "s3://data-lake/raw/products/",
        "format": "parquet",
        "partitioning": ["year", "month", "day", "source"],
        "retention": "2 years"
    },
    "processed": {
        "location": "s3://data-lake/processed/products/",
        "format": "delta", # Delta Lake for ACID transactions
        "partitioning": ["category", "brand"],
        "retention": "5 years"
    },
    "curated": {
        "location": "s3://data-lake/curated/products/",
        "format": "delta",
        "partitioning": ["category"],
        "retention": "7 years"
    }
}
```

#### 3.2 High-Performance Serving Layer

**Technology Choices:**

- **Apache Druid** for real-time analytics
- **ClickHouse** for fast OLAP queries
- **Elasticsearch** for full-text search
- **Redis Cluster** for caching

```python
# Multi-tier serving architecture
class ProductServingLayer:
    def __init__(self):
        self.redis_cache = redis.Redis(host='redis-cluster')
        self.clickhouse = clickhouse_connect.get_client(
            host='clickhouse-cluster'
        )
        self.elasticsearch = Elasticsearch(['es-cluster:9200'])

    def get_product(self, product_id):
        """
        Multi-tier data retrieval
        """
        # Tier 1: Redis cache
        cached = self.redis_cache.get(f"product:{product_id}")
        if cached:
            return json.loads(cached)

        # Tier 2: ClickHouse for structured queries
        result = self.clickhouse.query(
            f"SELECT * FROM products WHERE id = {product_id}"
        )

        if result.result_rows:
            product = dict(result.result_rows[0])
            # Cache for 1 hour
            self.redis_cache.setex(
                f"product:{product_id}", 3600, json.dumps(product)
            )
            return product

        return None
```

#### 3.3 Monitoring and Observability

**Monitoring Stack:**

- **Prometheus** for metrics collection
- **Grafana** for visualization
- **Jaeger** for distributed tracing
- **ELK Stack** for log aggregation

```python
# Observability integration
import prometheus_client
from prometheus_client import Counter, Histogram, Gauge

class ProductPipelineMetrics:
    def __init__(self):
        self.processed_products = Counter(
            'products_processed_total',
            'Total products processed',
            ['source', 'status']
        )

        self.processing_time = Histogram(
            'product_processing_duration_seconds',
            'Time spent processing products',
            ['source']
        )

        self.duplicate_products = Counter(
            'duplicate_products_total',
            'Total duplicate products found',
            ['source', 'match_type']
        )

    def record_processing_time(self, source, duration):
        self.processing_time.labels(source=source).observe(duration)

    def increment_processed(self, source, status):
        self.processed_products.labels(source=source, status=status).inc()
```

### 4. Cost Optimization Strategies

#### 4.1 Infrastructure Cost Management

**Cloud Cost Optimization:**

- **Spot Instances** for batch processing (60-90% cost reduction)
- **Reserved Instances** for predictable workloads
- **Auto-scaling** based on demand
- **Data compression** (Parquet, Snappy) to reduce storage costs

```python
# Cost-optimized processing
class CostOptimizedProcessor:
    def __init__(self):
        self.spot_fleet_config = {
            'instance_types': ['m5.xlarge', 'm5.2xlarge', 'c5.2xlarge'],
            'target_capacity': 10,
            'allocation_strategy': 'spot'
        }

    def schedule_batch_processing(self, batch_size):
        """
        Schedule processing based on cost optimization
        """
        # Check spot pricing
        current_spot_price = self.get_spot_price()

        if current_spot_price < self.max_spot_price:
            # Use spot instances
            return self.launch_spot_cluster(batch_size)
        else:
            # Defer processing or use reserved instances
            return self.schedule_for_later(batch_size)
```

#### 4.2 Data Lifecycle Management

**Tiered Storage Strategy:**

- **Hot data** (last 30 days): SSD storage
- **Warm data** (30 days - 1 year): Standard storage
- **Cold data** (1+ years): Glacier/Archive storage

### 5. System Components and Implementation

#### 5.1 Core Components

```python
# Main orchestrator
class ProductDataOrchestrator:
    def __init__(self, config):
        self.config = config
        self.ingestion_manager = IngestionManager(config)
        self.processing_engine = ProcessingEngine(config)
        self.storage_manager = StorageManager(config)
        self.monitoring = MonitoringSystem(config)

    async def run_pipeline(self):
        """
        Main pipeline execution
        """
        try:
            # Step 1: Ingest from all sources
            raw_data = await self.ingestion_manager.ingest_all_sources()

            # Step 2: Process and deduplicate
            processed_data = await self.processing_engine.process_batch(raw_data)

            # Step 3: Store in data lake
            await self.storage_manager.store_processed_data(processed_data)

            # Step 4: Update serving layer
            await self.storage_manager.update_serving_layer(processed_data)

            # Step 5: Report metrics
            self.monitoring.report_pipeline_success()

        except Exception as e:
            self.monitoring.report_pipeline_failure(e)
            raise
```

### 6. Technology Stack Summary

**Data Ingestion:**

- Apache Kafka / Amazon Kinesis
- Apache Airflow
- AWS Lambda / Google Cloud Functions

**Processing:**

- Apache Spark (PySpark)
- Apache Flink (for streaming)
- Kubernetes for orchestration

**Storage:**

- Data Lake: S3/GCS + Delta Lake
- Serving: ClickHouse + Redis + Elasticsearch
- Metadata: Apache Hive Metastore

**Monitoring:**

- Prometheus + Grafana
- Jaeger for tracing
- ELK Stack for logs

**Cost Optimization:**

- Spot instances
- Data compression
- Tiered storage
- Auto-scaling

### 7. Performance Characteristics

**Expected Performance:**

- **Ingestion**: 100K+ records/second per source
- **Processing**: 1M+ records/minute with Spark cluster
- **Deduplication**: 500K+ comparisons/second
- **Serving**: Sub-100ms query response times
- **Total Pipeline**: 10M+ records processed in 5-10 minutes

**Scalability:**

- Horizontal scaling to 100+ nodes
- Linear performance scaling with cluster size
- Auto-scaling based on workload

### 8. Implementation Roadmap

**Phase 1 (Months 1-2):**

- Set up basic ingestion pipeline
- Implement core deduplication logic
- Deploy to staging environment

**Phase 2 (Months 3-4):**

- Add real-time processing capabilities
- Implement advanced ML-based deduplication
- Deploy monitoring and alerting

**Phase 3 (Months 5-6):**

- Performance optimization
- Cost optimization implementation
- Production deployment

**Phase 4 (Months 7+):**

- Advanced analytics features
- Machine learning integration
- Continuous improvement

This architecture provides a robust, scalable, and cost-effective solution for processing millions of product records from multiple sources while maintaining high availability and performance standards.
