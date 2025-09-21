# streaming/stream_events.py
"""
Real-time event streaming pipeline for GA4 attribution demo
Simulates streaming GA4 events into BigQuery with deduplication and idempotency
"""

import json
import time
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
from dataclasses import dataclass
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class StreamingConfig:
    """Configuration for streaming pipeline"""
    project_id: str = "ga4-attribution-demo"
    dataset_id: str = "dbt_hp"
    table_id: str = "streaming_events"
    batch_size: int = 10
    stream_interval: int = 30  # seconds
    dedup_window_hours: int = 2

class GA4EventGenerator:
    """Generates realistic GA4 events for streaming demo"""
    
    def __init__(self):
        self.traffic_sources = [
            {"source": "google", "medium": "organic", "campaign": None},
            {"source": "google", "medium": "cpc", "campaign": "summer_sale_2024"},
            {"source": "facebook", "medium": "social", "campaign": "brand_awareness"},
            {"source": "email", "medium": "email", "campaign": "newsletter_weekly"},
            {"source": "(direct)", "medium": "(none)", "campaign": None},
            {"source": "bing", "medium": "cpc", "campaign": "competitor_keywords"},
        ]
        
        self.event_types = [
            {"name": "page_view", "weight": 0.6},
            {"name": "scroll", "weight": 0.2}, 
            {"name": "click", "weight": 0.1},
            {"name": "add_to_cart", "weight": 0.07},
            {"name": "purchase", "weight": 0.03}
        ]
        
        self.user_pool = [f"user_{str(uuid.uuid4())[:8]}" for _ in range(50)]
        
    def generate_event(self) -> Dict[str, Any]:
        """Generate a single GA4 event"""
        now = datetime.utcnow()
        
        # Select event type based on weights
        event_weights = [e["weight"] for e in self.event_types]
        event_type = random.choices(self.event_types, weights=event_weights, k=1)[0]
        
        # Select traffic source
        traffic_source = random.choice(self.traffic_sources)
        
        # Select user (some users more likely to return)
        user_id = random.choices(
            self.user_pool,
            weights=[2 if i < 10 else 1 for i in range(len(self.user_pool))],
            k=1
        )[0]
        
        # Base event structure
        event = {
            "event_date": now.strftime("%Y%m%d"),
            "event_timestamp": int(now.timestamp() * 1000000),  # microseconds
            "event_name": event_type["name"],
            "user_pseudo_id": user_id,
            "session_id": f"{user_id}_{int(now.timestamp() // 1800)}",  # 30-min sessions
            
            # Traffic source
            "traffic_source": traffic_source["source"],
            "traffic_medium": traffic_source["medium"],
            "traffic_campaign": traffic_source["campaign"],
            
            # Device info
            "device_category": random.choice(["mobile", "desktop", "tablet"]),
            "operating_system": random.choice(["Android", "iOS", "Windows", "macOS"]),
            "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            
            # Geo info
            "country": random.choice(["US", "CA", "GB", "DE", "FR"]),
            "city": random.choice(["New York", "London", "Toronto", "Berlin", "Paris"]),
            
            # Page info
            "page_location": f"https://example.com/{random.choice(['home', 'products', 'about', 'contact'])}",
            "page_title": f"Page {random.randint(1, 100)}",
            
            # Custom event ID for deduplication
            "event_id": str(uuid.uuid4()),
            
            # Metadata
            "_stream_inserted_at": now.isoformat(),
            "_batch_id": str(uuid.uuid4())[:8]
        }
        
        # Add ecommerce data for conversion events
        if event_type["name"] == "purchase":
            event.update({
                "purchase_revenue_in_usd": round(random.uniform(10, 500), 2),
                "transaction_id": f"txn_{str(uuid.uuid4())[:8]}",
                "total_item_quantity": random.randint(1, 5),
                "items": [
                    {
                        "item_id": f"item_{random.randint(1, 100)}",
                        "item_name": f"Product {random.randint(1, 100)}",
                        "item_category": random.choice(["Electronics", "Clothing", "Books"]),
                        "price": round(random.uniform(10, 200), 2),
                        "quantity": random.randint(1, 3)
                    }
                ]
            })
        elif event_type["name"] == "add_to_cart":
            event.update({
                "items": [
                    {
                        "item_id": f"item_{random.randint(1, 100)}",
                        "item_name": f"Product {random.randint(1, 100)}",
                        "price": round(random.uniform(10, 200), 2),
                        "quantity": 1
                    }
                ]
            })
            
        return event

class BigQueryStreamer:
    """Handles BigQuery streaming with deduplication"""
    
    def __init__(self, config: StreamingConfig):
        self.config = config
        self.client = bigquery.Client(project=config.project_id)
        self.table_ref = self.client.dataset(config.dataset_id).table(config.table_id)
        
        # Ensure table exists
        self._create_table_if_not_exists()
    
    def _create_table_if_not_exists(self):
        """Create streaming events table if it doesn't exist"""
        try:
            self.client.get_table(self.table_ref)
            logger.info(f"Table {self.table_ref} already exists")
        except Exception:
            # Define table schema matching GA4 structure
            schema = [
                bigquery.SchemaField("event_date", "STRING"),
                bigquery.SchemaField("event_timestamp", "INTEGER"),
                bigquery.SchemaField("event_name", "STRING"),
                bigquery.SchemaField("event_id", "STRING"),
                bigquery.SchemaField("user_pseudo_id", "STRING"),
                bigquery.SchemaField("session_id", "STRING"),
                bigquery.SchemaField("traffic_source", "STRING"),
                bigquery.SchemaField("traffic_medium", "STRING"),
                bigquery.SchemaField("traffic_campaign", "STRING"),
                bigquery.SchemaField("device_category", "STRING"),
                bigquery.SchemaField("operating_system", "STRING"),
                bigquery.SchemaField("browser", "STRING"),
                bigquery.SchemaField("country", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("page_location", "STRING"),
                bigquery.SchemaField("page_title", "STRING"),
                bigquery.SchemaField("purchase_revenue_in_usd", "FLOAT"),
                bigquery.SchemaField("transaction_id", "STRING"),
                bigquery.SchemaField("total_item_quantity", "INTEGER"),
                bigquery.SchemaField("items", "STRING"),  # JSON string
                bigquery.SchemaField("_stream_inserted_at", "TIMESTAMP"),
                bigquery.SchemaField("_batch_id", "STRING"),
            ]
            
            table = bigquery.Table(self.table_ref, schema=schema)
            
            # Partition by date for performance
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="_stream_inserted_at"
            )
            
            # Cluster for deduplication queries
            table.clustering_fields = ["event_id", "user_pseudo_id"]
            
            table = self.client.create_table(table)
            logger.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
    
    def _deduplicate_events(self, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove events that already exist in BigQuery within dedup window"""
        if not events:
            return events
            
        # Extract event IDs
        event_ids = [event["event_id"] for event in events]
        
        # Query existing events within dedup window
        dedup_query = f"""
        SELECT event_id
        FROM `{self.config.project_id}.{self.config.dataset_id}.{self.config.table_id}`
        WHERE event_id IN UNNEST(@event_ids)
        AND _stream_inserted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.config.dedup_window_hours} HOUR)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("event_ids", "STRING", event_ids)
            ]
        )
        
        try:
            existing_ids = set()
            query_job = self.client.query(dedup_query, job_config=job_config)
            for row in query_job:
                existing_ids.add(row.event_id)
            
            # Filter out existing events
            new_events = [event for event in events if event["event_id"] not in existing_ids]
            
            logger.info(f"Deduplication: {len(events)} incoming, {len(existing_ids)} duplicates, {len(new_events)} new")
            return new_events
            
        except GoogleCloudError as e:
            logger.error(f"Deduplication query failed: {e}")
            # Return original events if dedup fails
            return events
    
    def stream_events(self, events: List[Dict[str, Any]]) -> bool:
        """Stream events to BigQuery with deduplication"""
        if not events:
            return True
            
        # Deduplicate events
        unique_events = self._deduplicate_events(events)
        
        if not unique_events:
            logger.info("No new events to insert after deduplication")
            return True
        
        # Convert items to JSON string for BigQuery
        for event in unique_events:
            if "items" in event:
                event["items"] = json.dumps(event["items"])
            
            # Convert timestamp to proper format
            event["_stream_inserted_at"] = datetime.utcnow().isoformat()
        
        # Stream to BigQuery
        try:
            errors = self.client.insert_rows_json(self.table_ref, unique_events)
            
            if errors:
                logger.error(f"Failed to insert rows: {errors}")
                return False
            else:
                logger.info(f"Successfully streamed {len(unique_events)} events")
                return True
                
        except GoogleCloudError as e:
            logger.error(f"BigQuery streaming error: {e}")
            return False

class StreamingPipeline:
    """Main streaming pipeline orchestrator"""
    
    def __init__(self, config: StreamingConfig):
        self.config = config
        self.event_generator = GA4EventGenerator()
        self.streamer = BigQueryStreamer(config)
        self.running = False
    
    def start_streaming(self, duration_minutes: int = 30):
        """Start streaming events for specified duration"""
        logger.info(f"Starting streaming pipeline for {duration_minutes} minutes")
        
        self.running = True
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(minutes=duration_minutes)
        
        events_streamed = 0
        
        try:
            while self.running and datetime.utcnow() < end_time:
                # Generate batch of events
                events = []
                for _ in range(self.config.batch_size):
                    event = self.event_generator.generate_event()
                    events.append(event)
                
                # Stream events
                success = self.streamer.stream_events(events)
                
                if success:
                    events_streamed += len(events)
                    logger.info(f"Total events streamed: {events_streamed}")
                else:
                    logger.error("Failed to stream batch")
                
                # Wait before next batch
                if self.running:
                    time.sleep(self.config.stream_interval)
                    
        except KeyboardInterrupt:
            logger.info("Streaming interrupted by user")
        finally:
            self.running = False
            logger.info(f"Streaming completed. Total events: {events_streamed}")
    
    def stop_streaming(self):
        """Stop the streaming pipeline"""
        self.running = False
        logger.info("Stopping streaming pipeline")

def main():
    """Main execution function"""
    # Configuration
    config = StreamingConfig(
        project_id = "ga4-attribution-demo",
        dataset_id = "dbt_hp",
        table_id = "streaming_events",
        batch_size=5,  # Smaller batches for demo
        stream_interval=10,  # 10 seconds between batches
        dedup_window_hours=2
    )
    
    # Create pipeline
    pipeline = StreamingPipeline(config)
    
    try:
        # Stream for 10 minutes (adjust as needed)
        pipeline.start_streaming(duration_minutes=10)
    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        pipeline.stop_streaming()

if __name__ == "__main__":
    main()


