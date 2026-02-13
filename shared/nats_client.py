# shared/nats_client.py
# import asyncio
import logging
from typing import List, Optional

import nats
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext
from nats.js.api import (
    StreamConfig,
    RetentionPolicy,
    StorageType,
    ConsumerConfig,
    AckPolicy,
    # StreamInfo
)
# from nats.errors import TimeoutError, NoRespondersError

# Configuration Constants for easier tuning
# shared/nats_client.py

# Optimized Constants (Python library expects SECONDS)
MAX_AGE_60S = 60.0             # 60 seconds
MAX_AGE_5S = 5.0               # 5 seconds
ACK_WAIT_10S = 10.0            # 10 seconds
DUPLICATE_WINDOW_30S = 30.0    # 30 seconds

# This stays the same because it's in bytes
MAX_MSG_SIZE_50MB = 50_000_000

logger = logging.getLogger(__name__)

class PentecostNATSClient:
    def __init__(self, servers: List[str] = ["nats://localhost:4222"]):
        self.servers = servers
        self.nc: Optional[NATS] = None
        self.js: Optional[JetStreamContext] = None

    async def connect(self):
        """Establishes connection to NATS and initializes JetStream context."""
        try:
            self.nc = await nats.connect(
                servers=self.servers,
                reconnect_time_wait=2,
                max_reconnect_attempts=10,
                error_cb=self._error_callback,
                closed_cb=self._closed_callback,
                reconnected_cb=self._reconnected_callback
            )
            self.js = self.nc.jetstream()
            logger.info(f"Connected to NATS servers: {self.servers}")
        except Exception as e:
            logger.error(f"Failed to connect to NATS: {e}")
            raise

    async def close(self):
        """Gracefully closes the NATS connection."""
        if self.nc:
            await self.nc.close()
            logger.info("NATS connection closed.")

    async def _error_callback(self, e):
        logger.error(f"NATS Error: {e}")

    async def _closed_callback(self):
        logger.warning("NATS Connection Closed")

    async def _reconnected_callback(self):
        logger.info("NATS Connection Re-established")

    async def setup_streams(self):
        """Initialize all required streams with idempotency (update if exists)."""
        if not self.js:
            raise RuntimeError("JetStream context not initialized. Call connect() first.")

        streams = [
            StreamConfig(
                name="LIVESTREAM_RAW",
                subjects=["livestream.video.raw", "livestream.audio.raw"],
                retention=RetentionPolicy.LIMITS,
                max_msgs=50_000,
                max_age=MAX_AGE_60S,
                max_msg_size=MAX_MSG_SIZE_50MB,
                storage=StorageType.MEMORY,
                num_replicas=1, # Use 3 for production clustering
                duplicate_window=DUPLICATE_WINDOW_30S,
            ),
            StreamConfig(
                name="LIVESTREAM_PROCESSED",
                subjects=["livestream.video.processed", "livestream.audio.processed"],
                retention=RetentionPolicy.LIMITS,
                max_msgs=50_000,
                max_age=MAX_AGE_60S,
                max_msg_size=MAX_MSG_SIZE_50MB,
                storage=StorageType.MEMORY,
                num_replicas=1,
            ),
            StreamConfig(
                name="SYNC_CONTROL",
                subjects=["sync.control", "sync.heartbeat", "sync.drift"],
                retention=RetentionPolicy.LIMITS,
                max_msgs=1_000,
                max_age=MAX_AGE_5S,
                storage=StorageType.MEMORY,
                num_replicas=1,
            )
        ]

        for config in streams:
            try:
                # Try to add stream; if it exists, update it to match new config
                await self.js.add_stream(config)
                logger.info(f"Stream '{config.name}' configured successfully.")
            except Exception as e:
                 logger.info(f"Stream '{config.name}' already exists, ensuring configuration: {e}")
                 await self.js.update_stream(config)

    async def create_consumers(self):
        """Create consumer groups for AI processing."""
        if not self.js:
            raise RuntimeError("JetStream context not initialized.")

        consumers = [
            (
                "LIVESTREAM_RAW",
                ConsumerConfig(
                    durable_name="video-processors",
                    deliver_group="video-ai",
                    filter_subject="livestream.video.raw",
                    ack_policy=AckPolicy.EXPLICIT,
                    max_deliver=3,
                    max_ack_pending=200,
                    ack_wait=ACK_WAIT_10S,
                )
            ),
            (
                "LIVESTREAM_RAW",
                ConsumerConfig(
                    durable_name="audio-processors",
                    deliver_group="audio-ai",
                    filter_subject="livestream.audio.raw",
                    ack_policy=AckPolicy.EXPLICIT,
                    max_deliver=3,
                    max_ack_pending=200,
                    ack_wait=ACK_WAIT_10S,
                )
            )
        ]

        for stream_name, config in consumers:
            try:
                await self.js.add_consumer(stream_name, config)
                logger.info(f"Consumer '{config.durable_name}' created on stream '{stream_name}'.")
            except Exception as e:
                logger.error(f"Failed to create consumer '{config.durable_name}': {e}")