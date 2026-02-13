# tests/test_nats.py
import os
import sys

# Forces the root of the project into the path regardless of where the test is run
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest
import pytest_asyncio
from shared.nats_client import PentecostNATSClient

# Define a fixture for the NATS client to handle setup and teardown automatically
@pytest_asyncio.fixture
async def nats_client():
    client = PentecostNATSClient()
    await client.connect()
    yield client
    # Teardown: cleanup streams to leave a clean state for next test
    if client.js:
        try:
            await client.js.delete_stream("LIVESTREAM_RAW")
            await client.js.delete_stream("LIVESTREAM_PROCESSED")
            await client.js.delete_stream("SYNC_CONTROL")
        except Exception:
            pass # Ignore errors if streams didn't exist
    await client.close()

@pytest.mark.asyncio
async def test_nats_connection(nats_client):
    """Verify that the client can establish a connection."""
    assert nats_client.nc.is_connected
    assert nats_client.js is not None

@pytest.mark.asyncio
async def test_stream_creation(nats_client):
    """Verify that all required streams are created with correct config."""
    await nats_client.setup_streams()
    
    # Verify streams exist
    streams = await nats_client.js.streams_info()
    stream_names = [s.config.name for s in streams]
    
    assert "LIVESTREAM_RAW" in stream_names
    assert "LIVESTREAM_PROCESSED" in stream_names
    assert "SYNC_CONTROL" in stream_names

@pytest.mark.asyncio
async def test_consumer_creation(nats_client):
    """Verify that consumers are created correctly."""
    await nats_client.setup_streams()
    await nats_client.create_consumers()
    
    # Check for video processor consumer properties
    video_consumer = await nats_client.js.consumer_info("LIVESTREAM_RAW", "video-processors")
    assert video_consumer.config.durable_name == "video-processors"
    assert video_consumer.config.deliver_group == "video-ai"

    # Check for audio processor consumer properties
    audio_consumer = await nats_client.js.consumer_info("LIVESTREAM_RAW", "audio-processors")
    assert audio_consumer.config.durable_name == "audio-processors"
    assert audio_consumer.config.deliver_group == "audio-ai"