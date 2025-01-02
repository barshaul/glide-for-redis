# Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

import asyncio
import sys
import threading
from typing import Dict, List, Optional

from glide import GlideClusterClient, GlideClusterClientConfiguration
from glide.async_commands.core import CoreCommands
from glide.config import BaseClientConfiguration
from glide.constants import TEncodable, TRequest

from glide.protobuf.connection_request_pb2 import ConnectionRequest
from glide.protobuf.response_pb2 import Response
from glide.glide_client import GlideClient
import asyncio


if sys.version_info >= (3, 11):
    import asyncio as async_timeout
    from typing import Self
else:
    import async_timeout
    from typing_extensions import Self


class SyncClient(CoreCommands):
    def __init__(self, config: BaseClientConfiguration):
        """
        To create a new client, use the `create` classmethod
        """
        self.config: BaseClientConfiguration = config
        self._available_futures: Dict[int, asyncio.Future] = {}
        self._available_callback_indexes: List[int] = list()
        self._buffered_requests: List[TRequest] = list()
        self._writer_lock = threading.Lock()
        self.socket_path: Optional[str] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._is_closed: bool = False
        self._pubsub_futures: List[asyncio.Future] = []
        self._pubsub_lock = threading.Lock()
        self._pending_push_notifications: List[Response] = list()

    @classmethod
    def create(cls, config: BaseClientConfiguration) -> Self:
        """Creates a Glide client.

        Args:
            config (ClientConfiguration): The client configurations.
                If no configuration is provided, a default client to "localhost":6379 will be created.

        Returns:
            Self: a Glide Client instance.
        """
        config = config
        self = cls(config)
        loop = asyncio.new_event_loop()
        self.loop = loop
        def start_event_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        # Start the event loop in a separate thread
        thread = threading.Thread(target=start_event_loop, daemon=True)
        thread.start()
        if type(config) == GlideClusterClientConfiguration:
            self.inner_client = asyncio.run(GlideClusterClient.create(config, loop))
        else:
            self.inner_client = asyncio.run(GlideClient.create(config, loop))
        return self
    
    def set(self, key: TEncodable, value: TEncodable):
        future = asyncio.run_coroutine_threadsafe(self.inner_client.set(key, value), self.loop)
        return future.result()
    
    def get(self, key: TEncodable):
        future = asyncio.run_coroutine_threadsafe(self.inner_client.get(key), self.loop)
        return future.result()
