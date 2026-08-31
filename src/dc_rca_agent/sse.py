import asyncio
import logging
from typing import Set

log = logging.getLogger(__name__)

class SSEManager:
    def __init__(self):
        self._listeners: Set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        queue = asyncio.Queue()
        self._listeners.add(queue)
        log.info(f"New client subscribed to SSE stream. Total clients: {len(self._listeners)}")
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        if queue in self._listeners:
            self._listeners.remove(queue)
            log.info(f"Client unsubscribed from SSE stream. Total clients: {len(self._listeners)}")

    async def broadcast(self, event: dict):
        if not self._listeners:
            return
        log.info(f"Broadcasting SSE event: {event} to {len(self._listeners)} clients")
        for queue in list(self._listeners):
            try:
                queue.put_nowait(event)
            except Exception as e:
                log.warning(f"Failed to push to client queue: {e}")

sse_manager = SSEManager()

def broadcast_sync(event: dict):
    """
    Synchronous wrapper to broadcast SSE events from threaded/synchronous workers.
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(sse_manager.broadcast(event))
    except RuntimeError:
        # If no running event loop in current thread, create temporary loop
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(sse_manager.broadcast(event))
            loop.close()
        except Exception as ex:
            log.warning(f"Failed to broadcast from new event loop: {ex}")
    except Exception as e:
        log.warning(f"Failed to sync-broadcast SSE: {e}")

