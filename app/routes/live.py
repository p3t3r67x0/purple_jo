import asyncio
import contextlib
import json
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from app.services import perform_live_scan


router = APIRouter(prefix="/live", tags=["live"])


@router.websocket("/scan/{domain}")
async def stream_live_scan(websocket: WebSocket, domain: str) -> None:
    """Stream live scan progress events to the frontend via WebSocket."""

    await websocket.accept()

    queue: asyncio.Queue[Optional[Dict[str, Any]]] = asyncio.Queue()

    async def reporter(event: Dict[str, Any]) -> None:
        await queue.put(event)

    async def run_scan() -> None:
        try:
            await perform_live_scan(websocket.app.state.mongo, domain, reporter=reporter)
        except HTTPException as exc:
            await queue.put(
                {
                    "type": "error",
                    "status": exc.status_code,
                    "detail": exc.detail,
                }
            )
        except Exception as exc:  # pragma: no cover - defensive
            await queue.put(
                {
                    "type": "error",
                    "status": 500,
                    "detail": str(exc),
                }
            )
        finally:
            await queue.put(None)

    task = asyncio.create_task(run_scan())

    try:
        while True:
            event = await queue.get()
            if event is None:
                break
            payload = json.dumps(event, default=str)
            await websocket.send_text(payload)
    except WebSocketDisconnect:
        pass
    finally:
        if not task.done():
            task.cancel()
            with contextlib.suppress(Exception):
                await task
        if websocket.application_state != WebSocketState.DISCONNECTED:
            with contextlib.suppress(RuntimeError):
                await websocket.close()
