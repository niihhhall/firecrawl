from typing import Any, Dict, List
from ...types import Document, MonitorJob, MonitorRequest, MonitorResponse
from ...utils.error_handler import handle_response_error
from ...utils.validation import prepare_scrape_options, validate_scrape_options
from ...utils.http_client_async import AsyncHttpClient
from ...utils.normalize import normalize_document_input


def _normalize_monitor_webhook_events(webhook: Dict[str, Any]) -> None:
    events = webhook.get("events")
    if not isinstance(events, list):
        return

    normalized: List[str] = []
    for event in events:
        if isinstance(event, str) and event.startswith("monitor."):
            normalized.append(event.split(".", 1)[1])
        else:
            normalized.append(event)
    webhook["events"] = normalized


def _has_change_tracking(scrape_options: Any) -> bool:
    formats = getattr(scrape_options, "formats", None)

    def has_change_tracking_item(item: Any) -> bool:
        if isinstance(item, str):
            return item in ("changeTracking", "change_tracking")
        if isinstance(item, dict):
            return item.get("type") in ("changeTracking", "change_tracking")
        item_type = getattr(item, "type", None)
        return item_type in ("changeTracking", "change_tracking")

    if isinstance(formats, list):
        return any(has_change_tracking_item(item) for item in formats)

    if formats is None:
        return False

    if getattr(formats, "change_tracking", False):
        return True

    nested = getattr(formats, "formats", None)
    if isinstance(nested, list):
        return any(has_change_tracking_item(item) for item in nested)

    return False


def _prepare_monitor_request(request: MonitorRequest) -> Dict[str, Any]:
    if not request.urls:
        raise ValueError("urls must be a non-empty list")

    validate_scrape_options(request.scrape_options)
    if not _has_change_tracking(request.scrape_options):
        raise ValueError("scrape_options.formats must include changeTracking")

    payload: Dict[str, Any] = {
        "urls": request.urls,
        "scrapeOptions": prepare_scrape_options(request.scrape_options),
    }

    if request.interval is not None:
        payload["interval"] = request.interval
    if request.origin is not None:
        payload["origin"] = request.origin
    if request.integration is not None:
        payload["integration"] = request.integration
    if request.webhook is not None:
        webhook_payload = request.webhook.model_dump(exclude_none=True)
        _normalize_monitor_webhook_events(webhook_payload)
        payload["webhook"] = webhook_payload

    return payload


async def start_monitor(
    client: AsyncHttpClient, request: MonitorRequest
) -> MonitorResponse:
    payload = _prepare_monitor_request(request)
    response = await client.post("/v2/monitor", payload)
    if response.status_code >= 400:
        handle_response_error(response, "start monitor")

    body = response.json()
    if not body.get("success"):
        raise Exception(body.get("error", "Unknown error occurred"))

    return MonitorResponse(id=body.get("id"), url=body.get("url"))


async def get_monitor_status(client: AsyncHttpClient, job_id: str) -> MonitorJob:
    response = await client.get(f"/v2/monitor/{job_id}")
    if response.status_code >= 400:
        handle_response_error(response, "get monitor status")

    body = response.json()
    if not body.get("success"):
        raise Exception(body.get("error", "Unknown error occurred"))

    latest_data = []
    for group in body.get("latestData", []) or []:
        pages = []
        for page in group.get("pages", []) or []:
            if isinstance(page, dict):
                pages.append(Document(**normalize_document_input(page)))
        latest_data.append(
            {
                "source": group.get("source"),
                "pages": pages,
            }
        )

    normalized = {
        "id": body.get("id"),
        "status": body.get("status"),
        "urls": body.get("urls", []),
        "resolved_urls": body.get("resolvedUrls", []),
        "interval": body.get("interval"),
        "interval_ms": body.get("intervalMs"),
        "created_at": body.get("createdAt"),
        "updated_at": body.get("updatedAt"),
        "next_run_at": body.get("nextRunAt"),
        "last_run_at": body.get("lastRunAt"),
        "latest_data": latest_data,
        "latest_data_at": body.get("latestDataAt"),
        "last_error": body.get("lastError"),
    }

    return MonitorJob(**normalized)


async def cancel_monitor(client: AsyncHttpClient, job_id: str) -> bool:
    response = await client.delete(f"/v2/monitor/{job_id}")
    if response.status_code >= 400:
        handle_response_error(response, "cancel monitor")

    body = response.json()
    return body.get("status") == "cancelled"
