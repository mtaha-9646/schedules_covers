from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import datetime
from typing import Any, Dict, Optional

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COVERS_FILE = os.path.join(BASE_DIR, "covers.json")
COVERS_FORWARD_URL = os.getenv("COVERS_FORWARD_URL")
COVERS_FORWARD_SECRET = os.getenv("COVERS_FORWARD_SECRET")
COVERS_FORWARD_SECRET_HEADER = os.getenv("COVERS_FORWARD_SECRET_HEADER", "X-Leave-Webhook-Secret")

logger = logging.getLogger(__name__)


class CoversManager:
    def __init__(self, storage_path: Optional[str] = None):
        self.storage_path = storage_path or COVERS_FILE
        self.records: dict[str, list[dict[str, Any]]] = self._load_records()

    def _load_records(self) -> dict[str, list[dict[str, Any]]]:
        if not os.path.exists(self.storage_path):
            return {}
        try:
            with open(self.storage_path, encoding="utf-8") as handle:
                data = json.load(handle)
            if isinstance(data, dict):
                return data
        except (OSError, json.JSONDecodeError):
            pass
        return {}

    def _save_records(self) -> None:
        directory = os.path.dirname(self.storage_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
        with open(self.storage_path, "w", encoding="utf-8") as handle:
            json.dump(self.records, handle, indent=2)

    def record_leave(self, payload: Dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_payload(payload)
        date_key = normalized["leave_start"]
        day_records = [
            entry for entry in self.records.get(date_key, []) if entry["request_id"] != normalized["request_id"]
        ]
        existing_entry = next(
            (entry for entry in self.records.get(date_key, []) if entry["request_id"] == normalized["request_id"]),
            None,
        )
        if existing_entry:
            normalized.setdefault("forwarded_at", existing_entry.get("forwarded_at"))
            normalized.setdefault("forward_status", existing_entry.get("forward_status"))
            normalized.setdefault("forward_response", existing_entry.get("forward_response"))
        if self._should_forward(normalized):
            forward_result = self._forward_leave_entry(normalized)
            normalized["forwarded_at"] = forward_result["timestamp"]
            normalized["forward_status"] = forward_result["status"]
            normalized["forward_response"] = forward_result["detail"]
        else:
            normalized.setdefault("forward_status", existing_entry.get("forward_status") if existing_entry else "pending")
        if not normalized.get("forwarded_at"):
            normalized.setdefault("forwarded_at", None)
        day_records.append(normalized)
        self.records[date_key] = day_records
        self._save_records()
        return normalized

    def _normalize_payload(self, payload: Dict[str, Any]) -> dict[str, Any]:
        if "request_id" not in payload or "teacher" not in payload:
            raise ValueError("payload missing required request_id or teacher")
        if "leave_start" not in payload and "leave_date" not in payload:
            raise ValueError("payload missing leave_start date")
        if "leave_end" not in payload:
            raise ValueError("payload missing leave_end date")
        leave_start, leave_end, submitted_at = self._normalize_payload_dates(payload)
        entry = {
            "request_id": str(payload.get("request_id")),
            "teacher": str(payload.get("teacher")),
            "teacher_email": payload.get("email") or payload.get("teacher_email"),
            "leave_type": payload.get("leave_type"),
            "leave_start": leave_start,
            "leave_end": leave_end,
            "status": payload.get("status"),
            "reason": payload.get("reason"),
            "submitted_at": submitted_at,
            "recorded_at": datetime.utcnow().isoformat(),
            "payload": payload,
        }
        return entry

    def _normalize_date(self, raw_date: Optional[str | datetime]) -> str:
        if isinstance(raw_date, datetime):
            return raw_date.date().isoformat()
        if raw_date:
            raw = str(raw_date).strip()
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
                try:
                    return datetime.strptime(raw, fmt).date().isoformat()
                except ValueError:
                    continue
            try:
                return datetime.fromisoformat(raw).date().isoformat()
            except ValueError:
                pass
        return datetime.utcnow().date().isoformat()

    def get_absences_for_date(self, date_key: Optional[str] = None) -> list[dict[str, Any]]:
        normalized_date = self._normalize_date(date_key) if date_key else datetime.utcnow().date().isoformat()
        return self.records.get(normalized_date, [])

    def _normalize_payload_dates(self, payload: Dict[str, Any]) -> tuple[str, str, str]:
        leave_start = self._normalize_date(payload.get("leave_start") or payload.get("leave_date"))
        leave_end = self._normalize_date(payload.get("leave_end") or leave_start)
        submitted_at = payload.get("submitted_at")
        if not submitted_at:
            submitted_at = datetime.utcnow().isoformat()
        else:
            submitted_at = self._normalize_datetime(submitted_at)
        return leave_start, leave_end, submitted_at

    def _normalize_datetime(self, raw: Any) -> str:
        if isinstance(raw, datetime):
            return raw.isoformat()
        raw_str = str(raw).strip()
        try:
            return datetime.fromisoformat(raw_str).isoformat()
        except ValueError:
            try:
                return datetime.strptime(raw_str, "%Y-%m-%dT%H:%M:%S").isoformat()
            except ValueError:
                return datetime.utcnow().isoformat()

    def _should_forward(self, entry: dict[str, Any]) -> bool:
        if not COVERS_FORWARD_URL:
            return False
        status = entry.get("status")
        if not status:
            return False
        if status.strip().lower() != "approved":
            return False
        return entry.get("forward_status") != "sent"

    def _forward_leave_entry(self, entry: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "request_id": entry["request_id"],
            "teacher": entry["teacher"],
            "teacher_email": entry.get("teacher_email"),
            "leave_type": entry.get("leave_type"),
            "leave_start": entry["leave_start"],
            "leave_end": entry["leave_end"],
            "status": entry.get("status"),
            "reason": entry.get("reason"),
            "submitted_at": entry.get("submitted_at"),
            "notified_at": datetime.utcnow().isoformat(),
        }
        headers = {"Content-Type": "application/json"}
        if COVERS_FORWARD_SECRET:
            headers[COVERS_FORWARD_SECRET_HEADER] = COVERS_FORWARD_SECRET
        request_data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(COVERS_FORWARD_URL, data=request_data, headers=headers, method="POST")
        timestamp = datetime.utcnow().isoformat()
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                body = response.read().decode("utf-8", errors="ignore")
                status_code = response.getcode()
                detail = f"{status_code} {body}"
                return {"status": "sent", "detail": detail, "timestamp": timestamp}
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            detail = f"HTTP {exc.code}: {body}"
            logger.warning("Forwarding leave entry failed with HTTP error %s: %s", exc.code, body)
        except urllib.error.URLError as exc:
            detail = f"URL error: {exc}"
            logger.warning("Forwarding leave entry failed with URL error: %s", exc)
        except Exception as exc:  # pragma: no cover
            detail = f"Unknown error: {exc}"
            logger.exception("Unexpected error while forwarding leave entry: %s", exc)
        return {"status": "failed", "detail": detail, "timestamp": timestamp}

    def get_all_records(self) -> dict[str, list[dict[str, Any]]]:
        return self.records.copy()
