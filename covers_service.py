from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from datetime import date, datetime
from typing import Any, Callable, Dict, Optional

from models import AbsenceRecord

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COVERS_FILE = os.path.join(BASE_DIR, "covers.json")
COVERS_FORWARD_URL = os.getenv("COVERS_FORWARD_URL")
COVERS_FORWARD_SECRET = os.getenv("COVERS_FORWARD_SECRET")
COVERS_FORWARD_SECRET_HEADER = os.getenv("COVERS_FORWARD_SECRET_HEADER", "X-Leave-Webhook-Secret")
ABSENCES_REQUEST_URL = os.getenv("ABSENCES_REQUEST_URL")
ABSENCES_REQUEST_SECRET = os.getenv("ABSENCES_REQUEST_SECRET")
ABSENCES_REQUEST_SECRET_HEADER = os.getenv(
    "ABSENCES_REQUEST_SECRET_HEADER",
    "X-Absences-Request-Secret",
)

logger = logging.getLogger(__name__)


class CoversManager:
    def __init__(
        self,
        storage_path: Optional[str] = None,
        session_factory: Callable | None = None,
    ):
        self.storage_path = storage_path or COVERS_FILE
        self._session_factory = session_factory
        self.records: dict[str, list[dict[str, Any]]] = (
            self._load_records() if not self._session_factory else {}
        )
        if self._session_factory:
            with self._session_factory() as session:
                has_records = session.query(AbsenceRecord.id).first()
            if not has_records and os.path.exists(self.storage_path):
                self._import_json_records()

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

    def clear_records(self) -> None:
        if self._session_factory:
            with self._session_factory() as session:
                session.query(AbsenceRecord).delete()
                session.commit()
            return
        self.records = {}
        try:
            self._save_records()
        except OSError:
            logger.exception("Failed to clear cover records")

    def record_leave(self, payload: Dict[str, Any]) -> dict[str, Any]:
        if self._session_factory:
            return self._record_leave_db(payload)
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

    def _record_leave_db(self, payload: Dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_payload(payload)
        with self._session_factory() as session:
            existing = (
                session.query(AbsenceRecord)
                .filter(AbsenceRecord.request_id == normalized["request_id"])
                .one_or_none()
            )
            if existing:
                normalized.setdefault(
                    "forwarded_at",
                    existing.forwarded_at.isoformat() if existing.forwarded_at else None,
                )
                normalized.setdefault("forward_status", existing.forward_status)
                normalized.setdefault("forward_response", existing.forward_response)
            if self._should_forward(normalized):
                forward_result = self._forward_leave_entry(normalized)
                normalized["forwarded_at"] = forward_result["timestamp"]
                normalized["forward_status"] = forward_result["status"]
                normalized["forward_response"] = forward_result["detail"]
            else:
                normalized.setdefault(
                    "forward_status", existing.forward_status if existing else "pending"
                )
            if not normalized.get("forwarded_at"):
                normalized.setdefault("forwarded_at", None)
            record = existing or AbsenceRecord(request_id=normalized["request_id"])
            record.teacher = normalized["teacher"]
            record.teacher_email = normalized.get("teacher_email") or ""
            record.teacher_slug = normalized.get("teacher_slug")
            record.leave_type = normalized.get("leave_type")
            record.leave_start = date.fromisoformat(normalized["leave_start"])
            record.leave_end = date.fromisoformat(normalized["leave_end"])
            record.status = normalized.get("status")
            record.reason = normalized.get("reason")
            record.submitted_at = self._parse_datetime(normalized.get("submitted_at"))
            record.recorded_at = self._parse_datetime(normalized.get("recorded_at"))
            record.subject = normalized.get("subject")
            record.level_label = normalized.get("level_label")
            record.payload = json.dumps(payload)
            record.forwarded_at = self._parse_datetime(normalized.get("forwarded_at"))
            record.forward_status = normalized.get("forward_status")
            record.forward_response = normalized.get("forward_response")
            session.add(record)
            session.commit()
        return normalized

    def _import_json_records(self) -> None:
        if not os.path.exists(self.storage_path):
            return
        try:
            with open(self.storage_path, encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError):
            return
        if not isinstance(data, dict):
            return
        records = []
        for _, entries in data.items():
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                request_id = entry.get("request_id")
                teacher = entry.get("teacher")
                teacher_email = entry.get("teacher_email")
                leave_start = entry.get("leave_start")
                leave_end = entry.get("leave_end") or leave_start
                if not (request_id and teacher and teacher_email and leave_start and leave_end):
                    continue
                parsed_start = self._parse_date(leave_start)
                parsed_end = self._parse_date(leave_end)
                if not parsed_start or not parsed_end:
                    continue
                record = AbsenceRecord(
                    request_id=str(request_id),
                    teacher=str(teacher),
                    teacher_email=str(teacher_email),
                    teacher_slug=entry.get("teacher_slug"),
                    leave_type=entry.get("leave_type"),
                    leave_start=parsed_start,
                    leave_end=parsed_end,
                    status=entry.get("status"),
                    reason=entry.get("reason"),
                    submitted_at=self._parse_datetime(entry.get("submitted_at")),
                    recorded_at=self._parse_datetime(entry.get("recorded_at")),
                    subject=entry.get("subject"),
                    level_label=entry.get("level_label"),
                    payload=json.dumps(entry.get("payload") or entry),
                    forwarded_at=self._parse_datetime(entry.get("forwarded_at")),
                    forward_status=entry.get("forward_status"),
                    forward_response=entry.get("forward_response"),
                )
                records.append(record)
        if not records:
            return
        with self._session_factory() as session:
            session.bulk_save_objects(records)
            session.commit()

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
        for key in ("teacher_slug", "subject", "level_label"):
            if payload.get(key) is not None:
                entry[key] = payload[key]
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
        if self._session_factory:
            return self._get_absences_for_date_db(normalized_date)
        return self.records.get(normalized_date, [])

    def _get_absences_for_date_db(self, normalized_date: str) -> list[dict[str, Any]]:
        try:
            target_date = date.fromisoformat(normalized_date)
        except ValueError:
            target_date = datetime.utcnow().date()
        with self._session_factory() as session:
            records = (
                session.query(AbsenceRecord)
                .filter(AbsenceRecord.leave_start == target_date)
                .all()
            )
        return [self._record_to_dict(record) for record in records]

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
        if not self._session_factory:
            return self.records.copy()
        with self._session_factory() as session:
            records = session.query(AbsenceRecord).all()
        grouped: dict[str, list[dict[str, Any]]] = {}
        for record in records:
            key = record.leave_start.isoformat()
            grouped.setdefault(key, []).append(self._record_to_dict(record))
        return grouped

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    @staticmethod
    def _record_to_dict(record: AbsenceRecord) -> dict[str, Any]:
        return {
            "request_id": record.request_id,
            "teacher": record.teacher,
            "teacher_email": record.teacher_email,
            "teacher_slug": record.teacher_slug,
            "leave_type": record.leave_type,
            "leave_start": record.leave_start.isoformat() if record.leave_start else None,
            "leave_end": record.leave_end.isoformat() if record.leave_end else None,
            "status": record.status,
            "reason": record.reason,
            "submitted_at": record.submitted_at.isoformat() if record.submitted_at else None,
            "recorded_at": record.recorded_at.isoformat() if record.recorded_at else None,
            "subject": record.subject,
            "level_label": record.level_label,
            "payload": record.payload,
            "forwarded_at": record.forwarded_at.isoformat() if record.forwarded_at else None,
            "forward_status": record.forward_status,
            "forward_response": record.forward_response,
        }

    def can_request_absences(self) -> bool:
        return bool(ABSENCES_REQUEST_URL)

    def request_absences_webhook(self, payload: Optional[Dict[str, Any]] = None) -> dict[str, Any]:
        if not ABSENCES_REQUEST_URL:
            return {"status": "disabled", "detail": "ABSENCES_REQUEST_URL not configured"}
        request_payload = payload or {"requested_at": datetime.utcnow().isoformat()}
        headers = {"Content-Type": "application/json"}
        if ABSENCES_REQUEST_SECRET:
            headers[ABSENCES_REQUEST_SECRET_HEADER] = ABSENCES_REQUEST_SECRET
        request_data = json.dumps(request_payload).encode("utf-8")
        req = urllib.request.Request(ABSENCES_REQUEST_URL, data=request_data, headers=headers, method="POST")
        timestamp = datetime.utcnow().isoformat()
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                body = response.read().decode("utf-8", errors="ignore")
                status_code = response.getcode()
                records = self._parse_absence_response(body)
                return {
                    "status": "sent",
                    "detail": str(status_code),
                    "timestamp": timestamp,
                    "records": records,
                }
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="ignore")
            detail = f"HTTP {exc.code}: {body}"
            logger.warning("Absence request failed with HTTP error %s: %s", exc.code, body)
        except urllib.error.URLError as exc:
            detail = f"URL error: {exc}"
            logger.warning("Absence request failed with URL error: %s", exc)
        except Exception as exc:  # pragma: no cover
            detail = f"Unknown error: {exc}"
            logger.exception("Unexpected error while requesting absences: %s", exc)
        return {"status": "failed", "detail": detail, "timestamp": timestamp}

    @staticmethod
    def _parse_absence_response(body: str) -> list[dict[str, Any]]:
        if not body:
            return []
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return []
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            for key in ("records", "absences", "data"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []
