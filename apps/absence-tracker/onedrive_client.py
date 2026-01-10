import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import requests

GRAPH_BASE = "https://graph.microsoft.com/v1.0"
SMALL_FILE_LIMIT = 4 * 1024 * 1024  # 4 MiB


class GraphAPIError(RuntimeError):
    """Raised when a Microsoft Graph request fails."""


@dataclass
class DriveItem:
    id: str
    name: str
    web_url: Optional[str] = None

    @classmethod
    def from_json(cls, payload: dict) -> "DriveItem":
        return cls(
            id=payload.get("id", ""),
            name=payload.get("name", ""),
            web_url=payload.get("webUrl"),
        )


class OneDriveClient:
    """Minimal Graph client that uploads files using an existing access token."""

    def __init__(self, access_token: str):
        self.access_token = access_token
        self._root_item: Optional[DriveItem] = None

    # ---------- Private helpers ----------
    def _headers(self, extra: Optional[dict] = None) -> dict:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if extra:
            headers.update(extra)
        return headers

    def _request(self, method: str, url: str, expected=(200,), **kwargs) -> requests.Response:
        response = requests.request(
            method,
            url,
            headers=self._headers(kwargs.pop("headers", None)),
            **kwargs,
        )
        if response.status_code not in expected:
            try:
                detail = response.json()
            except ValueError:
                detail = response.text
            raise GraphAPIError(f"{method} {url} -> {response.status_code}: {detail}")
        return response

    def _get_item_by_path(self, path: str) -> Optional[DriveItem]:
        path = path.strip("/")
        if path:
            encoded = quote(path)
            url = f"{GRAPH_BASE}/me/drive/root:/{encoded}"
        else:
            url = f"{GRAPH_BASE}/me/drive/root"
        try:
            response = self._request("GET", url, expected=(200,))
        except GraphAPIError as exc:
            # Convert 404 to None so callers can create folders
            if "404" in str(exc):
                return None
            raise
        return DriveItem.from_json(response.json())

    def _root(self) -> DriveItem:
        if not self._root_item:
            item = self._get_item_by_path("")
            if not item:
                raise GraphAPIError("Unable to resolve OneDrive root.")
            self._root_item = item
        return self._root_item

    def _upload_simple(self, local_path: Path, dest_path: str, *, content_type: Optional[str] = None):
        encoded = quote(dest_path.strip("/"))
        url = (
            f"{GRAPH_BASE}/me/drive/root:/{encoded}:/content"
            "?@microsoft.graph.conflictBehavior=replace"
        )
        headers = {}
        if content_type:
            headers["Content-Type"] = content_type
        with local_path.open("rb") as fh:
            response = self._request("PUT", url, data=fh, headers=headers, expected=(200, 201))
        return DriveItem.from_json(response.json())

    def _upload_large(self, local_path: Path, dest_path: str, *, conflict_behavior: str = "replace"):
        encoded = quote(dest_path.strip("/"))
        session_url = f"{GRAPH_BASE}/me/drive/root:/{encoded}:/createUploadSession"
        session_body = {
            "item": {
                "@microsoft.graph.conflictBehavior": conflict_behavior,
                "name": Path(dest_path).name,
            }
        }
        session = self._request("POST", session_url, json=session_body, expected=(200,))
        upload_url = session.json()["uploadUrl"]
        file_size = local_path.stat().st_size
        chunk_size = 5 * 1024 * 1024  # 5 MiB
        with local_path.open("rb") as fh:
            start = 0
            while start < file_size:
                chunk = fh.read(chunk_size)
                end = start + len(chunk) - 1
                headers = {
                    "Content-Length": str(len(chunk)),
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                }
                resp = requests.put(upload_url, headers=headers, data=chunk)
                if resp.status_code in (200, 201):
                    return DriveItem.from_json(resp.json())
                if resp.status_code != 202:
                    try:
                        detail = resp.json()
                    except ValueError:
                        detail = resp.text
                    raise GraphAPIError(f"Chunk upload failed: {resp.status_code}: {detail}")
                start = end + 1
        raise GraphAPIError("Upload session completed without final response.")

    # ---------- Public helpers ----------
    def ensure_folder(self, folder_path: str) -> DriveItem:
        """
        Create the nested folder structure (if needed) and return its drive item.
        """
        folder_path = folder_path.strip("/")
        if not folder_path:
            return self._root()

        parent = self._root()
        segments = [seg for seg in folder_path.split("/") if seg.strip()]
        current_path = []
        for segment in segments:
            current_path.append(segment)
            path_so_far = "/".join(current_path)
            existing = self._get_item_by_path(path_so_far)
            if existing:
                parent = existing
                continue
            create_url = f"{GRAPH_BASE}/me/drive/items/{parent.id}/children"
            payload = {
                "name": segment,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "rename",
            }
            response = self._request("POST", create_url, json=payload, expected=(201,))
            parent = DriveItem.from_json(response.json())
        return parent

    def upload_text(self, folder_path: str, filename: str, content: str, *, return_folder: bool = False):
        """
        Upload (or replace) a UTF-8 text file into the folder path specified.
        """
        folder_path = folder_path.strip("/")
        folder_item = self.ensure_folder(folder_path)
        safe_filename = self._sanitize_filename(filename)
        if folder_path:
            dest_path = f"{folder_path}/{safe_filename}"
        else:
            dest_path = safe_filename
        data = content.encode("utf-8")
        encoded = quote(dest_path)
        url = (
            f"{GRAPH_BASE}/me/drive/root:/{encoded}:/content"
            "?@microsoft.graph.conflictBehavior=replace"
        )
        response = self._request(
            "PUT",
            url,
            data=data,
            headers={"Content-Type": "text/plain; charset=utf-8"},
            expected=(200, 201),
        )
        file_item = DriveItem.from_json(response.json())
        if return_folder:
            return file_item, folder_item
        return file_item

    def upload_file(self, local_path: str | Path, folder_path: str, filename: str, *, content_type: Optional[str] = None):
        folder_path = folder_path.strip("/")
        folder_item = self.ensure_folder(folder_path)
        safe_filename = self._sanitize_filename(filename)
        dest_path = f"{folder_path}/{safe_filename}" if folder_path else safe_filename
        local_path = Path(local_path)
        size = local_path.stat().st_size
        if size <= SMALL_FILE_LIMIT:
            file_item = self._upload_simple(local_path, dest_path, content_type=content_type)
        else:
            file_item = self._upload_large(local_path, dest_path)
        return file_item, folder_item

    def share_item_with_recipients(
        self,
        item_id: Optional[str],
        recipients: list[str],
        *,
        roles: Optional[list[str]] = None,
        send_invitation: bool = False,
        require_sign_in: bool = True,
    ):
        """
        Grant access to an item for the provided recipient emails.
        """
        if not item_id:
            return None
        clean_recipients = [addr.strip() for addr in recipients if addr and addr.strip()]
        if not clean_recipients:
            return None
        url = f"{GRAPH_BASE}/me/drive/items/{item_id}/invite"
        payload = {
            "requireSignIn": bool(require_sign_in),
            "sendInvitation": bool(send_invitation),
            "roles": roles or ["read"],
            "recipients": [{"email": addr} for addr in clean_recipients],
        }
        response = self._request("POST", url, json=payload, expected=(200,))
        return response.json()

    def delete_item_by_path(self, path: str):
        path = path.strip("/")
        if not path:
            return
        encoded = quote(path)
        url = f"{GRAPH_BASE}/me/drive/root:/{encoded}"
        self._request("DELETE", url, expected=(204, 404))

    @staticmethod
    def _sanitize_filename(name: str) -> str:
        cleaned = re.sub(r"[\\/:*?\"<>|]+", "_", name).strip()
        return cleaned or "export.txt"
