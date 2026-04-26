from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import requests


@dataclass
class DownloadResult:
    status: str
    path: Path | None = None
    error: str | None = None


def download_with_retries(
    url: str,
    dest_path: Path,
    max_retries: int = 5,
    backoff_sec: float = 2.0,
    timeout: int = 30,
    session: requests.Session | None = None,
) -> DownloadResult:
    """
    Download a URL to dest_path with retries. Returns status "ok", "not_found", or "failed".
    """
    sess = session or requests.Session()
    last_error: str | None = None
    for attempt in range(max_retries + 1):
        try:
            response = sess.get(url, stream=True, timeout=timeout)
            if response.status_code == 404:
                return DownloadResult(status="not_found")
            if response.status_code != 200:
                last_error = f"HTTP {response.status_code}: {response.text}"
                raise RuntimeError(last_error)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = dest_path.with_suffix(dest_path.suffix + ".tmp")
            with temp_path.open("wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
            temp_path.replace(dest_path)
            return DownloadResult(status="ok", path=dest_path)
        except Exception as exc:
            last_error = str(exc)
            if attempt >= max_retries:
                break
            import random

            jitter = random.uniform(0, 1)
            time.sleep(min(backoff_sec * (2**attempt) + jitter, 60.0))
    return DownloadResult(status="failed", error=last_error)
