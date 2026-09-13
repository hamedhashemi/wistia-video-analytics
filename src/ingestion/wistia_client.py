from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class WistiaResponse:
    status_code: int
    data: Any | None
    error: str | None = None


class WistiaClient:
    """Small Wistia API client for exploration and later Glue ingestion reuse."""

    BASE_URL = "https://api.wistia.com"

    def __init__(self, token: str, api_version: str = "2026-07", timeout: int = 30):
        if not token:
            raise ValueError("Wistia API token is required")

        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "X-Wistia-API-Version": api_version,
                "User-Agent": "wistia-video-analytics-project/0.1",
            }
        )

        retry = Retry(
            total=5,
            connect=3,
            read=3,
            status=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def get(self, path: str, params: dict[str, Any] | None = None) -> WistiaResponse:
        url = f"{self.BASE_URL}{path}"
        try:
            response = self.session.get(url, params=params, timeout=self.timeout)
        except requests.RequestException as exc:
            return WistiaResponse(status_code=0, data=None, error=type(exc).__name__)

        if not response.ok:
            return WistiaResponse(
                status_code=response.status_code,
                data=None,
                error=f"HTTP {response.status_code}",
            )

        try:
            return WistiaResponse(status_code=response.status_code, data=response.json())
        except ValueError:
            return WistiaResponse(
                status_code=response.status_code,
                data=None,
                error="Non-JSON response",
            )

    def media_metadata(self, media_id: str) -> WistiaResponse:
        return self.get(f"/modern/medias/{media_id}")

    def media_stats(self, media_id: str) -> WistiaResponse:
        return self.get(f"/modern/stats/medias/{media_id}")

    def media_stats_by_date(
        self, media_id: str, start_date: str, end_date: str
    ) -> WistiaResponse:
        return self.get(
            f"/modern/stats/medias/{media_id}/by_date",
            params={"start_date": start_date, "end_date": end_date},
        )

    def media_engagement(self, media_id: str) -> WistiaResponse:
        return self.get(f"/modern/stats/medias/{media_id}/engagement")

    def events(
        self,
        media_id: str,
        start_date: str,
        end_date: str,
        page: int = 1,
        per_page: int = 5,
    ) -> WistiaResponse:
        return self.get(
            "/modern/stats/events",
            params={
                "media_id": media_id,
                "start_date": start_date,
                "end_date": end_date,
                "page": page,
                "per_page": per_page,
            },
        )

    def visitor(self, visitor_key: str) -> WistiaResponse:
        return self.get(f"/modern/stats/visitors/{visitor_key}")
