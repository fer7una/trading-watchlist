from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

UTC = ZoneInfo("UTC")


@dataclass(frozen=True)
class NewsItem:
    has_catalyst: bool
    headline: Optional[str]
    summary: Optional[str]
    published_at: Optional[str]
    source: Optional[str]


class NewsProviderRestricted(RuntimeError):
    def __init__(self, provider: str, status: int | None):
        super().__init__(f"{provider} restricted endpoint ({status})")
        self.provider = provider
        self.status = status


def _parse_published_at(value: str | None) -> Optional[datetime]:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if " " in cleaned and "T" not in cleaned:
        cleaned = cleaned.replace(" ", "T", 1)
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _clean_symbol(symbol: str) -> str:
    cleaned = symbol.strip().upper()
    if ":" in cleaned:
        cleaned = cleaned.split(":")[-1].strip()
    return cleaned


def _base_result(
    *,
    has_catalyst: Optional[bool],
    status: Optional[int],
    total_news: int,
    recent_news: int,
    error: Optional[str],
    provider: Optional[str],
    headline: Optional[str] = None,
    summary: Optional[str] = None,
    published_at: Optional[str] = None,
    source: Optional[str] = None,
    url: Optional[str] = None,
) -> dict:
    return {
        "hasCatalyst": has_catalyst,
        "headline": headline,
        "summary": summary,
        "publishedAt": published_at,
        "source": source,
        "url": url,
        "status": status,
        "totalNews": total_news,
        "recentNews": recent_news,
        "error": error,
        "provider": provider,
    }


def _unavailable_result(*, provider: str, error: str) -> dict:
    return _base_result(
        has_catalyst=None,
        status=None,
        total_news=0,
        recent_news=0,
        error=error,
        provider=provider,
    )


def restricted_result(provider: str = "fmp") -> dict:
    return _base_result(
        has_catalyst=None,
        status=402,
        total_news=0,
        recent_news=0,
        error="restricted_endpoint_402",
        provider=provider,
    )


def disabled_result(provider: str = "none") -> dict:
    return _unavailable_result(provider=provider, error="disabled")


class FmpNewsProvider:
    def __init__(self, api_key: str, *, base_url: str = "https://financialmodelingprep.com/api/v3", timeout_s: float = 10.0):
        self._api_key = api_key.strip()
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._sess = requests.Session()

    def get_latest_news(self, symbol: str, lookback_hours: int) -> dict:
        if not self._api_key:
            return _base_result(
                has_catalyst=None,
                status=None,
                total_news=0,
                recent_news=0,
                error="missing_api_key",
                provider="fmp",
            )

        url = f"{self._base_url}/stock_news"
        params = {"tickers": _clean_symbol(symbol), "limit": 20, "apikey": self._api_key}
        status: Optional[int] = None
        try:
            r = self._sess.get(url, params=params, timeout=self._timeout_s)
            status = r.status_code
            if r.status_code == 402:
                raise NewsProviderRestricted("fmp", status)
            if r.status_code == 429:
                return _base_result(
                    has_catalyst=None,
                    status=status,
                    total_news=0,
                    recent_news=0,
                    error="rate_limited",
                    provider="fmp",
                )
            r.raise_for_status()
            data = r.json()
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError):
            return _base_result(
                has_catalyst=None,
                status=status,
                total_news=0,
                recent_news=0,
                error="request_failed",
                provider="fmp",
            )

        if not isinstance(data, list):
            return _base_result(
                has_catalyst=None,
                status=status,
                total_news=0,
                recent_news=0,
                error="bad_payload",
                provider="fmp",
            )

        now_utc = datetime.now(tz=UTC)
        cutoff = now_utc - timedelta(hours=lookback_hours)
        total_news = len(data)
        recent_news = 0
        first_item: dict | None = None
        first_published: Optional[datetime] = None
        for item in data:
            if not isinstance(item, dict):
                continue
            published = _parse_published_at(item.get("publishedDate") or item.get("publishedAt"))
            if not published:
                continue
            if published < cutoff:
                continue
            recent_news += 1
            if first_item is None:
                first_item = item
                first_published = published

        if first_item and first_published:
            headline = first_item.get("title") or first_item.get("headline")
            summary = first_item.get("text") or first_item.get("summary")
            source = first_item.get("site") or first_item.get("source")
            url_val = first_item.get("url") or first_item.get("link")
            return _base_result(
                has_catalyst=True,
                status=status,
                total_news=total_news,
                recent_news=recent_news,
                error=None,
                provider="fmp",
                headline=headline,
                summary=summary,
                published_at=first_published.replace(microsecond=0).isoformat(),
                source=source,
                url=url_val,
            )

        return _base_result(
            has_catalyst=False,
            status=status,
            total_news=total_news,
            recent_news=recent_news,
            error=None,
            provider="fmp",
        )


def fetch_news(
    symbols: List[str],
    lookback_hours: int,
    *,
    provider: str | None = None,
    api_key: str | None = None,
) -> Dict[str, dict]:
    if not symbols:
        return {}
    provider = (provider or os.getenv("NEWS_PROVIDER", "fmp")).strip().lower()
    if provider == "none":
        return {sym: disabled_result(provider="none") for sym in symbols}
    if provider != "fmp":
        return {sym: disabled_result(provider=provider) for sym in symbols}

    key = api_key if api_key is not None else os.getenv("FMP_API_KEY", "")
    fmp = FmpNewsProvider(key)
    results: Dict[str, dict] = {}
    for symbol in symbols:
        info = fmp.get_latest_news(symbol, lookback_hours)
        results[symbol] = info
    return results


_DEFAULT_PROVIDER: FmpNewsProvider | None = None


def get_latest_news(symbol: str, lookback_hours: int) -> dict:
    global _DEFAULT_PROVIDER
    api_key = os.getenv("FMP_API_KEY", "").strip()
    if not api_key:
        return _base_result(
            has_catalyst=None,
            status=None,
            total_news=0,
            recent_news=0,
            error="missing_api_key",
            provider="fmp",
        )
    if _DEFAULT_PROVIDER is None or _DEFAULT_PROVIDER._api_key != api_key:
        _DEFAULT_PROVIDER = FmpNewsProvider(api_key)
    try:
        return _DEFAULT_PROVIDER.get_latest_news(symbol, lookback_hours)
    except NewsProviderRestricted as exc:
        return restricted_result(provider=exc.provider)
