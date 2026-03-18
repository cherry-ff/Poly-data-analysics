from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Awaitable, Callable

from domain.models import MarketMetadata


Fetcher = Callable[[str], Awaitable[dict[str, Any]]]

DEFAULT_MIN_TARGET_DURATION_MINUTES = 12
DEFAULT_MAX_TARGET_DURATION_MINUTES = 20


class MarketMetadataLoader:
    def __init__(
        self,
        gamma_base_url: str = "https://gamma-api.polymarket.com",
        fetcher: Fetcher | None = None,
        cache_ttl_ms: int = 300_000,
        request_timeout_ms: int = 5000,
        return_stale_on_error: bool = True,
        proxy_url: str | None = None,
        market_filter_enabled: bool = False,
        filter_keywords: tuple[str, ...] = (),
        filter_exclude_keywords: tuple[str, ...] = (),
        filter_min_duration_minutes: int = DEFAULT_MIN_TARGET_DURATION_MINUTES,
        filter_max_duration_minutes: int = DEFAULT_MAX_TARGET_DURATION_MINUTES,
    ) -> None:
        self._gamma_base_url = gamma_base_url.rstrip("/")
        self._fetcher = fetcher
        self._cache_ttl_ms = cache_ttl_ms
        self._request_timeout_ms = request_timeout_ms
        self._return_stale_on_error = return_stale_on_error
        self._proxy_url = proxy_url or None
        self._market_filter_enabled = market_filter_enabled
        self._filter_keywords = filter_keywords
        self._filter_exclude_keywords = filter_exclude_keywords
        self._filter_min_duration_minutes = filter_min_duration_minutes
        self._filter_max_duration_minutes = filter_max_duration_minutes
        self._cache: dict[str, tuple[MarketMetadata, int]] = {}
        self._tag_id_cache: dict[str, str] = {}

    def seed(self, market: MarketMetadata) -> None:
        self._cache[market.market_id] = (market, self._now_ms())

    async def load_market(self, market_id: str) -> MarketMetadata:
        cached_entry = self._cache.get(market_id)
        if cached_entry is not None:
            cached, loaded_ts_ms = cached_entry
            if self._cache_ttl_ms <= 0 or self._now_ms() - loaded_ts_ms <= self._cache_ttl_ms:
                return cached

        fetcher = self._fetcher
        if fetcher is None and self._gamma_base_url:
            fetcher = self._fetch_remote_market

        if fetcher is None:
            raise LookupError(
                "no metadata fetcher configured; seed() or inject a fetcher first"
            )

        try:
            payload = await fetcher(market_id)
            if self._market_filter_enabled:
                payload = self._normalize_loaded_market_payload(payload, market_id)
            market = self.normalize_market_payload(payload)
        except Exception:
            if cached_entry is not None and self._return_stale_on_error:
                return cached_entry[0]
            raise

        self.seed(market)
        return market

    async def _fetch_remote_market(self, market_id: str) -> dict[str, Any]:
        payload = await self._fetch_json(self._market_url(market_id))
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            payload = payload["data"]
        if isinstance(payload, list):
            if not payload:
                raise LookupError(f"market {market_id} not found")
            payload = payload[0]
        if not isinstance(payload, dict):
            raise TypeError(f"unexpected metadata payload: {type(payload)!r}")
        return payload

    def _normalize_loaded_market_payload(
        self,
        payload: dict[str, Any],
        market_id: str,
    ) -> dict[str, Any]:
        normalized = self._normalize_discovery_candidate(
            payload,
            keywords=self._filter_keywords,
            exclude_keywords=self._filter_exclude_keywords,
            min_duration_minutes=self._filter_min_duration_minutes,
            max_duration_minutes=self._filter_max_duration_minutes,
        )
        if normalized is None:
            raise LookupError(
                f"market {market_id} does not match configured BTC 15-minute filters"
            )
        return normalized

    async def discover_markets(
        self,
        *,
        tag_slug: str = "bitcoin",
        keywords: tuple[str, ...] = ("btc", "bitcoin"),
        exclude_keywords: tuple[str, ...] = (
            "weekly",
            "daily",
            "election",
            "approval",
        ),
        min_duration_minutes: int = 12,
        max_duration_minutes: int = 20,
        max_markets: int = 3,
        page_limit: int = 100,
        max_pages: int = 3,
    ) -> list[MarketMetadata]:
        if not self._gamma_base_url:
            raise LookupError("gamma_base_url is empty")

        tag_id = await self._fetch_tag_id(tag_slug) if tag_slug else None
        candidates: list[dict[str, Any]] = []
        for page in range(max_pages):
            params = {
                "closed": "false",
                "active": "true",
                "limit": str(page_limit),
                "offset": str(page * page_limit),
                "order": "endDate",
                "ascending": "true",
            }
            if tag_id:
                params["tag_id"] = tag_id
            payload = await self._fetch_json(
                f"{self._gamma_base_url}/markets",
                params=params,
            )
            if not isinstance(payload, list):
                raise TypeError(f"unexpected markets payload: {type(payload)!r}")
            if not payload:
                break

            for item in payload:
                if not isinstance(item, dict):
                    continue
                candidate = self._normalize_discovery_candidate(
                    item,
                    keywords=keywords,
                    exclude_keywords=exclude_keywords,
                    min_duration_minutes=min_duration_minutes,
                    max_duration_minutes=max_duration_minutes,
                )
                if candidate is not None:
                    candidates.append(candidate)

            if len(payload) < page_limit:
                break

        candidates.sort(key=lambda item: self._ts_ms(item.get("endDate")))
        normalized_markets: list[MarketMetadata] = []
        seen: set[str] = set()
        for item in candidates:
            market = self.normalize_market_payload(item)
            if (
                market.market_id in seen
                or market.start_ts_ms <= 0
                or market.end_ts_ms <= 0
            ):
                continue
            seen.add(market.market_id)
            normalized_markets.append(market)

        markets = self._select_discovery_markets(
            normalized_markets,
            now_ts_ms=self._utc_now_ts_ms(),
            max_markets=max_markets,
        )
        for market in markets:
            self.seed(market)
        return markets

    async def _fetch_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        try:
            import aiohttp  # type: ignore
        except ImportError as exc:
            raise RuntimeError("aiohttp is required to fetch market metadata") from exc

        timeout = aiohttp.ClientTimeout(total=self._request_timeout_ms / 1000)
        connector = aiohttp.TCPConnector(ssl=False) if self._proxy_url else None
        async with aiohttp.ClientSession(
            timeout=timeout,
            trust_env=True,
            connector=connector,
        ) as session:
            async with session.get(
                url,
                params=params,
                proxy=self._proxy_url,
            ) as response:
                response.raise_for_status()
                return await response.json()

    async def _fetch_tag_id(self, tag_slug: str) -> str | None:
        if not tag_slug:
            return None
        cached = self._tag_id_cache.get(tag_slug)
        if cached is not None:
            return cached

        payload = await self._fetch_json(f"{self._gamma_base_url}/tags/slug/{tag_slug}")
        if not isinstance(payload, dict):
            return None
        tag_id = payload.get("id")
        if tag_id in (None, ""):
            return None
        tag_id_str = str(tag_id)
        self._tag_id_cache[tag_slug] = tag_id_str
        return tag_id_str

    @staticmethod
    def normalize_market_payload(payload: dict[str, Any]) -> MarketMetadata:
        token_ids = MarketMetadataLoader._token_ids(payload)
        payload = MarketMetadataLoader._coerce_payload_times(payload)
        market_id = str(
            payload.get("id")
            or payload.get("market_id")
            or payload.get("conditionId")
            or payload.get("condition_id")
            or ""
        )
        condition_id = str(
            payload.get("conditionId")
            or payload.get("condition_id")
            or market_id
        )
        start_ts_ms = MarketMetadataLoader._ts_ms(
            payload.get("startDate")
            or payload.get("start_time")
            or payload.get("start_ts_ms")
        )
        end_ts_ms = MarketMetadataLoader._ts_ms(
            payload.get("endDate")
            or payload.get("end_time")
            or payload.get("end_ts_ms")
            or payload.get("end_date_iso")
        )
        return MarketMetadata(
            market_id=market_id,
            condition_id=condition_id,
            up_token_id=token_ids[0],
            down_token_id=token_ids[1],
            start_ts_ms=start_ts_ms,
            end_ts_ms=end_ts_ms,
            tick_size=Decimal(
                str(
                    payload.get("minimum_tick_size")
                    or payload.get("tick_size")
                    or payload.get("orderPriceMinTickSize")
                    or "0.01"
                )
            ),
            fee_rate_bps=Decimal(
                str(
                    payload.get("fee_rate_bps")
                    or payload.get("feeRateBps")
                    or payload.get("feerateBps")
                    or "0"
                )
            ),
            min_order_size=Decimal(
                str(
                    payload.get("minimum_order_size")
                    or payload.get("min_order_size")
                    or payload.get("minOrderSize")
                    or "0"
                )
            ),
            status=str(
                payload.get("status") or ("active" if payload.get("active") else "")
            ),
            reference_price=MarketMetadataLoader._reference_price(payload),
        )

    def _market_url(self, market_id: str) -> str:
        if self._gamma_base_url.endswith("/markets"):
            return f"{self._gamma_base_url}/{market_id}"
        return f"{self._gamma_base_url}/markets/{market_id}"

    @staticmethod
    def _now_ms() -> int:
        return time.monotonic_ns() // 1_000_000

    @staticmethod
    def _utc_now_ts_ms() -> int:
        return int(datetime.now(timezone.utc).timestamp() * 1000)

    @staticmethod
    def _token_ids(payload: dict[str, Any]) -> tuple[str, str]:
        raw = payload.get("clobTokenIds") or payload.get("token_ids")
        outcomes = MarketMetadataLoader._decoded_list(
            payload.get("outcomes") or payload.get("outcome_names")
        )
        if raw is not None:
            token_ids = MarketMetadataLoader._decoded_list(raw)
            if len(token_ids) >= 2:
                mapped = MarketMetadataLoader._map_outcomes_to_tokens(
                    token_ids,
                    outcomes,
                )
                if mapped is not None:
                    return mapped
                return str(token_ids[0]), str(token_ids[1])

        tokens = payload.get("tokens")
        if isinstance(tokens, list) and len(tokens) >= 2:
            mapped_tokens = []
            for item in tokens:
                if not isinstance(item, dict):
                    continue
                token_id = item.get("token_id") or item.get("id")
                if token_id in (None, ""):
                    continue
                mapped_tokens.append((str(item.get("outcome") or ""), str(token_id)))
            if len(mapped_tokens) >= 2:
                mapped = MarketMetadataLoader._map_labeled_tokens(mapped_tokens)
                if mapped is not None:
                    return mapped
                return mapped_tokens[0][1], mapped_tokens[1][1]

        raise ValueError("unable to extract token ids from market payload")

    @staticmethod
    def _ts_ms(value: Any) -> int:
        if value is None:
            return 0
        if isinstance(value, (int, float)):
            numeric = int(value)
            return numeric * 1000 if numeric < 10_000_000_000 else numeric
        text = str(value).strip()
        if text.isdigit():
            numeric = int(text)
            return numeric * 1000 if numeric < 10_000_000_000 else numeric
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return int(parsed.timestamp() * 1000)

    @staticmethod
    def _reference_price(payload: dict[str, Any]) -> Decimal | None:
        for candidate in (
            payload.get("reference_price"),
            payload.get("referencePrice"),
            payload.get("strike_price"),
            payload.get("strikePrice"),
            payload.get("threshold"),
            payload.get("startPrice"),
            payload.get("start_price"),
        ):
            normalized = MarketMetadataLoader._normalize_reference_candidate(candidate)
            if normalized is not None:
                return normalized

        text_sources = [
            payload.get("question"),
            payload.get("title"),
            payload.get("description"),
            payload.get("slug"),
        ]
        for source in text_sources:
            if not source:
                continue
            text = str(source)
            dollar_match = re.search(
                r"\$(\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d{4,}(?:\.\d+)?)",
                text,
            )
            if dollar_match is not None:
                normalized = MarketMetadataLoader._normalize_reference_candidate(
                    dollar_match.group(1),
                )
                if normalized is not None:
                    return normalized

            large_number_match = re.search(
                r"\b(\d{4,}(?:,\d{3})*(?:\.\d+)?)\b",
                text,
            )
            if large_number_match is not None:
                normalized = MarketMetadataLoader._normalize_reference_candidate(
                    large_number_match.group(1),
                )
                if normalized is not None:
                    return normalized
        return None

    @staticmethod
    def _normalize_reference_candidate(value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        try:
            numeric = Decimal(str(value).replace(",", ""))
        except Exception:
            return None

        if numeric <= 0:
            return None

        if numeric >= Decimal("1000000"):
            return None

        integral = int(numeric)
        if Decimal(integral) == numeric and 1900 <= integral <= 2100:
            return None

        return numeric

    @staticmethod
    def _decoded_list(value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError:
                return [part.strip() for part in value.split(",") if part.strip()]
            if isinstance(decoded, list):
                return decoded
        return []

    @staticmethod
    def _map_outcomes_to_tokens(
        token_ids: list[Any],
        outcomes: list[Any],
    ) -> tuple[str, str] | None:
        if len(token_ids) < 2 or len(outcomes) < 2:
            return None
        labeled_tokens = [
            (str(outcomes[idx]), str(token_ids[idx]))
            for idx in range(min(len(token_ids), len(outcomes)))
        ]
        return MarketMetadataLoader._map_labeled_tokens(labeled_tokens)

    @staticmethod
    def _map_labeled_tokens(
        labeled_tokens: list[tuple[str, str]],
    ) -> tuple[str, str] | None:
        up_token = None
        down_token = None
        for raw_label, token_id in labeled_tokens:
            label = raw_label.strip().lower()
            if label in {"yes", "up"} and up_token is None:
                up_token = token_id
            elif label in {"no", "down"} and down_token is None:
                down_token = token_id
        if up_token and down_token:
            return up_token, down_token
        return None

    @staticmethod
    def _coerce_payload_times(payload: dict[str, Any]) -> dict[str, Any]:
        start = (
            payload.get("startDate")
            or payload.get("start_time")
            or payload.get("start_ts_ms")
        )
        end = (
            payload.get("endDate")
            or payload.get("end_time")
            or payload.get("end_ts_ms")
            or payload.get("end_date_iso")
        )
        question = payload.get("question") or payload.get("title") or ""
        api_start = MarketMetadataLoader._parse_iso_date(start)
        api_end = MarketMetadataLoader._parse_iso_date(end)
        payload_duration = MarketMetadataLoader._duration_minutes(api_start, api_end)
        parsed_start, parsed_end, parsed_duration = MarketMetadataLoader._extract_market_times(
            str(question),
            api_end,
        )
        normalized = dict(payload)
        if MarketMetadataLoader._is_duration_in_range(
            payload_duration,
            DEFAULT_MIN_TARGET_DURATION_MINUTES,
            DEFAULT_MAX_TARGET_DURATION_MINUTES,
        ):
            return payload
        if (
            parsed_start is not None
            and parsed_end is not None
            and MarketMetadataLoader._is_duration_in_range(
                parsed_duration,
                DEFAULT_MIN_TARGET_DURATION_MINUTES,
                DEFAULT_MAX_TARGET_DURATION_MINUTES,
            )
        ):
            normalized["startDate"] = parsed_start.isoformat().replace("+00:00", "Z")
            normalized["endDate"] = parsed_end.isoformat().replace("+00:00", "Z")
            return normalized
        if start in (None, "") and parsed_start is not None:
            normalized["startDate"] = parsed_start.isoformat().replace("+00:00", "Z")
        if end in (None, "") and parsed_end is not None:
            normalized["endDate"] = parsed_end.isoformat().replace("+00:00", "Z")
        return normalized

    @staticmethod
    def _normalize_discovery_candidate(
        payload: dict[str, Any],
        *,
        keywords: tuple[str, ...],
        exclude_keywords: tuple[str, ...],
        min_duration_minutes: int,
        max_duration_minutes: int,
    ) -> dict[str, Any] | None:
        full_text = " ".join(
            str(payload.get(field) or "")
            for field in ("question", "description", "title", "slug")
        ).lower()
        if keywords and not any(keyword.lower() in full_text for keyword in keywords):
            return None
        if exclude_keywords and any(
            keyword.lower() in full_text for keyword in exclude_keywords
        ):
            return None

        normalized = MarketMetadataLoader._coerce_payload_times(payload)
        start_dt = MarketMetadataLoader._parse_iso_date(
            normalized.get("startDate")
            or normalized.get("start_time")
            or normalized.get("start_ts_ms")
        )
        end_dt = MarketMetadataLoader._parse_iso_date(
            normalized.get("endDate")
            or normalized.get("end_time")
            or normalized.get("end_ts_ms")
            or normalized.get("end_date_iso")
        )
        duration = MarketMetadataLoader._duration_minutes(start_dt, end_dt)
        if end_dt is None or duration < 0:
            return None
        if (
            duration < min_duration_minutes or duration > max_duration_minutes
        ):
            return None
        if end_dt <= datetime.now(timezone.utc):
            return None

        if start_dt is not None:
            normalized["startDate"] = start_dt.isoformat().replace("+00:00", "Z")
        if end_dt is not None:
            normalized["endDate"] = end_dt.isoformat().replace("+00:00", "Z")
        return normalized

    @staticmethod
    def _select_discovery_markets(
        markets: list[MarketMetadata],
        *,
        now_ts_ms: int,
        max_markets: int,
    ) -> list[MarketMetadata]:
        if max_markets <= 0:
            return []

        active = sorted(
            [
                market
                for market in markets
                if market.start_ts_ms <= now_ts_ms < market.end_ts_ms
            ],
            key=lambda market: (market.end_ts_ms, market.start_ts_ms, market.market_id),
        )
        upcoming = sorted(
            [market for market in markets if market.start_ts_ms > now_ts_ms],
            key=lambda market: (market.start_ts_ms, market.end_ts_ms, market.market_id),
        )

        selected: list[MarketMetadata] = []
        if active:
            selected.append(active[0])

        remaining = max_markets - len(selected)
        if remaining > 0:
            selected.extend(upcoming[:remaining])

        if len(selected) >= max_markets:
            return selected[:max_markets]

        selected_ids = {market.market_id for market in selected}
        for market in (*active[1:], *upcoming[remaining:]):
            if market.market_id in selected_ids:
                continue
            selected.append(market)
            selected_ids.add(market.market_id)
            if len(selected) >= max_markets:
                break
        return selected

    @staticmethod
    def _parse_iso_date(date_str: Any) -> datetime | None:
        if date_str in (None, ""):
            return None
        try:
            text = str(date_str)
            if text.endswith("Z"):
                text = text[:-1] + "+00:00"
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    @staticmethod
    def _extract_market_times(
        question: str,
        api_end_date: datetime | None,
    ) -> tuple[datetime | None, datetime | None, float]:
        pattern = (
            r"([A-Za-z]+)\s+(\d+),.*?"
            r"(\d{1,2}:\d{2}\s*(?:AM|PM))\s*-\s*"
            r"(\d{1,2}:\d{2}\s*(?:AM|PM))\s+ET"
        )
        match = re.search(pattern, question, re.IGNORECASE)
        if match is None:
            return None, api_end_date, -1.0

        month_str, day_str, start_str, end_str = match.groups()
        now_utc = datetime.now(timezone.utc)
        base_str = f"{now_utc.year} {month_str} {day_str}"
        fmt = "%Y %B %d %I:%M%p"
        normalized_start_str = re.sub(r"\s+", "", start_str.upper())
        normalized_end_str = re.sub(r"\s+", "", end_str.upper())
        try:
            start_dt = datetime.strptime(f"{base_str} {normalized_start_str}", fmt)
            end_dt = datetime.strptime(f"{base_str} {normalized_end_str}", fmt)
        except ValueError:
            return None, api_end_date, -1.0

        if end_dt < start_dt:
            end_dt += timedelta(days=1)
        duration = (end_dt - start_dt).total_seconds() / 60.0

        try:
            from zoneinfo import ZoneInfo

            tz = ZoneInfo("America/New_York")
        except Exception:
            tz = timezone(timedelta(hours=-5))

        start_utc = start_dt.replace(tzinfo=tz).astimezone(timezone.utc)
        end_utc = end_dt.replace(tzinfo=tz).astimezone(timezone.utc)

        if (now_utc - end_utc).days > 300:
            start_utc = start_utc.replace(year=now_utc.year + 1)
            end_utc = end_utc.replace(year=now_utc.year + 1)
        elif (end_utc - now_utc).days > 300:
            start_utc = start_utc.replace(year=now_utc.year - 1)
            end_utc = end_utc.replace(year=now_utc.year - 1)

        return start_utc, end_utc, duration

    @staticmethod
    def _duration_minutes(
        start_dt: datetime | None,
        end_dt: datetime | None,
    ) -> float:
        if start_dt is None or end_dt is None or end_dt <= start_dt:
            return -1.0
        return (end_dt - start_dt).total_seconds() / 60.0

    @staticmethod
    def _is_duration_in_range(
        duration_minutes: float,
        min_duration_minutes: int,
        max_duration_minutes: int,
    ) -> bool:
        return min_duration_minutes <= duration_minutes <= max_duration_minutes
