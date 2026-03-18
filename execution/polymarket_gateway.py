from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any

logger = logging.getLogger(__name__)


class GatewayError(Exception):
    """Raised when the gateway encounters a non-retryable API error."""


def _require_py_clob_client() -> tuple[Any, Any, Any, Any]:
    try:
        from py_clob_client.client import ClobClient  # type: ignore
        from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType  # type: ignore
    except ImportError as exc:
        if "socksio" in str(exc).lower():
            raise GatewayError(
                "py-clob-client import failed because a SOCKS proxy is configured but "
                "'socksio' is missing. Unset ALL_PROXY or install httpx[socks]."
            ) from exc
        raise GatewayError(
            "py-clob-client is required for live Polymarket trading. "
            "Install dependencies or run with POLY15_EXEC_DRY_RUN=1."
        ) from exc
    return ClobClient, OrderArgs, OrderType, ApiCreds


class PolymarketGateway:
    """Gateway to Polymarket CLOB order APIs.

    Runtime modes:
    - ``dry_run=True``: log intents and return synthetic ids
    - ``dry_run=False``: use the official ``py_clob_client`` backend

    Live mode intentionally delegates order construction / signing to the
    official client rather than re-implementing order signing in-house.
    """

    def __init__(
        self,
        base_url: str = "https://clob.polymarket.com",
        api_key: str = "",
        api_secret: str = "",
        passphrase: str = "",
        *,
        dry_run: bool = True,
        chain_id: int = 137,
        signature_type: int = 1,
        private_key: str = "",
        funder_address: str = "",
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._api_secret = api_secret
        self._passphrase = passphrase
        self._dry_run = dry_run
        self._chain_id = chain_id
        self._signature_type = signature_type
        self._private_key = private_key
        self._funder_address = funder_address
        self._place_counter = 0
        self._live_client: Any | None = None
        self._order_args_cls: Any | None = None
        self._order_type_cls: Any | None = None
        self._api_creds_cls: Any | None = None

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    # ------------------------------------------------------------------
    # Core actions
    # ------------------------------------------------------------------

    async def place(self, payload: dict[str, Any]) -> str:
        """Place an order. Returns the exchange order id string."""
        cid = payload.get("client_order_id", "?")
        if self._dry_run:
            self._place_counter += 1
            fake_id = f"dry_{self._place_counter}_{cid}"
            logger.info(
                "[gateway][DRY] PLACE cid=%s token=%s side=%s price=%s size=%s -> %s",
                cid,
                payload.get("token_id", "?"),
                payload.get("side", "?"),
                payload.get("price", "?"),
                payload.get("size", "?"),
                fake_id,
            )
            return fake_id

        await self._ensure_live_client()
        order_args = self._build_live_order_args(payload)
        create_order = self._resolve_client_method("create_order")
        post_order = self._resolve_client_method("post_order")
        order_type = self._resolve_order_type(payload.get("time_in_force"))

        try:
            signed_order = await asyncio.to_thread(create_order, order_args)
            response = await asyncio.to_thread(
                self._invoke_post_order,
                post_order,
                signed_order,
                order_type,
                bool(payload.get("post_only")),
            )
        except Exception as exc:  # pragma: no cover - defensive wrapper
            raise GatewayError(f"live place failed: {exc}") from exc

        order_id = self._extract_order_id(response)
        if not order_id:
            raise GatewayError(f"live place returned no order id: {response!r}")

        logger.info(
            "[gateway][LIVE] PLACE cid=%s token=%s side=%s price=%s size=%s -> %s",
            cid,
            payload.get("token_id", "?"),
            payload.get("side", "?"),
            payload.get("price", "?"),
            payload.get("size", "?"),
            order_id,
        )
        return order_id

    async def cancel(self, order_id: str) -> None:
        """Cancel an order by exchange order id."""
        if self._dry_run:
            logger.info("[gateway][DRY] CANCEL order_id=%s", order_id)
            return

        await self._ensure_live_client()
        cancel_method = self._resolve_client_method("cancel", "cancel_order")
        try:
            await asyncio.to_thread(cancel_method, order_id)
        except Exception as exc:  # pragma: no cover - defensive wrapper
            raise GatewayError(f"live cancel failed for {order_id}: {exc}") from exc

    async def get_order_status(self, order_id: str) -> dict[str, Any] | None:
        """Fetch raw order status by exchange order id when supported."""
        if self._dry_run:
            logger.debug("[gateway][DRY] GET_STATUS order_id=%s -> None", order_id)
            return None

        await self._ensure_live_client()
        try:
            get_method = self._resolve_client_method("get_order", "get_order_status")
        except GatewayError:
            logger.warning("[gateway][LIVE] get_order_status unsupported by client backend")
            return None

        try:
            response = await asyncio.to_thread(get_method, order_id)
        except Exception as exc:  # pragma: no cover - defensive wrapper
            raise GatewayError(f"live get_order_status failed for {order_id}: {exc}") from exc
        return self._coerce_mapping(response)

    # ------------------------------------------------------------------
    # Live backend helpers
    # ------------------------------------------------------------------

    async def _ensure_live_client(self) -> None:
        if self._live_client is not None:
            return
        if not self._private_key:
            raise GatewayError("live trading requires POLY15_PM_PRIVATE_KEY")

        ClobClient, OrderArgs, OrderType, ApiCreds = _require_py_clob_client()
        kwargs: dict[str, Any] = {
            "host": self._base_url,
            "key": self._private_key,
            "chain_id": self._chain_id,
            "signature_type": self._signature_type,
        }
        if self._funder_address:
            kwargs["funder"] = self._funder_address

        client = ClobClient(**kwargs)
        self._set_api_creds_if_possible(client)
        self._live_client = client
        self._order_args_cls = OrderArgs
        self._order_type_cls = OrderType
        self._api_creds_cls = ApiCreds

    def _set_api_creds_if_possible(self, client: Any) -> None:
        set_api_creds = getattr(client, "set_api_creds", None)
        derive_api_creds = getattr(client, "create_or_derive_api_creds", None)
        if not callable(set_api_creds):
            return

        explicit = self._build_api_creds_candidate()
        if explicit is not None:
            try:
                set_api_creds(explicit)
                return
            except Exception:
                logger.warning(
                    "[gateway][LIVE] explicit API creds were rejected; falling back to derive"
                )

        if callable(derive_api_creds):
            derived = derive_api_creds()
            set_api_creds(derived)

    def _build_api_creds_candidate(self) -> Any | None:
        if not (self._api_key and self._api_secret and self._passphrase):
            return None
        candidates: list[Any] = [
            {
                "api_key": self._api_key,
                "api_secret": self._api_secret,
                "api_passphrase": self._passphrase,
            },
            {
                "apiKey": self._api_key,
                "secret": self._api_secret,
                "passphrase": self._passphrase,
            },
        ]

        if self._api_creds_cls is not None:
            ctor_attempts = [
                {"api_key": self._api_key, "api_secret": self._api_secret, "api_passphrase": self._passphrase},
                {"api_key": self._api_key, "secret": self._api_secret, "passphrase": self._passphrase},
                {"apiKey": self._api_key, "secret": self._api_secret, "passphrase": self._passphrase},
            ]
            for kwargs in ctor_attempts:
                try:
                    return self._api_creds_cls(**kwargs)
                except TypeError:
                    continue
            candidates.append(
                self._api_creds_cls(self._api_key, self._api_secret, self._passphrase)
            )

        return candidates[0]

    def _build_live_order_args(self, payload: dict[str, Any]) -> Any:
        if self._order_args_cls is None:
            raise GatewayError("live order args class is not initialized")
        return self._order_args_cls(
            token_id=str(payload["token_id"]),
            price=float(payload["price"]),
            size=float(payload["size"]),
            side=str(payload.get("side", "")).upper(),
            fee_rate_bps=int(payload.get("fee_rate_bps", 0)),
        )

    def _resolve_client_method(self, *names: str) -> Any:
        if self._live_client is None:
            raise GatewayError("live client is not initialized")
        for name in names:
            method = getattr(self._live_client, name, None)
            if callable(method):
                return method
        raise GatewayError(f"live client missing required method(s): {names}")

    def _resolve_order_type(self, value: Any) -> Any:
        if self._order_type_cls is None:
            raise GatewayError("live order type enum is not initialized")
        normalized = str(value or "GTC").upper()
        try:
            return getattr(self._order_type_cls, normalized)
        except AttributeError as exc:
            raise GatewayError(f"unsupported time_in_force for live mode: {normalized}") from exc

    @staticmethod
    def _invoke_post_order(
        method: Any,
        signed_order: Any,
        order_type: Any,
        post_only: bool,
    ) -> Any:
        signature = inspect.signature(method)
        kwargs: dict[str, Any] = {}
        if "post_only" in signature.parameters:
            kwargs["post_only"] = post_only
        elif "book_only" in signature.parameters:
            kwargs["book_only"] = post_only
        return method(signed_order, order_type, **kwargs)

    @staticmethod
    def _extract_order_id(response: Any) -> str | None:
        if isinstance(response, dict):
            for key in ("orderID", "order_id", "id"):
                value = response.get(key)
                if value:
                    return str(value)
            return None
        for attr in ("orderID", "order_id", "id"):
            value = getattr(response, attr, None)
            if value:
                return str(value)
        return None

    @staticmethod
    def _coerce_mapping(response: Any) -> dict[str, Any] | None:
        if response is None:
            return None
        if isinstance(response, dict):
            return response
        if hasattr(response, "__dict__"):
            return {
                key: value
                for key, value in vars(response).items()
                if not key.startswith("_")
            }
        return {"value": response}
