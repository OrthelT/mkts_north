from mkts_backend.esi.esi_auth import get_token
from mkts_backend.config.logging_config import configure_logging

logger = configure_logging(__name__)


class ESIConfig:
    """ESI configuration for primary and secondary markets."""

    _region_ids = {"primary_region_id": 10000003, "secondary_region_id": None}
    _system_ids = {"primary_system_id": 30000240, "secondary_system_id": None}
    _structure_ids = {"primary_structure_id": 1035466617946, "secondary_structure_id": None}
    _valid_aliases = ["primary", "secondary"]
    _shortcut_aliases = {"4h": "primary", "nakah": "secondary"}
    _names = {"primary": "4-HWWF Keepstar", "secondary": "Nakah I - Moon 1 - Thukker Mix Factory"}

    def __init__(self, alias: str):
        alias = alias.lower()
        if alias not in self._valid_aliases and alias not in self._shortcut_aliases:
            raise ValueError(
                f"Invalid alias: {alias}. Valid aliases are: {self._valid_aliases} or {list(self._shortcut_aliases.keys())}"
            )
        elif alias in self._shortcut_aliases:
            self.alias = self._shortcut_aliases[alias]
        else:
            self.alias = alias
        self.name = self._names[self.alias]
        self.region_id = self._region_ids[f"{self.alias}_region_id"]
        self.system_id = self._system_ids[f"{self.alias}_system_id"]
        self.structure_id = self._structure_ids[f"{self.alias}_structure_id"]

        self.user_agent = 'wcmkts_backend/2.1dev, orthel.toralen@gmail.com, (https://github.com/OrthelT/wcmkts_backend)'
        self.compatibility_date = "2025-08-26"

    def token(self, scope: str = "esi-markets.structure_markets.v1"):
        return get_token(scope)

    @property
    def market_orders_url(self):
        if self.alias == "primary":
            return f"https://esi.evetech.net/markets/structures/{self.structure_id}"
        elif self.alias == "secondary":
            return f"https://esi.evetech.net/markets/{self.region_id}/orders"

    @property
    def market_history_url(self):
        return f"https://esi.evetech.net/markets/{self.region_id}/history"

    @property
    def headers(self, etag: str = None) -> dict:
        if self.alias == "primary":
            token = self.token()
            return {
                "Accept-Language": "en",
                "If-None-Match": f"{etag}",
                "X-Compatibility-Date": self.compatibility_date,
                "X-Tenant": "tranquility",
                "Accept": "application/json",
                "Authorization": f"Bearer {token['access_token']}",
            }
        elif self.alias == "secondary":
            return {
                "Accept-Language": "en",
                "If-None-Match": etag,
                "X-Compatibility-Date": self.compatibility_date,
                "Accept": "application/json",
                "User-Agent": self.user_agent,
            }
        else:
            raise ValueError(f"Invalid alias: {self.alias}. Valid aliases are: {self._valid_aliases}")

