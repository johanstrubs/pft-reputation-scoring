from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    vhs_base_url: str = "https://vhs.testnet.postfiat.org"
    local_node_rpc: str = "http://127.0.0.1:5005"
    extra_node_rpcs: str = ""
    poll_interval_seconds: int = 300
    database_path: str = "data/scores.db"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    methodology_version: str = "1.0.0"
    log_level: str = "INFO"
    # Manual node-to-validator key mappings (comma-separated node_key:master_key pairs)
    # Example: "n9NodeKey1:nHMasterKey1,n9NodeKey2:nHMasterKey2"
    manual_key_mappings: str = ""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @property
    def extra_rpc_list(self) -> list[str]:
        if not self.extra_node_rpcs:
            return []
        return [url.strip() for url in self.extra_node_rpcs.split(",") if url.strip()]

    @property
    def key_mapping_pairs(self) -> dict[str, str]:
        """Parse manual key mappings into a dict of node_key -> master_key."""
        if not self.manual_key_mappings:
            return {}
        result = {}
        for pair in self.manual_key_mappings.split(","):
            pair = pair.strip()
            if ":" in pair:
                node_key, master_key = pair.split(":", 1)
                result[node_key.strip()] = master_key.strip()
        return result


settings = Settings()
