from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    vhs_base_url: str = "http://rpc.testnet.postfiat.org:3000"
    local_node_rpc: str = "http://127.0.0.1:5005"
    extra_node_rpcs: str = ""
    poll_interval_seconds: int = 300
    database_path: str = "data/scores.db"
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    methodology_version: str = "1.0.0"
    log_level: str = "INFO"

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    @property
    def extra_rpc_list(self) -> list[str]:
        if not self.extra_node_rpcs:
            return []
        return [url.strip() for url in self.extra_node_rpcs.split(",") if url.strip()]


settings = Settings()
