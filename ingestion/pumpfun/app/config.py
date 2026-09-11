import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    # PumpAPI
    pumpapi_ws_url: str = "wss://stream.pumpapi.io/"

    # Local raw landing directory
    output_dir: str = "./data/pumpapi"

    # Batching
    batch_size: int = 500
    flush_interval_seconds: int = 10

    # WebSocket
    reconnect_delay_seconds: int = 5
    ping_interval_seconds: int = 20
    ping_timeout_seconds: int = 20

    # Application
    source_name: str = "pumpapi"

    # Databricks
    databricks_host: str = ""
    databricks_token: str = ""
    databricks_volume_path: str = (
        "/Volumes/workspace/default/mnt/pumpapi"
    )
    upload_interval_seconds: int = 10

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            pumpapi_ws_url=os.getenv(
                "PUMPAPI_WS_URL",
                "wss://stream.pumpapi.io/",
            ),
            output_dir=os.getenv(
                "OUTPUT_DIR",
                "./data/pumpapi",
            ),
            batch_size=int(
                os.getenv("BATCH_SIZE", "500")
            ),
            flush_interval_seconds=int(
                os.getenv("FLUSH_INTERVAL_SECONDS", "10")
            ),
            reconnect_delay_seconds=int(
                os.getenv("RECONNECT_DELAY_SECONDS", "5")
            ),
            ping_interval_seconds=int(
                os.getenv("PING_INTERVAL_SECONDS", "20")
            ),
            ping_timeout_seconds=int(
                os.getenv("PING_TIMEOUT_SECONDS", "20")
            ),
            source_name=os.getenv(
                "SOURCE_NAME",
                "pumpapi",
            ),
            databricks_host=os.getenv(
                "DATABRICKS_HOST",
                "",
            ).rstrip("/"),
            databricks_token=os.getenv(
                "DATABRICKS_TOKEN",
                "",
            ),
            databricks_volume_path=os.getenv(
                "DATABRICKS_VOLUME_PATH",
                "/Volumes/workspace/default/mnt/pumpapi",
            ).rstrip("/"),
            upload_interval_seconds=int(
                os.getenv(
                    "UPLOAD_INTERVAL_SECONDS",
                    "10",
                )
            ),
        )