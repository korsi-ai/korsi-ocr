"""FastAPI server configuration."""

import dataclasses
import os
from pathlib import Path

import dotenv
from fastapi_mongo_base.core import config

dotenv.load_dotenv()


@dataclasses.dataclass
class Settings(config.Settings):
    """Server config settings."""

    project_name: str = os.getenv("PROJECT_NAME")
    base_dir: Path = Path(__file__).resolve().parent.parent
    base_path: str = "/api/ai/v1"

    coverage_dir: Path = base_dir / "htmlcov"
    currency: str = "IRR"

    finance_api_key: str = os.getenv("API_KEY")
    gemini_api_key: str = os.getenv("GEMINI_API_KEY")
    metis_api_key: str = os.getenv("METIS_API_KEY")
    pishrun_api_key: str = os.getenv("PISHRUN_API_KEY")
    dify_api_key: str = os.getenv("DIFY_API_KEY")

    minutes_price: float = os.getenv("MINUTES_PRICE", 1)

    @classmethod
    def get_log_config(cls, console_level: str = "INFO", **kwargs: object) -> dict:
        log_config = {
            "formatters": {
                "standard": {
                    "format": "[{levelname} {name} : {filename}:{lineno} : {asctime} -> {funcName:10}] {message}",  # noqa: E501
                    "style": "{",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": console_level,
                    "formatter": "standard",
                },
                "file": {
                    "class": "logging.FileHandler",
                    "level": "INFO",
                    "formatter": "standard",
                    "filename": "logs/app.log",
                },
            },
            "loggers": {
                "": {
                    "handlers": [
                        "console",
                        "file",
                    ],
                    "level": console_level,
                    "propagate": True,
                },
            },
            "version": 1,
        }
        return log_config
