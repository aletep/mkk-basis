from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", hide_input_in_errors=True)
    api_key: SecretStr = Field(min_length=16)
    database_url: SecretStr
    amqp_url: SecretStr
    webhook_allowed_origins: list[str] = ["http://webhook-receiver:8000"]
