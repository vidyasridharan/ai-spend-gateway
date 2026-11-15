from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    upstream_base_url: str = "http://localhost:11434/v1"
    upstream_api_key: str = "ollama"
    redis_url: str = "redis://localhost:6379/0"
    default_preauth_tokens: int = 2000

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()

