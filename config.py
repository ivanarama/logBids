from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DATABASE_URL: str
    SMTP_SERVER: str
    SMTP_PORT: int
    SMTP_USER: str
    SMTP_PASS: str
    REPORT_EMAIL_TO: str
    REPORT_EMAIL_TEST_TO: str = ""  # Тестовая почта для debug режима
    SECRET_KEY: str

    model_config = {
        "env_file": ".env"
    }

settings = Settings()