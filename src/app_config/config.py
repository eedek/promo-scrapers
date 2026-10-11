from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
ENV_PATH = ROOT_DIR / ".env"

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_PATH, extra = "ignore")
    m_e_shop: str
    mo_shop: str
    x_shop: str

    m_e_url: str
    api_m_e: str

    mo_url: str
    mo_url_2: str

    x_url: str
    x_url_2: str 


    number_of_workers: int
    scraping_dealy: int 
    insert_batch_size: int

    db_user: str
    db_admin: str
    db_name: str

Settings = Settings()