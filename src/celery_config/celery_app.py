from celery import Celery, group, chord

app = Celery(
    "scrapers", 
    broker="redis://redis", 
    backend='redis://redis:6379/0',
    include=['scrapers.m_e','scrapers.x','scrapers.mo', 'scrapers.bd', 'celery_config.celery_beat', 'scrapers.config', 'app_config.config'])