from app_config.config import settings
from celery import Celery
from celery_config.celery_app import app
from scrapers.m_e import run_m_e
from scrapers.mo import run_mo
from scrapers.x import run_x


@app.on_after_finalize.connect
def run_app(sender: Celery, **kwargs):
    sender.add_periodic_task(10800, run_m_e.s(), name='run every 3 hours')
    sender.add_periodic_task(10800, run_mo.s(), name='run every 3 hours')
    sender.add_periodic_task(10800, run_x.s(), name='run every 3 hours')

