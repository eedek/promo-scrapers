from celery import Celery
from celery_config.celery_app import app
from scrapers.m_e import run_m_e
from scrapers.mo import run_mo
from scrapers.x import run_x


@app.on_after_finalize.connect
def run_app(sender: Celery, **kwargs):
    # sender.add_periodic_task(10800, run_media_expert.s(), name='run every 3 hours')
    # sender.add_periodic_task(10800, run_morele.s(), name='run every 3 hours')
    # sender.add_periodic_task(10800, run_xkom.s(), name='run every 3 hours')


    sender.add_periodic_task(300, run_m_e.s(), name='run every 3 hours')

