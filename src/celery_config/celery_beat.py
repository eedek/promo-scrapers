from celery import Celery
from celery_app import app
from scrapers.media_expert import run_media_expert
from scrapers.morele import run_morele
from scrapers.xkom import run_xkom


@app.on_after_configure.connect
def run_app(sender: Celery, **kwargs):
    sender.add_periodic_task(10800, run_media_expert.s(), name='run every 3 hours')
    sender.add_periodic_task(10800, run_morele.s(), name='run every 3 hours')
    sender.add_periodic_task(10800, run_xkom.s(), name='run every 3 hours')

