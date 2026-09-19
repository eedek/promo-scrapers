from celery import Celery, group, chord

app = Celery(
    "scrapers", 
    broker="redis://redis", 
    backend='redis://redis:6379/0',
    include=['scrapers.media_expert','scrapers.xkom','scrapers.morele' ])