from scrapers.media_expert import run_media_expert
from scrapers.morele import run_morele
from scrapers.xkom import run_xkom

run_media_expert.delay()
# run_morele()
# run_xkom()