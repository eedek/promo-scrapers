from scrapers.m_e import run_m_e
from scrapers.mo import run_mo
from scrapers.x import run_x
from scrapers.bd import run_bd

run_bd.delay()
# run_m_e.delay()
# run_mo.delay()
# run_x.delay()