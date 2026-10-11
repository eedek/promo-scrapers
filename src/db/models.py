from sqlalchemy import Column, Integer, String, Float, Date, UniqueConstraint
from database import Base

class Promotion(Base):
    __tablename__ = "promotions"

    id = Column(Integer, primary_key=True)
    date = Column(Date)
    shop = Column(String)
    product_name = Column(String)
    producer = Column(String)
    category = Column(String)
    category_second = Column(String)
    product_url = Column(String)
    price_promo = Column(Float)
    price_old = Column(Float)
    price_omnibus = Column(Float)
    promo_code = Column(String)

    __table_args__ = (
        UniqueConstraint("date", "shop", "product_name", "price_omnibus", name = "unique_row"),
    )