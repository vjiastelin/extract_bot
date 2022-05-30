from sqlalchemy import Column, Sequence
from sqlalchemy import Integer,Date,String,Numeric,Boolean
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class AsicsList(Base):
    __tablename__ = 'asics_list'
    __table_args__ = {'schema': 'asics'}

    name = Column(String(128), primary_key=True)
    series = Column(String(64))
    model = Column(String(64))
    hashrate = Column(Integer)
    efficiency = Column(Integer)
    

    def __repr__(self):
        return f"asics_list(name={self.name!r}, series={self.series!r}, model={self.model!r})"

class GpuPrices(Base):
    __tablename__ = 'gpu_prices'
    __table_args__ = {'schema': 'asics'}

    price_date = Column(Date(), primary_key=True)
    #id = Column(Integer,  Sequence('asic_seq'), primary_key=True)
    gpu_name_raw = Column(String(64), primary_key=True)
    price_usd = Column(Numeric)

    def __repr__(self):
        return f"gpu_prices(price_date={self.price_date!r}, asic_name_raw={self.gpu_name_raw!r}, price_rub={self.price_usd!r})"


class AsicsPrices(Base):
    __tablename__ = 'asics_prices'
    __table_args__ = {'schema': 'asics'}

    price_date = Column(Date(), primary_key=True)
    asic_name_raw = Column(String(128), primary_key=True)
    price_rub = Column(Numeric)
    price_cny = Column(Numeric)
    used_flag = Column(Boolean, primary_key=True)
    asic_name = Column(String(128))
    price_usd = Column(Numeric)    

    def __repr__(self):
        return f"asics_prices(price_date={self.price_date!r}, asic_name_raw={self.asic_name_raw!r}, price_rub={self.price_rub!r},used_flag={self.used_flag!r},asic_name={self.asic_name!r},price_usd={self.price_usd!r})"