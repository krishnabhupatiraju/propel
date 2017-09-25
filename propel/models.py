from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Connection(Base):
    __tablename__ = 'connections'
    id = Column(Integer(), primary_key=True)
    conn_name = Column(String(255), nullable=False)
    conn_type = Column(String(255))
    key = Column(String(255))
    secret = Column(String(255))
    token = Column(String(255))
    created_at = Column(DateTime(), default = datetime.now)
    updated_on = Column(DateTime(), 
                        default = datetime.now, 
                        onupdate = datetime.now)
    