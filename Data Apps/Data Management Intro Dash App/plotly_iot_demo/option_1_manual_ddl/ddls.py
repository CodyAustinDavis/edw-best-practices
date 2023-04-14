
import os
import json
from databricks import sql
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy import Column, String, Integer, BOOLEAN, create_engine, select, TIMESTAMP, DECIMAL, INTEGER, BIGINT, DATE

## Return new base class with the mapper initialized
Base = declarative_base()


####### Bronze Tables


class BronzeSensors(Base):

    __tablename__ = "bronze_sensors"

    Id = Column(BIGINT, primary_key=True)
    device_id = Column(INTEGER)
    user_id = Column(INTEGER)
    calories_burnt = Column(DECIMAL(10,2))
    miles_walked = Column(DECIMAL(10,2))
    num_steps = Column(DECIMAL(10,2))
    timestamp = Column(TIMESTAMP)
    value = Column(String(1024))


class BronzeUsers(Base):

    __tablename__ = "bronze_users"

    user_id = Column(BIGINT, primary_key=True)
    gender = Column(String(10))
    age = Column(INTEGER)
    height = Column(DECIMAL(10,2))
    weight = Column(DECIMAL(10,2))
    smoker = Column(String(4))
    familyhistory = Column(String(100))
    cholestlevs = Column(String(100))
    bp = Column(String(50))
    risk = Column(DECIMAL(10,2))
    update_timestamp = Column(TIMESTAMP)




####### Silver  Tables

class SilverSensors(Base):

    __tablename__ = "silver_sensors"

    Id = Column(BIGINT, primary_key=True)
    device_id = Column(INTEGER)
    user_id = Column(INTEGER)
    calories_burnt = Column(DECIMAL(10,2))
    miles_walked = Column(DECIMAL(10,2))
    num_steps = Column(DECIMAL(10,2))
    timestamp = Column(TIMESTAMP)
    value = Column(String(1024))



class SilverUsers(Base):

    __tablename__ = "silver_users"

    user_id = Column(BIGINT, primary_key=True)
    gender = Column(String(10))
    age = Column(INTEGER)
    height = Column(DECIMAL(10,2))
    weight = Column(DECIMAL(10,2))
    smoker = Column(String(4))
    familyhistory = Column(String(100))
    cholestlevs = Column(String(100))
    bp = Column(String(50))
    risk = Column(DECIMAL(10,2))
    update_timestamp = Column(TIMESTAMP)




