from typing import List

import databases
from sqlalchemy import *
from fastapi import FastAPI
from pydantic import BaseModel

import uvicorn
from datetime import datetime

from sqlalchemy import create_engine


if __name__ == "__main__":
    uvicorn.run("hw1:app", port=8000, reload=True)

DATABASE_URL = "sqlite:///./hw1.db"
# async driver sqlite+aiosqlite

database = databases.Database(DATABASE_URL)
metadata = MetaData()

stores = Table(
    "stores",
    metadata,
    Column("id", Integer, primary_key=True,autoincrement=True),
    Column("address",VARCHAR)  
)

items = Table(
    "items",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name",VARCHAR,unique=True),
    Column("price", FLOAT)
)

sales = Table(
    "sales",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("sale_time", DateTime,default=datetime.now),
    Column("store_id", Integer, ForeignKey("stores.id")),
    Column("item_id", Integer, ForeignKey("items.id"))
)


# создание таблиц
engine = create_engine(
    DATABASE_URL, connect_args={"check_same_thread": False})

metadata.create_all(engine)

class SaleIn(BaseModel):
    store_id: int
    item_id: int  
    sale_time = datetime

class Sale(BaseModel):
    id: int
    store_id: int
    item_id: int  
    sale_time = datetime


class StoreIn(BaseModel):
    address: str

class Store(BaseModel):
    id: int
    address: str

              
class ItemIn(BaseModel):
    name: str
    price: float

class Item(BaseModel):
    id: int
    name: str
    price: float


class Store10(BaseModel):
    idstore:int
    address:str
    total_sum:float

class Item10(BaseModel):
    id: int
    name: str
    all_sale:int



app = FastAPI(title="Домашнее задание 1")

@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

@app.get("/items/", response_model=List[Item],description="Получить все товары")
async def read_items():
    query = items.select()
    return await database.fetch_all(query)

@app.get("/stores/", response_model=List[Store],description="Получить все магазины")
async def read_stores():
    query = stores.select()
    return await database.fetch_all(query)

@app.post("/sales/", response_model=Sale,description="Добавить продажу товара в БД")
async def create_sale(sale: SaleIn):
    query = sales.insert().values(sale_time=datetime.now(), item_id=sale.item_id, store_id=sale.store_id)
    last_record_id = await database.execute(query)
    return {**sale.dict(), "id": last_record_id} 

@app.get("/top-10 of stores/",response_model=List[Store10], description="Топ10 магазинов")
async def get10_stores():
    query =f"""
        select
            stores.id as idstore,
            stores.address as address,
            sum(items.price) as total_sum
        from sales
            join
                stores
                on sales.store_id=stores.id 
            join
                items
                on sales.item_id=items.id
        where sales.sale_time>=date('now','-1 month')
        group by idstore, address
        order by total_sum desc limit 10
        """
      
    print(query)
    return await database.fetch_all(query)

@app.get("/top-10 of items/", response_model=List[Item10], description="Топ10 товаров")
async def get10_items():
    query = """       
        SELECT items.id, items.name, COUNT(sales.id) AS all_sale
        FROM sales, items
        WHERE items.id = sales.item_id
        GROUP BY items.id
        ORDER BY all_sale DESC
        LIMIT 10
        """
    print(query)
    return await database.fetch_all(query)

@app.post("/db_record/create_store", response_model=Store,description="Добавить магазин БД")
async def create_store(store: StoreIn):
    query = stores.insert().values(address=store.address)
    last_record_id = await database.execute(query)
    return {**store.dict(), "id": last_record_id} 

@app.post("/db_record/create_item", response_model=Item,description="Добавить товар в БД")
async def create_store(item: ItemIn):
    query = items.insert().values(name=item.name, price=item.price)
    last_record_id = await database.execute(query)
    return {**item.dict(), "id": last_record_id} 