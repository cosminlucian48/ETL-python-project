from fastapi import FastAPI

from lead.api.routes.query import query_route
from lead.api.routes.security import security_route
from lead.api.routes.spark_jobs import spark_route

app = FastAPI()

app.include_router(query_route, prefix='/api')
app.include_router(security_route, prefix='/api')
app.include_router(spark_route, prefix='/api')

@app.get("/")
async def read_root():
    return {"Hello": "World!"}
