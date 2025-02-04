from fastapi import FastAPI
import uvicorn

from config import SERVER_PORT, SERVER_HOST, SERVER_LOG_LEVEL
from instruments_managers import PostgresInstrumentsManager

app = FastAPI()


@app.get("/get_currencies")
async def root(limit: int = 15, page: int = 1, sort_type: str = None):
    #validation(instrument_name, position, limit, filter_type)
    currencies_mas = PostgresInstrumentsManager(limit = limit, page = page, sort_type = sort_type).get_currencies()
    return currencies_mas


@app.get("/set/{value}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}



if __name__ == "__main__":
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT, log_level=SERVER_LOG_LEVEL)