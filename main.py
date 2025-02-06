from fastapi import FastAPI
import uvicorn

from config import SERVER_PORT, SERVER_HOST, SERVER_LOG_LEVEL
from instruments_managers import PostgresInstrumentsManager

app = FastAPI()


@app.get("/get_currencies")
async def get_currencies(limit: int = 15, page: int = 1, sort_type: str = None):
    #validation(instrument_name, position, limit, filter_type)
    instruments_mas = PostgresInstrumentsManager(limit = limit, page = page, sort_type = sort_type).get_currencies()
    return instruments_mas


@app.get("/get_shares")
async def get_shares(limit: int = 15, page: int = 1, sort_type: str = None, currency: str = None):
    #validation(instrument_name, position, limit, filter_type)
    instruments_mas = PostgresInstrumentsManager(limit = limit, page = page, sort_type = sort_type).get_shares()
    return instruments_mas



if __name__ == "__main__":
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT, log_level=SERVER_LOG_LEVEL)