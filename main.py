from fastapi import FastAPI, HTTPException
import uvicorn

from config import SERVER_PORT, SERVER_HOST, SERVER_LOG_LEVEL
from instruments_managers import PostgresCurrenciesManager, PostgresSharesManager, PostgresEtfsManager
from utils.validators import validation_currencies, validation_shares, validation_etfs

app = FastAPI()


@app.get("/get_currencies")
def get_currencies(limit: int = 15, page: int = 1, sort_type: str = None):
    validation_currencies(limit, page, sort_type)

    instruments_mas = PostgresCurrenciesManager(limit = limit, page = page, sort_type = sort_type).get()
    return instruments_mas


@app.get("/get_shares")
def get_shares(limit: int = 15, page: int = 1, sort_type: str = None,
               filter_currency: str = None, filter_country: str = None, filter_sector: str = None, filter_div: int = None):
    validation_shares(limit, page, sort_type, filter_currency, filter_country, filter_div)

    instruments_mas = PostgresSharesManager(limit, page, sort_type,
                                            currency = filter_currency, country_of_risk_name = filter_country,
                                            sector = filter_sector, div_yield_flag = filter_div).get()
    return instruments_mas


@app.get("/get_etfs")
def get_etfs(limit: int = 15, page: int = 1, sort_type: str = None,
               filter_currency: str = None, filter_country: str = None, filter_sector: str = None):
    validation_etfs(limit, page, sort_type, filter_currency, filter_country)

    instruments_mas = PostgresEtfsManager(limit, page, sort_type,
                                            currency = filter_currency, country_of_risk_name = filter_country,
                                            sector = filter_sector).get()
    return instruments_mas



if __name__ == "__main__":
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT, log_level=SERVER_LOG_LEVEL)