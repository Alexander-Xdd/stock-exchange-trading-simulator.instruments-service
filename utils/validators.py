from fastapi import HTTPException

from instruments_managers import sort_dict
from utils.helpers import filter_currency_dict, filter_country_dict, filter_div_dict


def validation_limit(limit):
    if not 1 <= limit <= 100:
        raise HTTPException(status_code=404, detail="Invalid limit")


def validation_page(page):
    if not 1 <= page < 1000:
        raise HTTPException(status_code=404, detail="Invalid page")


def validation_sort_type(sort_type):
    if sort_type not in sort_dict.keys():
        raise HTTPException(status_code=404, detail="Invalid sort_type")


def validation_filter_currency(filter_currency):
    if filter_currency not in filter_currency_dict.keys():
        raise HTTPException(status_code=404, detail="Invalid filter_currency")


def validation_filter_country(filter_country):
    if filter_country not in filter_country_dict.keys():
        raise HTTPException(status_code=404, detail="Invalid filter_country")


def validation_filter_div(filter_div):
    if filter_div not in filter_div_dict.keys():
        raise HTTPException(status_code=404, detail="Invalid filter_div")


def validation_currencies(limit, page, sort_type):
    validation_limit(limit)
    validation_page(page)
    validation_sort_type(sort_type)


def validation_shares(limit, page, sort_type, filter_currency, filter_country, filter_div):
    validation_limit(limit)
    validation_page(page)
    validation_sort_type(sort_type)
    validation_filter_currency(filter_currency)
    validation_filter_country(filter_country)
    validation_filter_div(filter_div)


def validation_etfs(limit, page, sort_type, filter_currency, filter_country):
    validation_limit(limit)
    validation_page(page)
    validation_sort_type(sort_type)
    validation_filter_currency(filter_currency)
    validation_filter_country(filter_country)
