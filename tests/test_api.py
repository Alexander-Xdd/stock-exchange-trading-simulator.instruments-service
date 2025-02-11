import pytest

from main import get_currencies, get_shares, get_etfs


@pytest.mark.parametrize("limit, expected", [
    (2, 2),
    (10, 10),
])
def test_get_currencies(limit, expected):
    instruments_mas = get_currencies(limit=limit, sort_type="first_price_diff_decrease")
    assert len(instruments_mas) == expected
    assert (instruments_mas[0].get('first_price_diff') >= instruments_mas[1].get('first_price_diff')) == True


@pytest.mark.parametrize("limit, expected", [
    (2, 2),
    (10, 10),
    (50, 50),
])
def test_get_shares(limit, expected):
    instruments_mas = get_shares(limit=limit, sort_type="first_price_diff_decrease", filter_currency="USD")
    assert len(instruments_mas) == expected
    assert instruments_mas[0].get('first_price_diff') >= instruments_mas[1].get('first_price_diff')
    assert instruments_mas[0].get('currency') == instruments_mas[1].get('currency') == "usd"


@pytest.mark.parametrize("limit, expected", [
    (2, 2),
    (10, 10),
])
def test_get_shares(limit, expected):
    instruments_mas = get_etfs(limit=limit, sort_type="first_price_diff_decrease", filter_country="RU")
    assert len(instruments_mas) == expected
    assert instruments_mas[0].get('first_price_diff') >= instruments_mas[1].get('first_price_diff')
    assert instruments_mas[0].get('country_of_risk_name') == instruments_mas[1].get('country_of_risk_name') == "Российская Федерация"