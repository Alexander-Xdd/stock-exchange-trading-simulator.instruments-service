filter_currency_dict = {
    None: None,
    "RUB": "rub",
    "USD": "usd",
    "CNY": "cny"
}
filter_country_dict = {
    None: None,
    "USA": "Соединенные Штаты Америки",
    "RU": "Российская Федерация",
    "CN": "Китайская Народная Республика",

}
filter_sector_dict = {

}
filter_div_dict = {
    None: None,
    1: "True",
    0: "False"
}


def get_filters(kwargs):
    srt = ""
    for key, value in kwargs.items():
        if value is None:
            continue
        else:
            if key == "currency":
                srt += f" AND {key} = '{filter_currency_dict.get(value)}'"
            elif key == "country_of_risk_name":
                srt += f" AND {key} = '{filter_country_dict.get(value)}'"
            elif key == "div_yield_flag":
                srt += f" AND {key} = '{filter_div_dict.get(value)}'"
    print(srt)
    return srt