from db_adapters import PostgresAdapter, mongo_conn
from utils.helpers import get_filters

sort_dict = {
            "id": "id",
            None: "prev_price_diff ASC",
            "prev_price_diff_increase": "prev_perc_diff ASC",
            "prev_price_diff_decrease": "prev_perc_diff DESC",
            "first_price_diff_increase": "first_perc_diff ASC",
            "first_price_diff_decrease": "first_perc_diff DESC",
            "price_increase": "price_units ASC",
            "price_decrease": "price_units DESC",
            "name_increase": "name ASC",
            "name_decrease": "name DESC",
        }



class PostgresInstrumentsManager:
    def __init__(self, limit, page, sort_type):
        self._psql = PostgresAdapter()
        self._limit = limit
        self._position = (page - 1) * limit
        self._sort_type = sort_type
        self._sort_dict = sort_dict


    def get(self):
        pass



class PostgresCurrenciesManager(PostgresInstrumentsManager):
    def __init__(self, limit, page, sort_type):
        super().__init__(limit, page, sort_type)


    def get(self):
        params = (self._limit, self._position)
        query = f"""
            WITH temp AS (
                SELECT 
                    currency_details.id,
                    instrument_id,
                    price_units,
                    price_nano,
                    name,
                    figi,

                    FIRST_VALUE(price_units) OVER (PARTITION BY instrument_id ORDER BY data) AS first_price,
                    COALESCE(price_units - FIRST_VALUE(price_units) OVER (PARTITION BY instrument_id ORDER BY data), 0) AS first_price_diff,

                    LAG(price_units) OVER (PARTITION BY instrument_id ORDER BY data) AS prev_price,
                    COALESCE(price_units - LAG(price_units) OVER (PARTITION BY instrument_id ORDER BY data), 0) AS prev_price_diff,

                    ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY data DESC) as criterion
                FROM
                    currency_details, instruments
                WHERE
                    currency_details.instrument_id = instruments.id)
            SELECT
                id,
                instrument_id,
                price_units,
                price_nano,
                first_price,
                first_price_diff,
                prev_price,
                prev_price_diff,
                name,
                figi,
                COALESCE(((100 * first_price_diff) / NULLIF(price_units, 0)), 0) AS first_perc_diff,
                COALESCE(((100 * prev_price_diff) / NULLIF(price_units, 0)), 0) AS prev_perc_diff
            FROM
                temp
            WHERE
                criterion = 1
            ORDER BY
                {self._sort_dict.get(self._sort_type)}
            LIMIT
                %s
            OFFSET
                %s;
        """

        self._psql.connect()
        data = self._psql.fetch_data(query, params)
        self._psql.disconnect()

        instruments_mas = []
        for element in data:
            obj = {"id": element[0],
                   "instrument_id": element[1],
                   "price_units": element[2],
                   "price_nano": element[3],
                   "first_price": element[4],
                   "first_price_diff": element[5],
                   "prev_price": element[6],
                   "prev_price_diff": element[7],
                   "name": element[8],
                   "figi": element[9]}
            instruments_mas.append(obj)

        return instruments_mas



class PostgresSharesManager(PostgresInstrumentsManager):
    def __init__(self, limit, page, sort_type, **kwargs):
        super().__init__(limit, page, sort_type)
        self._filters = get_filters(kwargs)


    def get(self):
        params = (self._limit, self._position)
        query = f"""
                    WITH temp AS (
                        SELECT
                            share_details.id,
                            instrument_id,
                            price_units,
                            price_nano,
                            nominal_units,
                            nominal_nano,
                            lot,
                            currency,
                            country_of_risk_name,
                            sector,
                            div_yield_flag,
                            name,
                            figi,

                            FIRST_VALUE(price_units) OVER (PARTITION BY instrument_id ORDER BY data) AS first_price,
                            COALESCE(price_units - FIRST_VALUE(price_units) OVER (PARTITION BY instrument_id ORDER BY data), 0) AS first_price_diff,

                            LAG(price_units) OVER (PARTITION BY instrument_id ORDER BY data) AS prev_price,
                            COALESCE(price_units - LAG(price_units) OVER (PARTITION BY instrument_id ORDER BY data), 0) AS prev_price_diff,

                            ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY data DESC) as criterion
                        FROM
                            share_details, instruments
                        WHERE
                            share_details.instrument_id = instruments.id)
                    SELECT 
                        id,
                        instrument_id,
                        price_units,
                        price_nano,
                        nominal_units,
                        nominal_nano,
                        lot,
                        currency,
                        country_of_risk_name,
                        sector,
                        div_yield_flag,

                        first_price,
                        first_price_diff,
                        prev_price,
                        prev_price_diff,
                        name,
                        figi,
                        
                        COALESCE(((100 * first_price_diff) / NULLIF(price_units, 0)), 0) AS first_perc_diff,
                        COALESCE(((100 * prev_price_diff) / NULLIF(price_units, 0)), 0) AS prev_perc_diff
                    FROM
                        temp
                    WHERE
                        criterion = 1 {self._filters}
                    ORDER BY
                        {self._sort_dict.get(self._sort_type)}
                    LIMIT
                        %s
                    OFFSET
                        %s;
                """

        self._psql.connect()
        data = self._psql.fetch_data(query, params)
        self._psql.disconnect()
        instruments_mas = []
        for element in data:
            obj = {"id": element[0],
                   "instrument_id": element[1],
                   "price_units": element[2],
                   "price_nano": element[3],
                   "nominal_units": element[4],
                   "nominal_nano": element[5],
                   "lot": element[6],
                   "currency": element[7],
                   "country_of_risk_name": element[8],
                   "sector": element[9],
                   "div_yield_flag": element[10],

                   "first_price": element[11],
                   "first_price_diff": element[12],
                   "prev_price": element[13],
                   "prev_price_diff": element[14],
                   "name": element[15],
                   "figi": element[16]}
            instruments_mas.append(obj)
        return instruments_mas



class PostgresEtfsManager(PostgresInstrumentsManager):
    def __init__(self, limit, page, sort_type, **kwargs):
        super().__init__(limit, page, sort_type)
        self._filters = get_filters(kwargs)


    def get(self):
        params = (self._limit, self._position)
        query = f"""
                    WITH temp AS (
                        SELECT
                            etf_details.id,
                            instrument_id,
                            price_units,
                            price_nano,
                            lot,
                            currency,
                            country_of_risk_name,
                            fixed_commission_units,
                            fixed_commission_nano,
                            focus_type,
                            num_shares,
                            sector,
                            name,
                            figi,

                            FIRST_VALUE(price_units) OVER (PARTITION BY instrument_id ORDER BY data) AS first_price,
                            COALESCE(price_units - FIRST_VALUE(price_units) OVER (PARTITION BY instrument_id ORDER BY data), 0) AS first_price_diff,

                            LAG(price_units) OVER (PARTITION BY instrument_id ORDER BY data) AS prev_price,
                            COALESCE(price_units - LAG(price_units) OVER (PARTITION BY instrument_id ORDER BY data), 0) AS prev_price_diff,

                            ROW_NUMBER() OVER (PARTITION BY instrument_id ORDER BY data DESC) as criterion
                        FROM
                            etf_details, instruments
                        WHERE
                            etf_details.instrument_id = instruments.id)
                    SELECT 
                        id,
                        instrument_id,
                        price_units,
                        price_nano,
                        lot,
                        currency,
                        country_of_risk_name,
                        fixed_commission_units,
                        fixed_commission_nano,
                        focus_type,
                        num_shares,
                        sector,

                        first_price,
                        first_price_diff,
                        prev_price,
                        prev_price_diff,
                        name,
                        figi,
                        
                        COALESCE(((100 * first_price_diff) / NULLIF(price_units, 0)), 0) AS first_perc_diff,
                        COALESCE(((100 * prev_price_diff) / NULLIF(price_units, 0)), 0) AS prev_perc_diff
                    FROM
                        temp
                    WHERE
                        criterion = 1 {self._filters}
                    ORDER BY
                        {self._sort_dict.get(self._sort_type)}
                    LIMIT
                        %s
                    OFFSET
                        %s;
                """

        self._psql.connect()
        data = self._psql.fetch_data(query, params)
        self._psql.disconnect()
        instruments_mas = []
        for element in data:
            obj = {"id": element[0],
                   "instrument_id": element[1],
                   "price_units": element[2],
                   "price_nano": element[3],
                   "lot": element[4],
                   "currency": element[5],
                   "country_of_risk_name": element[6],
                   "fixed_commission_units": element[7],
                   "fixed_commission_nano": element[8],
                   "focus_type": element[9],
                   "num_shares": element[10],
                   "sector": element[11],

                   "first_price": element[12],
                   "first_price_diff": element[13],
                   "prev_price": element[14],
                   "prev_price_diff": element[15],
                   "name": element[16],
                   "figi": element[17]}
            instruments_mas.append(obj)
        return instruments_mas



class PostgresSearchManager:
    def __init__(self, keyword, limit):
        self._psql = PostgresAdapter()
        self._keyword = '%' + keyword + '%'
        self._limit = limit

    def get(self):
        params = (self._keyword, self._keyword, self._limit)
        query = f"""
            SELECT
                id,
                name,
                figi,
                instrument_name_id
            FROM
                instruments
            WHERE
                name ILIKE %s OR figi ILIKE %s
            LIMIT
                %s;
        """

        self._psql.connect()
        data = self._psql.fetch_data(query, params)
        self._psql.disconnect()

        instruments_mas = []
        for element in data:
            obj = {"id": element[0],
                   "name": element[1],
                   "figi": element[2],
                   "instrument_name_id": element[3]}
            instruments_mas.append(obj)

        return instruments_mas



class MongoInstrumentsManager:
    def __init__(self, figi):
        self._db = mongo_conn()
        self._figi = figi


    def get_currency(self):
        collection = self._db.currencies
        temp = collection.find_one({"figi": self._figi})
        temp.pop("_id", None)
        return temp


    def get_share(self):
        collection = self._db.shares
        temp = collection.find_one({"figi": self._figi})
        temp.pop("_id", None)
        return temp


    def get_etf(self):
        collection = self._db.etfs
        temp = collection.find_one({"figi": self._figi})
        temp.pop("_id", None)
        return temp