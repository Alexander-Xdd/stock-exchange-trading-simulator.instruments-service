from db_adapters import PostgresAdapter


class PostgresInstrumentsManager:
    def __init__(self, limit, page, sort_type):
        self.__psql = PostgresAdapter()
        self.__limit = limit
        self.__position = (page - 1) * limit
        self.__sort_type = sort_type
        self.__sort_dict = {
            None: "prev_price_diff ASC",
            "prev_price_diff_increase": "prev_price_diff ASC",
            "prev_price_diff_decrease": "prev_price_diff DESC",
            "first_price_diff_increase": "first_price_diff ASC",
            "first_price_diff_decrease": "first_price_diff DESC",
            "price_increase": "id ASC",
            "price_decrease": "id DESC",
            "name_increase": "name ASC",
            "name_decrease": "name DESC",
        }


    def get_currencies(self):
        params = (self.__limit, self.__position)
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
                figi
            FROM
                temp
            WHERE
                criterion = 1
            ORDER BY
                {self.__sort_dict.get(self.__sort_type)}
            LIMIT
                %s
            OFFSET
                %s;
        """

        self.__psql.connect()
        data = self.__psql.fetch_data(query, params)
        self.__psql.disconnect()

        currencies_mas = []
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
            currencies_mas.append(obj)

        return currencies_mas