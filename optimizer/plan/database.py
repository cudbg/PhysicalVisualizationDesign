import os
import duckdb

from plan import expressions


class Statistics:
    def __init__(self, avg_card: float, upper_card: float, n_cols: int) -> None:
        self.avg_card = avg_card
        self.upper_card = upper_card
        self.n_cols = n_cols

    def __str__(self) -> str:
        return f"STAT[avg_card={self.avg_card}; upper_card={self.upper_card}; n_cols={self.n_cols}]"


class TestCloud:
    def __init__(self):
        self.first = False
        pvd_base = os.environ["PVD_BASE"]
        self.db = duckdb.connect(os.path.join(pvd_base, 'data/pvd.db'), read_only=True)
        self.cache_statistics = {}
        self.cache_schema = {}
        self.cache_query = {}

    def query_statistics(self, query) -> Statistics:
        if query not in self.cache_statistics:
            result = self.db.execute(f"SELECT count(*) FROM ({query})").fetchall()
            n_rows = result[0][0]
            stat = Statistics(n_rows, n_rows, 0)
            self.cache_statistics[query] = stat
        return self.cache_statistics[query]

    def query(self, query):
        if query in self.cache_query:
            return self.cache_query[query]
        try:
            result = self.db.execute(query).fetchall()
            self.cache_query[query] = result[0][0]
            return result[0][0]
        except:
            self.cache_query[query] = None
            raise RuntimeError("Query failed: " + query)

    def schema(self, table):
        if table not in self.cache_schema:
            result = self.db.query(f"DESCRIBE " + table).fetchall()
            schema = []
            for c in result:
                schema.append(expressions.C(table, c[0]))
            self.cache_schema[table] = schema
        return self.cache_schema[table]

database = TestCloud()

column_type = {
    'Address': "string",
    'Bottle Volume (ml)': "number",
    'Bottles Sold': "number",
    'Category': "number",
    'Category Name': "string",
    'City': "string",
    'County': "string",
    'County Number': "number",
    'Date': "string",
    'Invoice/Item Number': "number",
    'Item Description': "string",
    'Item Number': "number",
    'Pack': "number",
    'Sale (Dollars)': "number",
    'Sales': "number",
    'State Bottle Cost': "number",
    'State Bottle Retail': "number",
    'Store Location': "string",
    'Store Name': "string",
    'Store Number': "number",
    'Vendor Name': "string",
    'Vendor Number': "number",
    'Volume Sold (Gallons)': "number",
    'Volume Sold (Liters)': "number",
    'bin_air_time': "number",
    'bin_arr_delay': "number",
    'bin_arr_time': "number",
    'bin_dep_delay': "number",
    'bin_dep_time': "number",
    'bin_distance': "number",
    'cases': "number",
    'count': "number",
    'state': "string",
    'fips': "number",
    'count_n': "number",
    'country': "strintg",
    'county': "string",
    'cx': "number",
    'cy': "number",
    'cz': "number",
    'date': "string",
    'day': "number",
    'deaths': "number",
    'dec': "number",
    'hour': "number",
    'latitude': "number",
    'longitude': "number",
    'month': "number",
    'ra': "number",
    'temperature_celsius': "number",
    'total': "number",
    'location_name': "string",
    'timezone': "string",
    'last_updated_epoch': "number",
    'last_updated': "string",
    'temperature_fahrenheit': "number",
    'condition_text': "string",
    'wind_mph': "number",
    'wind_kph': "number",
    'wind_degree': "number",
    'wind_direction': "string",
    'pressure_mb': "number",
    'pressure_in': "number",
    'precip_mm': "number",
    'precip_in': "number",
    'humidity': "number",
    'cloud': "number",
    'feels_like_celsius': "number",
    'feels_like_fahrenheit': "number",
    'visibility_km': "number",
    'visibility_miles': "number",
    'uv_index': "number",
    'gust_mph': "number",
    'gust_kph': "number",
    'air_quality_Carbon_Monoxide': "number",
    'air_quality_Ozone': "number",
    'air_quality_Nitrogen_dioxide': "number",
    'air_quality_Sulphur_dioxide': "number",
    'air_quality_PM2.5': "number",
    'air_quality_PM10': "number",
    'air_quality_us-epa-index': "number",
    'air_quality_gb-defra-index': "number",
    'sunrise': "string",
    'sunset': "string",
    'moonrise': "string",
    'moonset': "string",
    'moon_phase': "string",
    'moon_illumination': "number",
    'user': "number",
    'checkin_time': "string",
    'location_id': "string",
    'fl_date': "string",
    'FL_DATE': "string",
    'dep_delay': "number",
    'DEP_DELAY': "number",
    'arr_delay': "number",
    'ARR_DELAY': "number",
    'air_time': "number",
    'AIR_TIME': "number",
    'distance': "number",
    'DISTANCE': "number",
    'dep_time': "number",
    'DEP_TIME': "number",
    'arr_time': "number",
    'ARR_TIME': "number",
}
