from plan_json_constructor import *
import json
import duckdb
import os

pvd_base = os.environ["PVD_BASE"]

NB = 100000000
for n in [0.01, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 1]:
    n = int(NB * n)
    table = Table(f"weather_{n}")
    date = C(f"weather_{n}", "date")
    country = C(f"weather_{n}", "country")
    temperature = C(f"weather_{n}", "temperature_celsius")

    n_date = Named("date", date)
    n_country = Named("country", country)
    n_temperature = Named("temperature_celsius", temperature)

    c_country = Val("choose_date", Func("domain", [country]))

    client_plan = table\
            .aggregate([n_date, n_country], [Named("temperature_celsius", Func("avg", [temperature]))])\
            .cloud()\
            .hashtable_build([country])\
            .network()\
            .scache()\
            .hashtable_query([c_country])\
            .project([n_date, n_temperature])

    server_plan = table\
            .aggregate([n_date, n_country], [Named("temperature_celsius", Func("avg", [temperature]))])\
            .cloud()\
            .hashtable_build([country])\
            .scache()\
            .hashtable_query([c_country])\
            .project([n_date, n_temperature])\
            .network()

    plans = {
        "--scache_server": server_plan.to_json()
    }

    def get_type(value):
        if isinstance(value, int):
            return "Int"
        if isinstance(value, str):
            return "String"
        if isinstance(value, float):
            return "Float"
        return "Unknown"

    conn = duckdb.connect(os.path.join(pvd_base, 'data/pvd.db'))

    with open(f"weather_{n}.js", "w") as f:
        country_domain = conn.sql(f"SELECT DISTINCT country FROM weather_{n}").fetchall()

        values = {
            c_country.id: [ {"type": get_type(row[0]), "value": row[0]} for row in country_domain ]
        }

        tasks = {
            "--scache_server": [c_country.id]
        }

        f.write("var weather_plans = " + json.dumps(plans) + ";\n")
        f.write("var weather_values = " + json.dumps(values) + ";\n")
        f.write("var weather_tasks = " + json.dumps(tasks) + ";\n")
        f.write(f"register_tasks(\"weather_{n}\", weather_plans, weather_values, weather_tasks);")

    conn.close()
