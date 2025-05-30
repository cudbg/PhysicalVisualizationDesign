from plan_json_constructor import *
import json
import duckdb
import os

pvd_base = os.environ["PVD_BASE"]

table = Table(f"covid")
date = C(f"covid", "date")
county = C(f"covid", "county")
cases = C(f"covid", "cases")
deaths = C(f"covid", "deaths")

n_date = Named("date", date)
n_county = Named("county", county)
n_cases = Named("cases", cases)
n_deaths = Named("deaths", deaths)

c_county = Val("choose_county", Func("domain", [county]))
date_lower = Val("date_lower", Func("domain", [date]))
date_upper = Val("date_upper", Func("domain", [date]))

client_plan = AnyPlan(random_id(), "case_or_death", [
    table.aggregate([n_date, n_county], [Named("cases", Func("sum", [cases]))])\
        .cloud()\
        .hashtable_build([county])\
        .network()\
        .scache()\
        .hashtable_query([c_county])\
        .filter((date >= date_lower) & (date <= date_upper))
        .project([n_date, n_cases]),
    table.aggregate([n_date, n_county], [Named("deaths", Func("sum", [deaths]))])\
        .cloud()\
        .hashtable_build([county])\
        .network()\
        .scache()\
        .hashtable_query([c_county])\
        .filter((date >= date_lower) & (date <= date_upper))
        .project([n_date, n_deaths]),
])

server_plan = AnyPlan(random_id(), "case_or_death", [
    table.aggregate([n_date, n_county], [Named("cases", Func("sum", [cases]))])\
        .cloud()\
        .hashtable_build([county])\
        .scache()\
        .hashtable_query([c_county])\
        .filter((date >= date_lower) & (date <= date_upper))
        .project([n_date, n_cases])\
        .network(),
    table.aggregate([n_date, n_county], [Named("deaths", Func("sum", [deaths]))])\
        .cloud()\
        .hashtable_build([county])\
        .scache()\
        .hashtable_query([c_county])\
        .filter((date >= date_lower) & (date <= date_upper))
        .project([n_date, n_deaths])\
        .network()
])

plans = {
    "--server": server_plan.to_json(),
    #"--client": client_plan.to_json(),
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

with open(f"covid.js", "w") as f:
    county_domain = conn.sql(f"SELECT DISTINCT county FROM covid").fetchall()
    date_domain = conn.sql(f"SELECT DISTINCT date FROM covid").fetchall()

    values = {
        c_county.id: [ {"type": get_type(row[0]), "value": row[0]} for row in county_domain ],
        date_lower.id: [ {"type": get_type(row[0]), "value": row[0]} for row in date_domain ],
        date_upper.id: [ {"type": get_type(row[0]), "value": row[0]} for row in date_domain ],
        "case_or_death": [ {"type": "Index", "value": 0}, {"type": "Index", "value": 1} ]
    }

    tasks = {
        "--server": ["case_or_death", c_county.id, [date_lower.id, date_upper.id]],
        "--client": ["case_or_death", c_county.id, [date_lower.id, date_upper.id]],
    }

    f.write("var covid_plans = " + json.dumps(plans) + ";\n")
    f.write("var covid_values = " + json.dumps(values) + ";\n")
    f.write("var covid_tasks = " + json.dumps(tasks) + ";\n")
    f.write(f"register_tasks(\"covid\", covid_plans, covid_values, covid_tasks);")

conn.close()
