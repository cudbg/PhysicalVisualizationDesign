from plan_json_constructor import *
import json
import duckdb
import os

pvd_base = os.environ["PVD_BASE"]

table = Table(f"brightkite")
month = C(f"brightkite", "month")
day = C(f"brightkite", "day")
hour = C(f"brightkite", "hour")
lat = C(f"brightkite", "latitude")
long = C(f"brightkite", "longitude")

n_month = Named("month", month)
n_day = Named("day", day)
n_hour = Named("hour", hour)
n_lat = Named("latitude", lat)
n_long = Named("longitude", long)
n_count = Named("count", C("", "count"))

lat_lower = Val("lat_lower", Func("domain", [lat]))
lat_upper = Val("lat_upper", Func("domain", [lat]))
long_lower = Val("long_lower", Func("domain", [long]))
long_upper = Val("long_upper", Func("domain", [long]))
c_month = Val("choose_month", Func("domain", [month]))
c_day = Val("choose_day", Func("domain", [day]))
c_hour = Val("choose_hour", Func("domain", [hour]))

table = table\
    .project([Named("latitude", Func("int", [lat * V(10)])), 
                Named("longitude", Func("int", [long * V(10)])),
                n_month, n_day, n_hour])

def map_plan_template(filter, mode):
    n_filter = { "month": n_month, "day": n_day, "hour": n_hour }[filter]
    key = { "month": month, "day": day, "hour": hour }[filter]
    query = { "month": c_month, "day": c_day, "hour": c_hour }[filter]
    if mode == "build_server_query_server":
        return table\
                .aggregate([n_lat, n_long, n_filter], [Named("count", Func("count", []))])\
                .cloud()\
                .hashtable_build([key])\
                .scache()\
                .hashtable_query([query])\
                .project([n_lat, n_long, n_count])\
                .network()
    elif mode == "build_server_query_client":
        return table\
                .aggregate([n_lat, n_long, n_filter], [Named("count", Func("count", []))])\
                .cloud()\
                .hashtable_build([key])\
                .network()\
                .scache()\
                .hashtable_query([query])\
                .project([n_lat, n_long, n_count])
    else:
        return table\
                .aggregate([n_lat, n_long, n_filter], [Named("count", Func("count", []))])\
                .cloud()\
                .network()\
                .hashtable_build([key])\
                .scache()\
                .hashtable_query([query])\
                .project([n_lat, n_long, n_count])

                
def bar_plan_interact_bar_template(show, filter, mode):
    n_show = { "month": n_month, "day": n_day, "hour": n_hour }[show]
    n_filter = { "month": n_month, "day": n_day, "hour": n_hour }[filter]
    key = { "month": month, "day": day, "hour": hour }[filter]
    query = { "month": c_month, "day": c_day, "hour": c_hour }[filter]
    if mode == "build_server_query_server":
        return table\
                .aggregate([n_show, n_filter], [Named("count", Func("count", []))])\
                .cloud()\
                .hashtable_build([key])\
                .scache()\
                .hashtable_query([query])\
                .project([n_show, n_count])\
                .network()
    elif mode == "build_server_query_client":
        return table\
                .aggregate([n_show, n_filter], [Named("count", Func("count", []))])\
                .cloud()\
                .hashtable_build([key])\
                .network()\
                .scache()\
                .hashtable_query([query])\
                .project([n_show, n_count])
    else:
        return table\
                .aggregate([n_show, n_filter], [Named("count", Func("count", []))])\
                .cloud()\
                .network()\
                .hashtable_build([key])\
                .scache()\
                .hashtable_query([query])\
                .project([n_show, n_count])


def bar_plan_interact_map_template(show, mode):
    n_show = { "month": n_month, "day": n_day, "hour": n_hour }[show]
    if mode == "build_server_query_server":
        return table\
                .aggregate([n_show, n_lat, n_long], [Named("count", Func("count", []))])\
                .cloud()\
                .prefixsum2d_build(n_lat, n_long, n_show, n_count)\
                .scache()\
                .prefixsum2d_query(lat_lower, lat_upper, long_lower, long_upper)\
                .network()
    elif mode == "build_server_query_client":
        return table\
                .aggregate([n_show, n_lat, n_long], [Named("count", Func("count", []))])\
                .cloud()\
                .prefixsum2d_build(n_lat, n_long, n_show, n_count)\
                .network()\
                .scache()\
                .prefixsum2d_query(lat_lower, lat_upper, long_lower, long_upper)
    else:
        return table\
                .aggregate([n_show, n_lat, n_long], [Named("count", Func("count", []))])\
                .cloud()\
                .network()\
                .prefixsum2d_build(n_lat, n_long, n_show, n_count)\
                .scache()\
                .prefixsum2d_query(lat_lower, lat_upper, long_lower, long_upper)

plans = {}
for mode in ["build_server_query_server", "build_server_query_client", "build_client_query_client"]:
    for filter in ["month", "day", "hour"]:
        plans[f"map-{filter}-{mode}"] = map_plan_template(filter, mode).to_json()
    for show in ["month", "day", "hour"]:
        for filter in ["month", "day", "hour"]:
            if show == filter: continue
            plans[f"{show}-{filter}-{mode}"] = bar_plan_interact_bar_template(show, filter, mode).to_json()
        if "client" not in mode:
            plans[f"{show}-map-{mode}"] = bar_plan_interact_map_template(show, mode).to_json()

def get_type(value):
    if isinstance(value, int):
        return "Int"
    if isinstance(value, str):
        return "String"
    if isinstance(value, float):
        return "Float"
    return "Unknown"

conn = duckdb.connect(os.path.join(pvd_base, 'data/pvd.db'))

with open(f"brightkite.js", "w") as f:
    month_domain = conn.sql(f"SELECT DISTINCT month FROM brightkite").fetchall()
    day_domain = conn.sql(f"SELECT DISTINCT day FROM brightkite").fetchall()
    hour_domain = conn.sql(f"SELECT DISTINCT hour FROM brightkite").fetchall()

    values = {
        c_day.id: [ {"type": get_type(row[0]), "value": row[0]} for row in day_domain ],
        c_month.id: [ {"type": get_type(row[0]), "value": row[0]} for row in month_domain ],
        c_hour.id: [ {"type": get_type(row[0]), "value": row[0]} for row in hour_domain ],
        lat_lower.id: [{"type": "Int", "value": random.randint(-900, 900)} for _ in range(100)],
        lat_upper.id: [{"type": "Int", "value": random.randint(-900, 900)} for _ in range(100)],
        long_lower.id: [{"type": "Int", "value": random.randint(-1800, 1800)} for _ in range(100)],
        long_upper.id: [{"type": "Int", "value": random.randint(-1800, 1800)} for _ in range(100)],
    }

    tasks = {}
    for mode in ["build_server_query_server", "build_server_query_client", "build_client_query_client"]:
        for filter in ["month", "day", "hour"]:
            filter_id = { "month": c_month.id, "day": c_day.id, "hour": c_hour.id }[filter]
            tasks[f"map-{filter}-{mode}"] = [filter_id]
        for show in ["month", "day", "hour"]:
            for filter in ["month", "day", "hour"]:
                if show == filter: continue
                filter_id = { "month": c_month.id, "day": c_day.id, "hour": c_hour.id }[filter]
                tasks[f"{show}-{filter}-{mode}"] = [filter_id]
            if "client" not in mode:
                tasks[f"{show}-map-{mode}"] = [[lat_lower.id, lat_upper.id], [long_lower.id, long_upper.id]]

    f.write("var brightkite_plans = " + json.dumps(plans) + ";\n")
    f.write("var brightkite_values = " + json.dumps(values) + ";\n")
    f.write("var brightkite_tasks = " + json.dumps(tasks) + ";\n")
    f.write(f"register_tasks(\"brightkite\", brightkite_plans, brightkite_values, brightkite_tasks);")

conn.close()
