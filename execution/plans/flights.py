from plan_json_constructor import *
import json
import duckdb
import os

pvd_base = os.environ["PVD_BASE"]

table = Table(f"flight")

colname = ["dep_delay", "arr_delay", "air_time", "distance", "dep_time", "arr_time"]

col = [C(f"flight", name) for name in colname]
bin_col = [C("", f"bin_{name}") for name in colname]
lower = [Val(f"lower_{name}", Func("domain", [c])) for name, c in zip(colname, col)]
upper = [Val(f"upper_{name}", Func("domain", [c])) for name, c in zip(colname, col)]
named = [Named(name, c) for name, c in zip(colname, col)]
named_bin = [Named(f"bin_{name}", c) for name, c in zip(colname, bin_col)]
bin_size = [10, 10, 20, 100, 1, 1]

table = table\
    .aggregate([Named(colname[i], Func("int", [col[i]])) for i in range(len(colname))] +
                [Named(f"bin_{colname[i]}", Func("int", [col[i] / V(bin_size[i])])) for i in range(len(colname))],
                [Named("count", Func("count", []))])\
    .cloud()\
    .scache()

def bar_interact_bar_template(show, filter, mode):
    show_idx = colname.index(show)
    filter_idx = colname.index(filter)
    filter_cond = None
    for i in range(len(colname)):
        if i != filter_idx and i != show_idx:
            if filter_cond is None:
                filter_cond = (col[i] >= lower[i]) & (col[i] <= upper[i])
            else:
                filter_cond = filter_cond & (col[i] >= lower[i]) & (col[i] <= upper[i])
    if mode == "build_server_query_server":
        return table\
                .filter(filter_cond)\
                .prefixsum_build(named[filter_idx], named_bin[show_idx], Named("sum", C("", "count")))\
                .dcache()\
                .prefixsum_query(lower[filter_idx], upper[filter_idx])\
                .network()
    else:
        return table\
                .filter(filter_cond)\
                .prefixsum_build(named[filter_idx], named_bin[show_idx], Named("sum", C("", "count")))\
                .network()\
                .dcache()\
                .prefixsum_query(lower[filter_idx], upper[filter_idx])
        
def bar_interact_2d_bar_template(show):
    show_idx = colname.index(show)
    filter_idx = [0, 1]
    filter_cond = None
    for i in range(len(colname)):
        if i not in filter_idx and i != show_idx:
            if filter_cond is None:
                filter_cond = (col[i] >= lower[i]) & (col[i] <= upper[i])
            else:
                filter_cond = filter_cond & (col[i] >= lower[i]) & (col[i] <= upper[i])
    return table\
            .filter(filter_cond)\
            .prefixsum2d_build(named[0], named[1], named_bin[show_idx], Named("sum", C("", "count")))\
            .dcache()\
            .prefixsum2d_query(lower[0], upper[0], lower[1], upper[1])\
            .network()

plans = {}
for mode in ["build_server_query_server", "build_server_query_client"]:
    for show in ["air_time", "distance", "dep_time", "arr_time"]:
        for filter in ["air_time", "distance", "dep_time", "arr_time"]:
            if show == filter: continue
            plans[f"{show}-{filter}-{mode}"] = bar_interact_bar_template(show, filter, mode).to_json()
for show in ["air_time", "distance", "dep_time", "arr_time"]:
    plans[f"{show}-delay-server"] = bar_interact_2d_bar_template(show).to_json()

conn = duckdb.connect(os.path.join(pvd_base, 'data/pvd.db'))

with open(f"flights.js", "w") as f:
    time_domain = list(range(0, 25))
    airtime_domain = random.sample(list(range(0, 500)), 100)
    distance_domain = random.sample(list(range(0, 4000)), 100)
    delay_domain = list(range(-20, 60))   

    values = {
        lower[0].id: [ {"type": "Int", "value": v} for v in delay_domain],
        upper[0].id: [ {"type": "Int", "value": v} for v in delay_domain],
        lower[1].id: [ {"type": "Int", "value": v} for v in delay_domain],
        upper[1].id: [ {"type": "Int", "value": v} for v in delay_domain],
        lower[2].id: [ {"type": "Int", "value": v} for v in airtime_domain],
        upper[2].id: [ {"type": "Int", "value": v} for v in airtime_domain],
        lower[3].id: [ {"type": "Int", "value": v} for v in distance_domain],
        upper[3].id: [ {"type": "Int", "value": v} for v in distance_domain],
        lower[4].id: [ {"type": "Int", "value": v} for v in time_domain],
        upper[4].id: [ {"type": "Int", "value": v} for v in time_domain],
        lower[5].id: [ {"type": "Int", "value": v} for v in time_domain],
        upper[5].id: [ {"type": "Int", "value": v} for v in time_domain],
    }

    tasks = {}
    for mode in ["build_server_query_server", "build_server_query_client"]:
        for show in ["air_time", "distance", "dep_time", "arr_time"]:
            for filter in ["air_time", "distance", "dep_time", "arr_time"]:
                if show == filter: continue
                filter_idx = colname.index(filter)
                tasks[f"{show}-{filter}-{mode}"] = [[lower[filter_idx].id, upper[filter_idx].id]]

    for show in ["air_time", "distance", "dep_time", "arr_time"]:
        tasks[f"{show}-delay-server"] = [[lower[0].id, upper[0].id], [lower[1].id, upper[1].id]]

    f.write("var flights_plans = " + json.dumps(plans) + ";\n")
    f.write("var flights_values = " + json.dumps(values) + ";\n")
    f.write("var flights_tasks = " + json.dumps(tasks) + ";\n")
    f.write(f"register_tasks(\"flights\", flights_plans, flights_values, flights_tasks);")

conn.close()
