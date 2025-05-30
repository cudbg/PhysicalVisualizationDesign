from plan_json_constructor import *
import json
import duckdb
import os

pvd_base = os.environ["PVD_BASE"]

table = Table(f"sdss")
ra = C(f"sdss", "ra")
dec = C(f"sdss", "dec")
cx = C(f"sdss", "cx")
cy = C(f"sdss", "cy")
cz = C(f"sdss", "cz")

n_ra = Named("ra", ra)
n_dec = Named("dec", dec)
n_cx = Named("cx", cx)
n_cy = Named("cy", cy)
n_cz = Named("cz", cz)

ra_lower = Val("ra_lower", Func("domain", [ra]))
ra_upper = Val("ra_upper", Func("domain", [ra]))
dec_lower = Val("dec_lower", Func("domain", [dec]))
dec_upper = Val("dec_upper", Func("domain", [dec]))

rtree_plan = table\
        .project([n_ra, n_dec])\
        .cloud()\
        .rtree_build([ra, dec])\
        .scache()\
        .rtree_query([ra_lower, dec_lower], [ra_upper, dec_upper])\
        .network()

query_plan = table\
        .project([n_ra, n_dec])\
        .cloud()\
        .scache()\
        .filter((ra >= ra_lower) & (ra <= ra_upper) & (dec >= dec_lower) & (dec <= dec_upper))\
        .network()

plans = {
    "--rtree": rtree_plan.to_json(),
    "--query": query_plan.to_json(),
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

with open(f"sdss.js", "w") as f:
    var_ranges = conn.sql(f"SELECT min(ra), max(ra), min(dec), max(dec) FROM sdss").fetchall()[0]

    values = {
        ra_lower.id: [{"type": "Float", "value": random.uniform(var_ranges[0], var_ranges[1])} for _ in range(100)],
        ra_upper.id: [{"type": "Float", "value": random.uniform(var_ranges[0], var_ranges[1])} for _ in range(100)],
        dec_lower.id: [{"type": "Float", "value": random.uniform(var_ranges[2], var_ranges[3])} for _ in range(100)],
        dec_upper.id: [{"type": "Float", "value": random.uniform(var_ranges[2], var_ranges[3])} for _ in range(100)]
    }

    tasks = {
        "--rtree": [[ra_lower.id, ra_upper.id], [dec_lower.id, dec_upper.id]],
        "--query": [[ra_lower.id, ra_upper.id], [dec_lower.id, dec_upper.id]]
    }

    f.write("var sdss_plans = " + json.dumps(plans) + ";\n")
    f.write("var sdss_values = " + json.dumps(values) + ";\n")
    f.write("var sdss_tasks = " + json.dumps(tasks) + ";\n")
    f.write(f"register_tasks(\"sdss\", sdss_plans, sdss_values, sdss_tasks);")

conn.close()
