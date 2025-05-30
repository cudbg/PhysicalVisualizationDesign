from plan_json_constructor import *
import json
import duckdb
import os

pvd_base = os.environ["PVD_BASE"]

table = Table(f"liquor")
date = C(f"liquor", "Date")
sales = C(f"liquor", "Sales")
category = C(f"liquor", "Category")
county = C(f"liquor", "County")
pack = C(f"liquor", "Pack")
n_date = Named("Date", date)
n_sales = Named("Sales", sales)
n_category = Named("Category", category)
n_county = Named("County", county)
n_category = Named("Category", category)
n_pack = Named("Pack", pack)

c_date_lower = Val("date_lower", Func("domain", [date]))
c_date_upper = Val("date_upper", Func("domain", [date]))
c_category = Val("choose_category", Func("domain", [category]))
c_county = Val("choose_county", Func("domain", [county]))
c_pack = Val("choose_pack", Func("domain", [pack]))

agg_table = table.aggregate([n_date, n_category, n_county, n_pack], [Named("Sales", Func("sum", [sales]))]).cloud().scache()

def static_date_sales():
    return agg_table\
            .filter((category == c_category) & (county == c_county) & (pack == c_pack))\
            .aggregate([n_date], [Named("Sales", Func("sum", [sales]))])\
            .scache()\
            .network()

def static_categorical_sales_template(column):
    conds = {
        "category": (county == c_county) & (pack == c_pack),
        "county": (category == c_category) & (pack == c_pack),
        "pack": (category == c_category) & (county == c_county)
    }[column]

    n_column = { "category": n_category, "county": n_county, "pack": n_pack }[column]

    return agg_table\
            .filter(conds)\
            .prefixsum_build(n_date, n_column, n_sales)\
            .scache()\
            .prefixsum_query(c_date_lower, c_date_upper)\
            .network()

def dynamic_server_date_sales_interact_categorical_template(column):
    conds = {
        "category": (county == c_county) & (pack == c_pack),
        "county": (category == c_category) & (pack == c_pack),
        "pack": (category == c_category) & (county == c_county)
    }[column]

    n_column = { "category": n_category, "county": n_county, "pack": n_pack }[column]
    c_column = { "category": c_category, "county": c_county, "pack": c_pack }[column]
    column = { "category": category, "county": county, "pack": pack }[column]

    return agg_table\
            .filter(conds)\
            .aggregate([n_column, n_date], [Named("Sales", Func("sum", [sales]))])\
            .hashtable_build([column])\
            .dcache()\
            .hashtable_query([c_column])\
            .network()

def dynamic_server_categorical_sales_interact_date_template(column):
    conds = {
        "category": (county == c_county) & (pack == c_pack),
        "county": (category == c_category) & (pack == c_pack),
        "pack": (category == c_category) & (county == c_county)
    }[column]

    n_column = { "category": n_category, "county": n_county, "pack": n_pack }[column]

    return agg_table\
            .filter(conds)\
            .prefixsum_build(n_date, n_column, n_sales)\
            .dcache()\
            .prefixsum_query(c_date_lower, c_date_upper)\
            .network()

def dynamic_server_categorical_sales_interact_categorical_template(target_column, interact_column):
    third_column = list({ "category", "county", "pack" } - {target_column, interact_column })[0]

    column = { "category": category, "county": county, "pack": pack }
    c_column = { "category": c_category, "county": c_county, "pack": c_pack }
    n_column = { "category": n_category, "county": n_county, "pack": n_pack }

    conds = {
        "category": (category == c_category) & (date._between(c_date_lower, c_date_upper)),
        "county": (county == c_county) & (date._between(c_date_lower, c_date_upper)),
        "pack": (pack == c_pack) & (date._between(c_date_lower, c_date_upper))
    }[third_column]

    return agg_table\
            .filter(conds)\
            .aggregate([n_column[target_column], n_column[interact_column]], [Named("Sales", Func("sum", [sales]))])\
            .hashtable_build([column[interact_column]])\
            .dcache()\
            .hashtable_query([c_column[interact_column]])\
            .network()

def dynamic_client_date_sales_interact_categorical_template(column):
    conds = {
        "category": (county == c_county) & (pack == c_pack),
        "county": (category == c_category) & (pack == c_pack),
        "pack": (category == c_category) & (county == c_county)
    }[column]

    n_column = { "category": n_category, "county": n_county, "pack": n_pack }[column]
    c_column = { "category": c_category, "county": c_county, "pack": c_pack }[column]
    column = { "category": category, "county": county, "pack": pack }[column]

    return agg_table\
            .filter(conds)\
            .aggregate([n_column, n_date], [Named("Sales", Func("sum", [sales]))])\
            .hashtable_build([column])\
            .network()\
            .dcache()\
            .hashtable_query([c_column])

def dynamic_client_categorical_sales_interact_date_template(column):
    conds = {
        "category": (county == c_county) & (pack == c_pack),
        "county": (category == c_category) & (pack == c_pack),
        "pack": (category == c_category) & (county == c_county)
    }[column]

    n_column = { "category": n_category, "county": n_county, "pack": n_pack }[column]

    return agg_table\
            .filter(conds)\
            .prefixsum_build(n_date, n_column, n_sales)\
            .network()\
            .dcache()\
            .prefixsum_query(c_date_lower, c_date_upper)

def dynamic_client_categorical_sales_interact_categorical_template(target_column, interact_column):
    third_column = list({ "category", "county", "pack" } - {target_column, interact_column })[0]

    column = { "category": category, "county": county, "pack": pack }
    c_column = { "category": c_category, "county": c_county, "pack": c_pack }
    n_column = { "category": n_category, "county": n_county, "pack": n_pack }

    conds = {
        "category": (category == c_category) & (date._between(c_date_lower, c_date_upper)),
        "county": (county == c_county) & (date._between(c_date_lower, c_date_upper)),
        "pack": (pack == c_pack) & (date._between(c_date_lower, c_date_upper))
    }[third_column]

    return agg_table\
            .filter(conds)\
            .aggregate([n_column[target_column], n_column[interact_column]], [Named("Sales", Func("sum", [sales]))])\
            .hashtable_build([column[interact_column]])\
            .network()\
            .dcache()\
            .hashtable_query([c_column[interact_column]])

with open(f"liquor.js", "w") as f:

    plans = {}
    for col in ["category", "county", "pack"]:
        #plans[f"static_{col}_sales"] = static_categorical_sales_template(col)
        plans[f"date-{col}-server"] = dynamic_server_date_sales_interact_categorical_template(col)
        plans[f"date-{col}-client"] = dynamic_server_date_sales_interact_categorical_template(col)
        plans[f"{col}-date-server"] = dynamic_server_categorical_sales_interact_date_template(col)
        plans[f"{col}-date-client"] = dynamic_client_categorical_sales_interact_date_template(col)
        for col2 in ["category", "county", "pack"]:
            if col != col2:
                plans[f"{col}-{col2}-server"] = dynamic_server_categorical_sales_interact_categorical_template(col, col2)
                plans[f"{col}-{col2}-client"] = dynamic_client_categorical_sales_interact_categorical_template(col, col2)

    conn = duckdb.connect(os.path.join(pvd_base, 'data/pvd.db'))

    def get_type(value):
        if isinstance(value, int):
            return "Int"
        if isinstance(value, str):
            return "String"
        if isinstance(value, float):
            return "Float"
        return "Unknown"

    values = {}
    for column in ["Date", "Category", "County", "Pack"]:
        result = conn.sql(f"SELECT {column}, count(*) as CNT FROM liquor GROUP BY {column} ORDER BY CNT DESC LIMIT 30;").fetchall()
        for cid in {
            "Date": [c_date_lower.id, c_date_upper.id], 
            "Category": [c_category.id], 
            "County": [c_county.id], 
            "Pack": [c_pack.id]
        }[column]:
            values[cid] = [{"type": get_type(row[0]), "value": row[0]} for row in sorted(result, key=lambda x: x[0])]

    plans = {name: plan.to_json() for name, plan in plans.items()}

    tasks = {}
    for col in ["category", "county", "pack"]:
        choice = { "category": c_category, "county": c_county, "pack": c_pack }[col]
        #tasks[f"static_{col}_sales"] = [c_category.id, c_county.id, c_pack.id, c_date_lower.id, c_date_upper.id]
        tasks[f"date-{col}-server"] = [choice.id]
        tasks[f"date-{col}-client"] = [choice.id]
        tasks[f"{col}-date-server"] = [[c_date_lower.id, c_date_upper.id]]
        tasks[f"{col}-date-client"] = [[c_date_lower.id, c_date_upper.id]]
        for col2 in ["category", "county", "pack"]:
            choice = { "category": c_category, "county": c_county, "pack": c_pack }[col2]
            if col != col2:
                tasks[f"{col}-{col2}-server"] = [choice.id]
                tasks[f"{col}-{col2}-client"] = [choice.id]

    f.write("var liquor_plans = " + json.dumps(plans) + ";\n")
    f.write("var liquor_values = " + json.dumps(values) + ";\n")
    f.write("var liquor_tasks = " + json.dumps(tasks) + ";\n")
    f.write(f"register_tasks(\"liquor\", liquor_plans, liquor_values, liquor_tasks);")

    conn.close()
