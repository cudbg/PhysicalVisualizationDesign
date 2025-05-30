from plan import *

FAST = 20
MID = 200
SLOW = 5000

def weather(client_mem=10000000, server_mem=1000000000, latency=None, switchon=None):
    table = Table(f"weather")
    date = C(f"weather", "date")
    country = C(f"weather", "country")
    temperature = C(f"weather", "temperature_celsius")

    n_date = Named("date", date)
    n_country = Named("country", country)
    n_temperature = Named("temperature_celsius", temperature)

    c_country = Val("choose_country", Func("domain", [country]))

    plan = table \
        .filter(country == c_country) \
        .aggregate([n_date, n_country],
                   [Named("temperature_celsius", Func("avg", [temperature]))]) \
        .project([n_date, n_temperature])

    I = Interaction("choose_country",
                    [c_country.choice_id],
                    Latency(latency or MID, latency or MID))

    return Task([plan], [I], Memory(server_mem, client_mem))

def sdss(client_mem=10000000, server_mem=1000000000, latency=None, switchon=None):
    table = Table(f"sdss")
    ra = C(f"sdss", "ra")
    dec = C(f"sdss", "dec")

    n_ra = Named("ra", ra)
    n_dec = Named("dec", dec)

    ra_lower = Val("ra_lower", Func("domain", [ra]))
    ra_upper = Val("ra_upper", Func("domain", [ra]))
    dec_lower = Val("dec_lower", Func("domain", [dec]))
    dec_upper = Val("dec_upper", Func("domain", [dec]))

    plan = table \
        .project([n_ra, n_dec])\
        .filter(ra._between(ra_lower, ra_upper) & dec._between(dec_lower, dec_upper))

    I = Interaction("choose_range",
                    [ra_lower.choice_id, ra_upper.choice_id, dec_lower.choice_id, dec_upper.choice_id],
                    Latency(latency or MID, latency or MID))

    return Task([plan], [I], Memory(server_mem, client_mem))

def immens(client_mem=10000000, server_mem=10000000000, latency=None, switchon=None):
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

    lat_lower = Val("lat_lower", Func("domain", [lat]))
    lat_upper = Val("lat_upper", Func("domain", [lat]))
    long_lower = Val("long_lower", Func("domain", [long]))
    long_upper = Val("long_upper", Func("domain", [long]))
    c_month = Val("choose_month", Func("domain", [month]))
    c_day = Val("choose_day", Func("domain", [day]))
    c_hour = Val("choose_hour", Func("domain", [hour]))

    table = table \
        .project([Named("latitude", Func("int", [lat * V(5)])),
                  Named("longitude", Func("int", [long * V(5)])),
                  n_month, n_day, n_hour])

    map_view = AnyPlan("choose_iact", [
        table.aggregate([n_lat, n_long], [Named("count", Func("count", []))]),
        table.filter(month == c_month)
                       .aggregate([n_lat, n_long], [Named("count", Func("count", []))]),
        table.filter(day == c_day)
                       .aggregate([n_lat, n_long], [Named("count", Func("count", []))]),
        table.filter(hour == c_hour)
                       .aggregate([n_lat, n_long], [Named("count", Func("count", []))]),
    ])

    month_view = AnyPlan("choose_iact", [
        table.filter(lat._between(lat_lower, lat_upper) & long._between(long_lower, long_upper))
                         .aggregate([n_month], [Named("count", Func("count", []))]),
        table.aggregate([n_month], [Named("count", Func("count", []))]),
        table.filter(day == c_day)
                         .aggregate([n_month], [Named("count", Func("count", []))]),
        table.filter(hour == c_hour)
                         .aggregate([n_month], [Named("count", Func("count", []))]),
    ])

    day_view = AnyPlan("choose_iact", [
        table.filter(lat._between(lat_lower, lat_upper) & long._between(long_lower, long_upper))
                       .aggregate([n_day], [Named("count", Func("count", []))]),
        table.filter(month == c_month)
                       .aggregate([n_day], [Named("count", Func("count", []))]),
        table.aggregate([n_day], [Named("count", Func("count", []))]),
        table.filter(hour == c_hour)
                       .aggregate([n_day], [Named("count", Func("count", []))]),
    ])

    hour_view = AnyPlan("choose_iact", [
        table.filter(lat._between(lat_lower, lat_upper) & long._between(long_lower, long_upper))
                        .aggregate([n_hour], [Named("count", Func("count", []))]),
        table.filter(month == c_month)
                        .aggregate([n_hour], [Named("count", Func("count", []))]),
        table.filter(day == c_day)
                        .aggregate([n_hour], [Named("count", Func("count", []))]),
        table.aggregate([n_hour], [Named("count", Func("count", []))]),
    ])

    I1 = Interaction("choose_range", [lat_lower.choice_id, lat_upper.choice_id, long_lower.choice_id, long_upper.choice_id], Latency(latency or FAST, latency or FAST))
    I2 = Interaction("choose_month", [c_month.choice_id], Latency(latency or MID, latency or MID))
    I3 = Interaction("choose_day", [c_day.choice_id], Latency(latency or MID, latency or MID))
    I4 = Interaction("choose_hour", [c_hour.choice_id], Latency(latency or MID, latency or MID))

    return Task([map_view, month_view, day_view, hour_view],
                [I1, I2, I3, I4],
                Memory(server_mem, client_mem))

def covid(client_mem=10000000, server_mem=1000000000000, latency=None, switchon=None):
    table = Table(f"covid")
    date = C(f"covid", "date")
    county = C(f"covid", "county")
    cases = C(f"covid", "cases")
    deaths = C(f"covid", "deaths")

    n_date = Named("date", date)
    n_county = Named("county", county)

    c_county = Val("choose_county", Func("domain", [county]))
    date_lower = Val("date_lower", Func("domain", [date]))
    date_upper = Val("date_upper", Func("domain", [date]))

    plan = table \
        .filter((county == c_county) & date._between(date_lower, date_upper)) \
        .aggregate([n_date],
                   [Any("case_or_death",
                        [Named("cases", Func("sum", [cases])),
                         Named("deaths", Func("sum", [deaths]))])])

    I1 = Interaction("choose_county",
                     [c_county.choice_id],
                     Latency(latency or MID, latency or MID))
    I2 = Interaction("choose_date",
                     [date_lower.choice_id, date_upper.choice_id],
                     Latency(latency or FAST, latency or FAST))
    return Task([plan], [I1, I2], Memory(server_mem, client_mem))

def flights(client_mem=10000000, server_mem=1000000000, latency=None, switchon=None):
    table = Table(f"flight")
    colname = ["dep_delay", "arr_delay", "air_time", "distance", "dep_time", "arr_time"]

    col = [C(f"flight", name) for name in colname]
    bin_col = [C("", f"bin_{name}") for name in colname]
    lower = [Val(f"lower_{name}", Func("domain", [c])) for name, c in zip(colname, col)]
    upper = [Val(f"upper_{name}", Func("domain", [c])) for name, c in zip(colname, col)]
    cond = [bin_col[i]._between(lower[i], upper[i]) for i in range(len(colname))]
    named_bin = [Named(f"bin_{name}", c) for name, c in zip(colname, bin_col)]
    bin_size = [10, 10, 20, 100, 1, 1]

    table = table \
        .aggregate([Named(f"bin_{colname[i]}", Func("int", [col[i] / V(bin_size[i])])) for i in range(len(colname))],
                   [Named("count_n", Func("count", []))])

    delay_plan = table \
        .filter(cond[2] & cond[3] & cond[4] & cond[5]) \
        .aggregate([named_bin[0], named_bin[1]], [Named("total", Func("sum", [C("", "count_n")]))])

    air_time_plan = table \
        .filter(cond[0] & cond[1] & cond[3] & cond[4] & cond[5]) \
        .aggregate([named_bin[2]], [Named("total", Func("sum", [C("", "count_n")]))])

    distance_plan = table \
        .filter(cond[0] & cond[1] & cond[2] & cond[4] & cond[5]) \
        .aggregate([named_bin[3]], [Named("total", Func("sum", [C("", "count_n")]))])

    dep_time_plan = table \
        .filter(cond[0] & cond[1] & cond[2] & cond[3] & cond[5]) \
        .aggregate([named_bin[4]], [Named("total", Func("sum", [C("", "count_n")]))])

    arr_time_plan = table \
        .filter(cond[0] & cond[1] & cond[2] & cond[3] & cond[4]) \
        .aggregate([named_bin[5]], [Named("total", Func("sum", [C("", "count_n")]))])

    I1 = Interaction("choose_delay",
                     [lower[0].choice_id, upper[0].choice_id, lower[1].choice_id, upper[1].choice_id],
                     Latency(latency or FAST, switchon or SLOW))
    I2 = Interaction("choose_air_time",
                     [lower[2].choice_id, upper[2].choice_id],
                     Latency(latency or FAST, switchon or SLOW))
    I3 = Interaction("choose_distance",
                     [lower[3].choice_id, upper[3].choice_id],
                     Latency(latency or FAST, switchon or SLOW))
    I4 = Interaction("choose_dep_time",
                     [lower[4].choice_id, upper[4].choice_id],
                     Latency(latency or FAST, switchon or SLOW))
    I5 = Interaction("choose_arr_time",
                     [lower[5].choice_id, upper[5].choice_id],
                     Latency(latency or FAST, switchon or SLOW))
    return Task([air_time_plan, distance_plan, dep_time_plan, arr_time_plan],
                [I1, I2, I3, I4, I5],
                Memory(server_mem, client_mem))

def liquor(client_mem=10000000, server_mem=1000000000, latency=None, switchon=None):
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

    table = table.aggregate([n_date, n_category, n_county, n_pack], [Named("Sales", Func("sum", [sales]))])

    date_plan = table \
        .filter((category == c_category) & (county == c_county) & (pack == c_pack)) \
        .aggregate([n_date], [Named("Sales", Func("sum", [sales]))])
    category_plan = table \
        .filter((date._between(c_date_lower, c_date_upper)) & (county == c_county) & (pack == c_pack)) \
        .aggregate([n_category], [Named("Sales", Func("sum", [sales]))])
    county_plan = table \
        .filter((date._between(c_date_lower, c_date_upper)) & (category == c_category) & (pack == c_pack)) \
        .aggregate([n_county], [Named("Sales", Func("sum", [sales]))])
    pack_plan = table \
        .filter((date._between(c_date_lower, c_date_upper)) & (category == c_category) & (county == c_county)) \
        .aggregate([n_pack], [Named("Sales", Func("sum", [sales]))])

    I1 = Interaction("choose_date",
                     [c_date_lower.choice_id, c_date_upper.choice_id],
                     Latency(latency or FAST, switchon or SLOW))
    I2 = Interaction("choose_category",
                     [c_category.choice_id],
                     Latency(latency or MID, switchon or SLOW))
    I3 = Interaction("choose_county",
                     [c_county.choice_id],
                     Latency(latency or MID, switchon or SLOW))
    I4 = Interaction("choose_pack",
                     [c_pack.choice_id],
                     Latency(latency or MID, switchon or SLOW))
    return Task([date_plan, category_plan, county_plan, pack_plan],
                [I1, I2, I3, I4],
                Memory(server_mem, client_mem))
