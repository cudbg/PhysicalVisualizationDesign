from plan.plan_base import *


class Aggregate(Plan):
    def __init__(self, input, groupbys, aggs):
        super().__init__()
        self.input = input
        self.groupbys = groupbys
        self.aggs = aggs
        self.input.set_parent(Parent(self, "input"))

    def find_nodes(self, cond):
        found = self.input.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        found = self.input.find_exprs(cond)
        for groupby in self.groupbys:
            found += groupby.find_exprs(cond)
        for agg in self.aggs:
            found += agg.find_exprs(cond)
        return found

    def clone(self):
        return Aggregate(self.input.clone(),
                         [groupby.clone() for groupby in self.groupbys],
                         [agg.clone() for agg in self.aggs])

    def bind(self, binding):
        return Aggregate(self.input.bind(binding),
                         [groupby.bind(binding) for groupby in self.groupbys],
                         [agg.bind(binding) for agg in self.aggs])

    def to_schema(self):
        schema = []
        for groupby in self.groupbys:
            schema.append(groupby.expr if isinstance(groupby.expr, C) else C("", groupby.name))
        for agg in self.aggs:
            schema.append(C("", agg.name))
        return schema

    def selectivity(self, stat: Statistics) -> Selectivity:
        comb = 1.0
        for groupby in self.groupbys:
            dist = self.safe_get_distinct(groupby.expr)
            if dist is not None:
                comb *= dist
            else:
                comb = stat.upper_card
        return Selectivity(min(1.,comb / stat.upper_card), min(1., comb / stat.upper_card))

    def to_cost(self, switchon) -> tuple[Cost, Statistics]:
        input_cost, input_stat = self.input.cost(switchon)
        avg_input_n_rows = input_stat.avg_card
        upper_input_n_rows = input_stat.upper_card
        sel = self.selectivity(input_stat)
        avg_output_n_rows = avg_input_n_rows * sel.avg_sel
        upper_output_n_rows = upper_input_n_rows * sel.upper_sel
        input_n_cols = input_stat.n_cols
        output_n_cols = len(self.schema())
        input_n_string_cols = len([c for c in self.input.schema() if column_type[c.column] == "string"])
        output_n_string_cols = len([c for c in self.schema() if column_type[c.column] == "string"])

        column_info = {
            "input_num_cols": input_n_cols,
            "input_num_string_cols": input_n_string_cols,
            "output_num_cols": output_n_cols,
            "output_num_string_cols": output_n_string_cols
        }

        avg_latency = self.latency("Aggregate", avg_input_n_rows, avg_output_n_rows, column_info)
        upper_latency = self.latency("Aggregate", upper_input_n_rows, upper_output_n_rows, column_info)
        upper_latency = max(upper_latency, avg_latency)
        mem = self.memory("Table",upper_output_n_rows, column_info)

        cost = Cost(avg_latency + input_cost.avg_latency, upper_latency + input_cost.upper_latency, mem)
        stat = Statistics(avg_output_n_rows, upper_output_n_rows, output_n_cols)

        return cost, stat

    def to_str(self):
        return f"Aggregate[{self.cost(False)[0].upper_latency}](\n" + \
                    add_indent(f"groupby=({', '.join(str(g)for g in self.groupbys)})\nagg=({', '.join(str(a) for a in self.aggs)})", 4) + "\n)" +\
                    "\n|\n" + str(self.input)

    def to_functional_str(self):
        return f"Aggregate(" + \
            f"groupby=({','.join(str(g)for g in self.groupbys)})agg=({','.join(str(a) for a in self.aggs)}))" + \
            self.input.functional_str()

    def to_sql(self):
        return f"SELECT {','.join([str(groupby) for groupby in self.groupbys])}, {','.join([str(agg) for agg in self.aggs])}" + \
            f" FROM ({self.input.sql()}) GROUP BY {','.join([str(groupby.expr) for groupby in self.groupbys])}"

    def to_hash(self):
        return hash(("Aggregate", hash(self.input), (tuple(str(groupby) for groupby in self.groupbys), tuple(str(agg) for agg in self.aggs))))

    def to_json(self):
        return {
            "id": self.id,
            "type": "Aggregate",
            "input": self.input.to_json(),
            "groupbys": [group.to_json() for group in self.groupbys],
            "aggs": [agg.to_json() for agg in self.aggs]
        }
