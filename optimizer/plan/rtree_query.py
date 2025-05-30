from plan.plan_base import *
from plan.table import Table
from plan.rtree_build import RTreeBuild
import config


class RTreeQuery(Plan):
    def __init__(self, input, lowers, uppers):
        super().__init__()
        self.input = input
        self.lowers = lowers
        self.uppers = uppers
        self.input.set_parent(Parent(self, "input"))

    def find_nodes(self, cond):
        found = self.input.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        found = self.input.find_exprs(cond)
        for lower in self.lowers:
            found += lower.find_exprs(cond)
        for upper in self.uppers:
            found += upper.find_exprs(cond)
        return found

    def clone(self):
        return RTreeQuery(self.input.clone(),
                          [lower.clone() for lower in self.lowers],
                          [upper.clone() for upper in self.uppers])

    def bind(self, binding):
        return RTreeQuery(self.input.bind(binding),
                          [lower.bind(binding) for lower in self.lowers],
                          [upper.bind(binding) for upper in self.uppers])

    def to_schema(self):
        return self.input.schema()

    def selectivity(self, stat: Statistics) -> Selectivity:
        table = self.find_nodes(lambda n: isinstance(n, Table))[0]
        build = self.find_nodes(lambda n: isinstance(n, RTreeBuild))[0]
        if config.selectivity_mode != "NoFilter" and "sdss" in table.name:
            sel = Selectivity(0.002 ** len(build.keys), 0.01 ** len(build.keys))
        else:
            sel = Selectivity(0.1 ** len(build.keys), 1)
        if config.selectivity_mode == "Avg":
            sel.upper_sel = sel.avg_sel
        return sel

    def to_cost(self, switchon):
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

        avg_latency = self.latency("RTreeQuery", avg_input_n_rows, avg_output_n_rows, column_info)
        upper_latency = self.latency("RTreeQuery", upper_input_n_rows, upper_output_n_rows, column_info)
        upper_latency = max(upper_latency, avg_latency)
        mem = self.memory("Table", upper_output_n_rows, column_info)

        cost = Cost(avg_latency + input_cost.avg_latency, upper_latency + input_cost.upper_latency, mem)
        stat = Statistics(avg_output_n_rows, upper_output_n_rows, output_n_cols)

        return cost, stat

    def to_str(self):
        return f"RTreeQuery[{self.cost(False)[0].upper_latency}](LOWER=[{', '.join([str(lower) for lower in self.lowers])}], UPPER=[{', '.join([str(upper) for upper in self.uppers])}])\n|\n" + str(self.input)

    def to_functional_str(self):
        return f"RTreeQuery({','.join([str(lower) for lower in self.lowers])}, {','.join([str(upper) for upper in self.uppers])})" + self.input.functional_str()

    def to_hash(self):
        return hash(("RTreeQuery", hash(self.input), (tuple(str(lower) for lower in self.lowers), tuple(str(upper) for upper in self.uppers))))

    def to_json(self):
        return {
            "id": self.id,
            "type": "RTreeQuery",
            "input": self.input.to_json(),
            "lowers": [lower.to_json() for lower in self.lowers],
            "uppers": [upper.to_json() for upper in self.uppers]
        }
