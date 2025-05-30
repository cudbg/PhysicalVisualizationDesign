from plan.plan_base import *


class PrefixSumBuild(Plan):
    def __init__(self, input, sum_col, target_col, agg_col):
        super().__init__()
        self.input = input
        self.sum_col = sum_col
        self.target_col = target_col
        self.agg_col = agg_col
        self.input.set_parent(Parent(self, "input"))

    def find_nodes(self, cond):
        found = self.input.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        found = self.input.find_exprs(cond)
        found += self.sum_col.find_exprs(cond)
        found += self.target_col.find_exprs(cond)
        found += self.agg_col.find_exprs(cond)
        return found

    def clone(self):
        return PrefixSumBuild(self.input.clone(), self.sum_col.clone(), self.target_col.clone(), self.agg_col.clone())

    def bind(self, binding):
        return PrefixSumBuild(self.input.bind(binding), self.sum_col.bind(binding), self.target_col.bind(binding), self.agg_col.bind(binding))

    def to_schema(self):
        return [C("", self.sum_col.name), C("", self.target_col.name), C("", self.agg_col.name)]

    def to_cost(self, switchon):
        input_cost, input_stat = self.input.cost(switchon)
        avg_input_n_rows = input_stat.avg_card
        upper_input_n_rows = input_stat.upper_card
        avg_output_n_rows = self.safe_get_distinct(self.target_col.expr) or avg_input_n_rows
        upper_output_n_rows = self.safe_get_distinct(self.target_col.expr) or upper_input_n_rows
        input_n_cols = input_stat.n_cols
        output_n_cols = self.safe_get_distinct(self.sum_col.expr) or upper_input_n_rows
        input_n_string_cols = 0
        output_n_string_cols = 0

        column_info = {
            "input_num_cols": input_n_cols,
            "input_num_string_cols": input_n_string_cols,
            "output_num_cols": output_n_cols,
            "output_num_string_cols": output_n_string_cols
        }

        avg_latency = self.latency("PrefixSumBuild", avg_input_n_rows, avg_output_n_rows, column_info)
        upper_latency = self.latency("PrefixSumBuild", upper_input_n_rows, upper_output_n_rows, column_info)
        upper_latency = max(upper_latency, avg_latency)
        mem = self.memory("PrefixSumBuild", upper_output_n_rows, column_info)

        cost = Cost(avg_latency + input_cost.avg_latency, upper_latency + input_cost.upper_latency, mem)
        stat = Statistics(avg_output_n_rows, upper_output_n_rows, output_n_cols)

        return cost, stat

    def to_str(self):
        return f"PrefixSumBuild[{self.cost(False)[0].upper_latency}]({self.sum_col}, {self.target_col}, {self.agg_col})\n|\n" + str(self.input)

    def to_functional_str(self):
        return f"PrefixSumBuild({self.sum_col}, {self.target_col}, {self.agg_col})" + self.input.functional_str()

    def to_hash(self):
        return hash(("PrefixSumBuild", hash(self.input), str(self.sum_col), str(self.target_col), str(self.agg_col)))

    def to_json(self):
        return {
            "id": self.id,
            "type": "PrefixSumBuild",
            "input": self.input.to_json(),
            "sum_col": self.sum_col.to_json(),
            "target_col": self.target_col.to_json(),
            "agg_col": self.agg_col.to_json()
        }
