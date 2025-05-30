from plan.plan_base import *
from plan import PrefixSumBuild


class PrefixSumQuery(Plan):
    def __init__(self, input, lower, upper):
        super().__init__()
        self.input = input
        self.lower = lower
        self.upper = upper
        self.input.set_parent(Parent(self, "input"))

    def find_nodes(self, cond):
        found = self.input.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        found = self.input.find_exprs(cond)
        found += self.lower.find_exprs(cond)
        found += self.upper.find_exprs(cond)
        return found

    def clone(self):
        return PrefixSumQuery(self.input.clone(), self.lower.clone(), self.upper.clone())

    def bind(self, binding):
        return PrefixSumQuery(self.input.bind(binding), self.lower.bind(binding), self.upper.bind(binding))

    def to_schema(self):
        node = self.input
        while not isinstance(node, PrefixSumBuild):
            node = node.input
        return [C("", node.target_col.name), C("", node.agg_col.name)]

    def to_cost(self, switchon):
        input_cost, input_stat = self.input.cost(switchon)
        avg_input_n_rows = input_stat.avg_card
        upper_input_n_rows = input_stat.upper_card
        avg_output_n_rows = avg_input_n_rows
        upper_output_n_rows = upper_input_n_rows
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

        avg_latency = 1
        upper_latency = 1
        mem = self.memory("Table", upper_output_n_rows, column_info)

        cost = Cost(avg_latency + input_cost.avg_latency, upper_latency + input_cost.upper_latency, mem)
        stat = Statistics(avg_output_n_rows, upper_output_n_rows, output_n_cols)

        return cost, stat

    def to_str(self):
        return f"PrefixSumQuery[{self.cost(False)[0].upper_latency}]({self.lower}, {self.upper})\n|\n" + str(self.input)

    def to_functional_str(self):
        return f"PrefixSumQuery({self.lower}, {self.upper})" + self.input.functional_str()

    def to_hash(self):
        return hash(("PrefixSumQuery", hash(self.input), str(self.lower), str(self.upper)))

    def to_json(self):
        return {
            "id": self.id,
            "type": "PrefixSumQuery",
            "input": self.input.to_json(),
            "lower": self.lower.to_json(),
            "upper": self.upper.to_json()
        }
