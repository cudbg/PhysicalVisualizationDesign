from plan.plan_base import *
from plan import PrefixSum2DBuild


class PrefixSum2DQuery(Plan):
    def __init__(self, input, x_lower, x_upper, y_lower, y_upper):
        super().__init__()
        self.input = input
        self.x_lower = x_lower
        self.x_upper = x_upper
        self.y_lower = y_lower
        self.y_upper = y_upper
        self.input.set_parent(Parent(self, "input"))

    def find_nodes(self, cond):
        found = self.input.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        found = self.input.find_exprs(cond)
        found += self.x_lower.find_exprs(cond)
        found += self.x_upper.find_exprs(cond)
        found += self.y_lower.find_exprs(cond)
        found += self.y_upper.find_exprs(cond)
        return found

    def clone(self):
        return PrefixSum2DQuery(self.input.clone(), self.x_lower.clone(), self.x_upper.clone(), self.y_lower.clone(), self.y_upper.clone())

    def bind(self, binding):
        return PrefixSum2DQuery(self.input.bind(binding), self.x_lower.bind(binding), self.x_upper.bind(binding), self.y_lower.bind(binding), self.y_upper.bind(binding))

    def to_schema(self):
        node = self.input
        while not isinstance(node, PrefixSum2DBuild):
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
        return f"PrefixSum2DQuery[{self.cost(False)[0].upper_latency}]({self.x_lower}, {self.x_upper}, {self.y_lower}, {self.y_upper})\n|\n" + str(self.input)

    def to_functional_str(self):
        return f"PrefixSum2DQuery({self.x_lower}, {self.x_upper}, {self.y_lower}, {self.y_upper})" + self.input.functional_str()

    def to_hash(self):
        return hash(("PrefixSum2DQuery", hash(self.input), str(self.x_lower), str(self.x_upper), str(self.y_lower), str(self.y_upper)))

    def to_json(self):
        return {
            "id": self.id,
            "type": "PrefixSum2DQuery",
            "input": self.input.to_json(),
            "lower_x": self.x_lower.to_json(),
            "upper_x": self.x_upper.to_json(),
            "lower_y": self.y_lower.to_json(),
            "upper_y": self.y_upper.to_json(),
        }
