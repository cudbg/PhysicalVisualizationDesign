from plan.plan_base import *


class Network(Plan):
    def __init__(self, input):
        super().__init__()
        self.input = input
        self.input.set_parent(Parent(self, "input"))

    def find_nodes(self, cond):
        found = self.input.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        return self.input.find_exprs(cond)

    def clone(self):
        return Network(self.input.clone())

    def bind(self, binding):
        return Network(self.input.bind(binding))

    def to_schema(self):
        return self.input.schema()

    def to_cost(self, switchon):
        input_cost, input_stat = self.input.cost(switchon)
        avg_input_n_rows = input_stat.avg_card
        upper_input_n_rows = input_stat.upper_card
        avg_output_n_rows = avg_input_n_rows
        upper_output_n_rows = upper_input_n_rows
        input_n_cols = input_stat.n_cols
        output_n_cols = input_n_cols
        input_n_string_cols = len([c for c in self.input.schema() if column_type[c.column] == "string"])
        output_n_string_cols = len([c for c in self.schema() if column_type[c.column] == "string"])

        column_info = {
            "input_num_cols": input_n_cols,
            "input_num_string_cols": input_n_string_cols,
            "output_num_cols": output_n_cols,
            "output_num_string_cols": output_n_string_cols
        }

        avg_latency = self.latency("Network", avg_input_n_rows, avg_output_n_rows, column_info)
        upper_latency = self.latency("Network", upper_input_n_rows, upper_output_n_rows, column_info)
        upper_latency = max(upper_latency, avg_latency)
        mem = self.memory("Table", upper_output_n_rows, column_info)

        cost = Cost(avg_latency + input_cost.avg_latency, upper_latency + input_cost.upper_latency, mem)
        stat = Statistics(avg_output_n_rows, upper_output_n_rows, output_n_cols)

        return cost, stat

    def to_str(self):
        return f"Network[{self.cost(False)[0].upper_latency}]\n|\n" +str(self.input)

    def to_sql(self):
        return self.input.sql()

    def to_functional_str(self):
        return self.input.functional_str()

    def to_hash(self):
        return hash(("Network", hash(self.input)))

    def to_json(self):
        return {
            "id": self.id,
            "type": "Network",
            "input": self.input.to_json(),
        }
