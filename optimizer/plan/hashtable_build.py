from plan.plan_base import *


class HashTableBuild(Plan):
    def __init__(self, input, keys ):
        super().__init__()
        self.input = input
        self.keys = keys
        self.input.set_parent(Parent(self, "input"))

    def find_nodes(self, cond):
        found = self.input.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        found = self.input.find_exprs(cond)
        for key in self.keys:
            found += key.find_exprs(cond)
        return found

    def clone(self):
        return HashTableBuild(self.input.clone(), [key.clone() for key in self.keys])

    def bind(self, binding):
        return HashTableBuild(self.input.bind(binding), [key.bind(binding) for key in self.keys])

    def to_schema(self) -> list[C]:
        return self.input.schema()

    def to_cost(self, switchon) -> tuple[Cost, Statistics]:
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

        avg_latency = self.latency("HashTableBuild", avg_input_n_rows, avg_output_n_rows, column_info)
        upper_latency = self.latency("HashTableBuild", upper_input_n_rows, upper_output_n_rows, column_info)
        upper_latency = max(upper_latency, avg_latency)
        mem = self.memory("HashTableBuild", upper_output_n_rows, column_info)

        cost = Cost(avg_latency + input_cost.avg_latency, upper_latency + input_cost.upper_latency, mem)
        stat = Statistics(avg_output_n_rows, upper_output_n_rows, output_n_cols)

        return cost, stat

    def to_str(self) -> str:
        return f"HashTableBuild[{self.cost(False)[0].upper_latency}]({', '.join([str(key) for key in self.keys])})\n|\n" + str(self.input)

    def to_functional_str(self) -> str:
        return f"HashTableBuild({','.join([str(key) for key in self.keys])})" + self.input.functional_str()

    def to_hash(self):
        return hash(("HashTableBuild", hash(self.input), tuple(str(key) for key in self.keys)))

    def to_json(self):
        return {
            "id": self.id,
            "type": "HashTableBuild",
            "input": self.input.to_json(),
            "keys": [key.to_json() for key in self.keys]
        }
