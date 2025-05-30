from plan.plan_base import *


class Projection(Plan):
    def __init__(self, input, projs):
        super().__init__()
        self.input = input
        self.projs = projs
        self.input.set_parent(Parent(self, "input"))

    def find_nodes(self, cond):
        found = self.input.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        found = self.input.find_exprs(cond)
        for proj in self.projs:
            found += proj.find_exprs(cond)
        return found

    def clone(self):
        return Projection(self.input.clone(), [proj.clone() for proj in self.projs])

    def bind(self, binding):
        return Projection(self.input.bind(binding), [proj.bind(binding) for proj in self.projs])

    def to_schema(self):
        return [proj.expr if isinstance(proj.expr, C) else C("", proj.name) for proj in self.projs]

    def to_cost(self, switchon):
        input_cost, input_stat = self.input.cost(switchon)
        avg_input_n_rows = input_stat.avg_card
        upper_input_n_rows = input_stat.upper_card
        avg_output_n_rows = avg_input_n_rows
        upper_output_n_rows = upper_input_n_rows
        input_n_cols = input_stat.n_cols
        output_n_cols = len(self.schema())
        input_n_string_cols = len([c for c in self.input.schema() if column_type.get(c.column, '') == "string"])
        output_n_string_cols = len([c for c in self.schema() if column_type.get(c.column, '') == "string"])

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
        return f"Projection[{self.cost(False)[0].upper_latency}]({','.join([str(proj) for proj in self.projs])})\n|\n" + str(self.input)

    def to_functional_str(self):
        return f"Projection({','.join([str(proj) for proj in self.projs])})" + self.input.functional_str()

    def to_sql(self):
        return f"SELECT {','.join([str(proj) for proj in self.projs])} FROM ({self.input.sql()})"

    def to_hash(self):
        return hash(("Projection", hash(self.input), tuple(str(proj) for proj in self.projs)))

    def to_json(self):
        return {
            "id": self.id,
            "type": "Projection",
            "input": self.input.to_json(),
            "projs": [proj.to_json() for proj in self.projs]
        }
