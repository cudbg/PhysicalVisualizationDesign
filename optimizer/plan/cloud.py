from plan.plan_base import *


class Cloud(Plan):
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
        return Cloud(self.input.clone())

    def bind(self, binding):
        return Cloud(self.input.bind(binding))
    def to_str(self) -> str:
        return f"Cloud\n|\n" + str(self.input)

    def to_schema(self) -> list[C]:
        return self.input.schema()

    def to_cost(self, switchon) -> tuple[Cost, Statistics]:
        stat = database.query_statistics(self.to_sql())
        n_rows = stat.upper_card
        n_cols = len(self.schema())
        stat.n_cols = n_cols
        output_n_string_cols = len([c for c in self.schema() if column_type[c.column] == "string"])

        column_info = {
            "output_num_cols": n_cols,
            "output_num_string_cols": output_n_string_cols
        }

        mem = self.memory("Table", n_rows, column_info)
        cost = Cost(0, 0, mem)
        return cost, stat

    def to_sql(self):
        return self.input.sql()

    def to_functional_str(self):
        return self.input.functional_str()

    def to_hash(self):
        return hash(("Cloud", hash(self.input)))

    def to_json(self):
        return {
            "id": self.id,
            "type": "Cloud",
            "input": self.input.to_json(),
        }
