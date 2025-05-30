from plan.plan_base import *


class DCache(Plan):
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
        return DCache(self.input.clone())

    def bind(self, binding):
        return DCache(self.input.bind(binding))

    def to_schema(self):
        return self.input.schema()

    def to_cost(self, switchon):
        input_cost, input_stat = self.input.cost(switchon)
        avg_input_n_rows = input_stat.avg_card
        upper_input_n_rows = input_stat.upper_card
        input_n_cols = input_stat.n_cols
        mem = input_cost.memory
        if switchon:
            cost = Cost(input_cost.avg_latency, input_cost.upper_latency, mem)
        else:
            cost = Cost(0, 0, mem)
        stat = Statistics(avg_input_n_rows, upper_input_n_rows, input_n_cols)
        return cost, stat

    def to_str(self):
        return f"DCache[{self.get_memory()}]\n|\n" + str(self.input)

    def to_sql(self):
        return self.input.sql()

    def to_functional_str(self):
        return self.input.functional_str()

    def sig(self):
        return "d_" + str(hash(f"{self.at_server()}" + self.functional_str()))

    def to_hash(self):
        return hash(("DCache", hash(self.input)))

    def to_json(self):
        return {
            "id": self.id,
            "type": "DCache",
            "input": self.input.to_json(),
        }
