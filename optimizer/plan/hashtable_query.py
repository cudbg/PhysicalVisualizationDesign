from plan.plan_base import *
from plan.hashtable_build import HashTableBuild
import config


class HashTableQuery(Plan):
    def __init__(self, input, queries):
        super().__init__()
        self.input = input
        self.queries = queries
        self.input.set_parent(Parent(self, "input"))

    def find_nodes(self, cond):
        found = self.input.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        found = self.input.find_exprs(cond)
        for query in self.queries:
            found += query.find_exprs(cond)
        return found

    def clone(self):
        return HashTableQuery(self.input.clone(), [query.clone() for query in self.queries])

    def bind(self, binding):
        return HashTableQuery(self.input.bind(binding), [query.bind(binding) for query in self.queries])

    def to_schema(self):
        return self.input.schema()

    def selectivity(self, stat: Statistics) -> Selectivity:
        build = self.find_nodes(lambda n: isinstance(n, HashTableBuild))[0]
        sel = Selectivity(1, 1)
        for key in build.keys:
            max_card = self.safe_get_max_card(key)
            avg_card = self.safe_get_avg_card(key)
            if config.selectivity_mode == "Upper" and max_card is not None:
                sel = Selectivity(min(1, max_card / stat.upper_card), min(1, max_card / stat.upper_card))
            elif config.selectivity_mode == "Avg" and avg_card is not None:
                sel = Selectivity(min(0.1, avg_card / stat.upper_card), min(0.1, avg_card / stat.upper_card))
            else:
                sel = Selectivity(sel.avg_sel * 0.1, sel.upper_sel * 1)
        if config.selectivity_mode == "Avg":
            sel.upper_sel = min(0.001, sel.avg_sel)
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

        avg_latency = 1
        upper_latency = 1
        mem = self.memory("Table", upper_output_n_rows, column_info)

        cost = Cost(avg_latency + input_cost.avg_latency, upper_latency + input_cost.upper_latency, mem)
        stat = Statistics(avg_output_n_rows, upper_output_n_rows, output_n_cols)

        return cost, stat

    def to_str(self):
        return f"HashTableQuery[{self.cost(False)[0].upper_latency}]({', '.join([str(query) for query in self.queries])})\n|\n" + str(self.input)

    def to_functional_str(self):
        return f"HashTableQuery({','.join([str(query) for query in self.queries])})" + self.input.functional_str()

    def to_hash(self):
        return hash(("HashTableQuery", hash(self.input), tuple(str(query) for query in self.queries)))

    def to_json(self):
        return {
            "id": self.id,
            "type": "HashTableQuery",
            "input": self.input.to_json(),
            "queries": [query.to_json() for query in self.queries]
        }
