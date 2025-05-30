from plan.plan_base import *
import config


class Filter(Plan):
    def __init__(self, input, cond):
        super().__init__()
        self.input = input
        self.cond = cond
        self.input.set_parent(Parent(self, "input"))

    def find_nodes(self, cond):
        found = self.input.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        found = self.input.find_exprs(cond)
        found += self.cond.find_exprs(cond)
        return found

    def clone(self):
        return Filter(self.input.clone(), self.cond.clone())

    def bind(self, binding):
        return Filter(self.input.bind(binding), self.cond.bind(binding))

    def to_str(self) -> str:
        return f"Filter[{self.cost(False)[0].upper_latency}]({self.cond})\n|\n" + str(self.input)

    def to_schema(self) -> list[C]:
        return self.input.schema()

    def selectivity(self, stat: Statistics) -> Selectivity:
        def get_cond(cond):
            if isinstance(cond, Op) and cond.op == "and":
                return get_cond(cond.operands[0]) + get_cond(cond.operands[1])
            else:
                return [cond]
        conds = get_cond(self.cond)
        sel = Selectivity(1, 1)
        for cond in conds:
            if isinstance(cond, Op) and cond.op == "=":
                max_card = self.safe_get_max_card(cond.operands[0])
                avg_card = self.safe_get_avg_card(cond.operands[0])
                if config.selectivity_mode == "Upper" and max_card is not None:
                    sel = Selectivity(min(1, max_card / stat.upper_card), min(1, max_card / stat.upper_card))
                elif config.selectivity_mode == "Avg" and avg_card is not None:
                    sel = Selectivity(min(0.1, avg_card / stat.upper_card), min(0.1, avg_card / stat.upper_card))
                else:
                    sel = Selectivity(sel.avg_sel * 0.1, sel.upper_sel * 1)
            else:
                sel = Selectivity(sel.avg_sel * 0.1, sel.upper_sel * 1)
        if config.selectivity_mode == "Avg":
            sel.upper_sel = sel.avg_sel

        return sel

    def to_cost(self, switchon) -> tuple[Cost, Statistics]:
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

        avg_latency = self.latency("Filter", avg_input_n_rows, avg_output_n_rows, column_info)
        upper_latency = self.latency("Filter", upper_input_n_rows, upper_output_n_rows, column_info)
        upper_latency = max(upper_latency, avg_latency)
        mem = self.memory("Table", upper_output_n_rows, column_info)

        cost = Cost(avg_latency + input_cost.avg_latency, upper_latency + input_cost.upper_latency, mem)
        stat = Statistics(avg_output_n_rows, upper_output_n_rows, output_n_cols)

        return cost, stat

    def to_sql(self):
        return f"SELECT * FROM ({self.input.sql()}) WHERE {self.cond}"

    def to_functional_str(self):
        return f"Filter({self.cond})" + self.input.functional_str()

    def to_hash(self):
        return hash(("Filter", hash(self.input), str(self.cond)))

    def to_json(self):
        return {
            "id": self.id,
            "type": "Filter",
            "input": self.input.to_json(),
            "cond": self.cond.to_json()
        }
