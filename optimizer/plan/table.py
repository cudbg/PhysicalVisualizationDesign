from plan.plan_base import *


class Table(Plan):
    def __init__(self, name):
        super().__init__()
        self.name = name

    def sync_children(self):
        pass

    def find_nodes(self, cond):
        return [self] if cond(self) else []

    def find_exprs(self, cond):
        return []

    def clone(self):
        return Table(self.name)

    def bind(self, binding):
        return self.clone()

    def to_schema(self):
        return database.schema(self.name)

    def to_cost(self, switchon=False):
        stat = database.query_statistics(self.sql())
        schema = self.schema()
        stat.n_cols = len(schema)
        return Cost(0, 0, Memory(0, 0)), stat

    def to_str(self):
        return f"Table({self.name})"

    def to_functional_str(self):
        return f"Table({self.name})"

    def to_hash(self):
        return hash(("Table", self.name))

    def to_sql(self):
        return "SELECT * FROM " + self.name

    def to_json(self):
        return {
            "id": self.id,
            "type": "TableSource",
            "name": self.name
        }
