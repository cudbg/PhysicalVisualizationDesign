from plan.cost import *
from plan.expressions import *
from plan.database import *
from plan.utils import *
import pickle
import pandas as pd

class Interaction:
    """
    interaction: interaction name
    choice_nodes: list of choice node id
    latency: latency constraint of the interaction
    """
    def __init__(self, name: str, choice_nodes: list[str], latency: Latency):
        self.name = name
        self.choice_nodes = choice_nodes
        self.latency = latency

    def __str__(self):
        return f"Interaction({self.name}, [{','.join(self.choice_nodes)}], {self.latency})"


class Task:
    """
    views: list of views
    interactions: list of interactions
    memory: memory constraint of the task
    """
    def __init__(self, views: list["Plan"], interactions: list[Interaction], memory: Memory):
        self.views = views
        self.interactions = interactions
        self.memory = memory

class Parent:
    """
    parent: parent node
    index: index of this node in the parent node
    """
    def __init__(self, node: "Plan", index):
        self.node = node
        self.index = index

"""
Base class for all plans
"""
class Plan:
    def __init__(self) -> None:
        self.id = random_id()
        self.parent = None # parent node
        self._cost = None # cached cost
        self._switchon_cost = None # cached switchon cost
        self._str = None # cached string representation
        self._func_str = None # cached functional string representation
        self._hash = None # cached hash value
        self._schema = None # cached schema
        self._sql = None # cached sql

    def reset_cache(self) -> None:
        self._cost = None
        self._switchon_cost = None
        self._str = None
        self._func_str = None
        self._hash = None
        self._schema = None
        self._sql = None

    def sync_children(self):
        if self.input.parent.node != self or self.input.parent.index != "input":
            self.input.set_parent(Parent(self, "input"))

    def has_choice_node(self) -> bool:
        from plan.choice_plan import ChoicePlan
        if self.find_nodes(lambda n: isinstance(n, ChoicePlan)): return True
        if self.find_exprs(lambda n: isinstance(n, ChoiceExpr)): return True
        return False

    def __str__(self) -> str:
        if self._str is None:
            self._str = self.to_str()
        return self._str

    def __hash__(self):
        if self._hash is None:
            self._hash = self.to_hash()
        return self._hash

    def schema(self):
        if self._schema is None:
            self._schema = self.to_schema()
        return self._schema

    def cost(self, switchon):
        if switchon:
            if self._switchon_cost is None:
                self._switchon_cost = self.to_cost(switchon)
            return self._switchon_cost
        else:
            if self._cost is None:
                self._cost = self.to_cost(switchon)
            return self._cost

    def sql(self):
        if self._sql is None:
            self._sql = self.to_sql()
        return self._sql

    def functional_str(self):
        if self._func_str is None:
            self._func_str = self.to_functional_str()
        return self._func_str

    def path(self):
        # get the path to the node from root
        path = []
        node = self
        while node.parent is not None:
            path.append(node.parent.index)
            node = node.parent.node
        path.reverse()
        return path

    def get(self, index):
        node = self
        for i in index:
            if isinstance(i, str) and hasattr(node, i):
                node = getattr(node, i)
            else:
                node = node[i]
        return node

    def set(self, index, value):
        node = self
        if not isinstance(index, tuple):
            index = (index,)
        for i in index[:-1]:
            if isinstance(i, str) and hasattr(node, i):
                node = getattr(node, i)
            else:
                node = node[i]
        if isinstance(index[-1], str) and hasattr(node, index[-1]):
            setattr(node, index[-1], value)
        else:
            node[index[-1]] = value

        self.reset_cache()
        parent = self.parent
        while parent is not None:
            parent.node.reset_cache()
            parent = parent.node.parent

    def set_parent(self, parent):
        self.parent = parent
        if parent is not None:
            parent.node.set(parent.index, self)

    def sync_parent(self):
        if self.parent is not None:
            self.parent.node.set(self.parent.index, self)

    def at_server(self) -> int:
        from plan.network import Network
        if self.find_nodes(lambda n: isinstance(n, Network)): return 0
        return 1

    def latency(self, node, input_n_rows, output_n_rows, column_info):
        from plan.scache import SCache
        from plan.dcache import DCache
        from plan.rtree_build import RTreeBuild
        if node in ["Projection", "HashTableQuery", "PrefixSumQuery", "PrefixSum2DQuery", "DCache", "SCache", "Cloud"]:
            return 1
        if node == "Network":
            return column_info["input_num_cols"] * input_n_rows * 0.0001

        coef = latency_coef[(node, self.at_server())]
        return max(1, input_n_rows * coef["input_num_rows"] +
                      column_info["input_num_cols"] * coef["input_num_cols"] +
                      column_info["input_num_string_cols"] * coef["input_num_string_cols"] +
                      input_n_rows * column_info["input_num_cols"] * coef["input_size"] +
                      input_n_rows * column_info["input_num_string_cols"] * coef["input_string_size"] +
                      output_n_rows * coef["output_num_rows"] +
                      column_info["output_num_cols"] * coef["output_num_cols"] +
                      column_info["output_num_string_cols"] * coef["output_num_string_cols"] +
                      output_n_rows * column_info["output_num_cols"] * coef["output_size"] +
                      output_n_rows * column_info["output_num_string_cols"] * coef["output_string_size"] +
                      coef["bias"])

    def memory(self, node, n_rows, column_info):
        coef = memory_coef[node]
        return max(1, n_rows * coef["output_num_rows"] +
                      column_info["output_num_cols"] * coef["output_num_cols"] +
                      column_info["output_num_string_cols"] * coef["output_num_string_cols"] +
                      n_rows * column_info["output_num_cols"] * coef["output_size"] +
                      n_rows * column_info["output_num_string_cols"] * coef["output_string_size"])

    def get_memory(self):
        from plan.scache import SCache
        from plan.dcache import DCache
        from plan.network import Network

        node = self
        while isinstance(node, (SCache, DCache, Network)):
            node = node.input
        cost = node.cost(False)[0]
        return cost.memory

    def safe_get_max_card(self, expr: Expression):
        expr = str(expr)
        node = self
        while True:
            try:
                query = f"select __cnt__ from (select {expr}, count(*) as __cnt__ from ({node.sql()}) group by {expr} order by __cnt__ desc limit 1)"
                return int(database.query(query))
            except:
                from plan.table import Table
                if isinstance(node, Table):
                    return None
                node = node.input

    def safe_get_avg_card(self, expr: Expression):
        expr = str(expr)
        node = self
        while True:
            try:
                query = f"select avg(__cnt__) from (select {expr}, count(*) as __cnt__ from ({node.sql()}) group by {expr})"
                return int(database.query(query))
            except:
                from plan.table import Table
                if isinstance(node, Table):
                    return None
                node = node.input

    def safe_get_distinct(self, expr: Expression):
        expr = str(expr)
        node = self
        while True:
            try:
                query = f"select count(*) from (select distinct {expr} from ({node.sql()}))"
                return int(database.query(query))
            except:
                from plan.table import Table
                if isinstance(node, Table):
                    return None
                node = node.input

    def project(self, projs):
        from plan.projection import Projection
        return Projection(self, projs)

    def filter(self, cond):
        from plan.filter import Filter
        return Filter(self, cond)

    def aggregate(self, groups, aggs):
        from plan.aggregate import Aggregate
        return Aggregate(self, groups, aggs)

    def network(self):
        from plan.network import Network
        return Network(self)

    def cloud(self):
        from plan.cloud import Cloud
        return Cloud(self)

    def scache(self):
        from plan.scache import SCache
        return SCache(self)

    def dcache(self):
        from plan.dcache import DCache
        return DCache(self)

    def hashtable_build(self, keys):
        from plan.hashtable_build import HashTableBuild
        return HashTableBuild(self, keys)

    def hashtable_query(self, queries):
        from plan.hashtable_query import HashTableQuery
        return HashTableQuery(self, queries)

    def rtree_build(self, keys):
        from plan.rtree_build import RTreeBuild
        return RTreeBuild(self, keys)

    def rtree_query(self, lowers, uppers):
        from plan.rtree_query import RTreeQuery
        return RTreeQuery(self, lowers, uppers)

    def prefixsum_build(self, sum_col, target_col, agg_col):
        from plan.prefixsum_build import PrefixSumBuild
        return PrefixSumBuild(self, sum_col, target_col, agg_col)

    def prefixsum_query(self, lower, upper):
        from plan.prefixsum_query import PrefixSumQuery
        return PrefixSumQuery(self, lower, upper)

    def prefixsum2d_build(self, sum_col_x, sum_col_y, target_col, agg_col):
        from plan.prefixsum2d_build import PrefixSum2DBuild
        return PrefixSum2DBuild(self, sum_col_x, sum_col_y, target_col, agg_col)

    def prefixsum2d_query(self, x_lower, x_upper, y_lower, y_upper):
        from plan.prefixsum2d_query import PrefixSum2DQuery
        return PrefixSum2DQuery(self, x_lower, x_upper, y_lower, y_upper)

    @staticmethod
    def from_json(obj):
        from plan.table import Table
        from plan.projection import Projection
        from plan.filter import Filter
        from plan.aggregate import Aggregate
        from plan.network import Network
        from plan.cloud import Cloud
        from plan.scache import SCache
        from plan.dcache import DCache
        from plan.hashtable_build import HashTableBuild
        from plan.hashtable_query import HashTableQuery
        from plan.rtree_build import RTreeBuild
        from plan.rtree_query import RTreeQuery
        from plan.prefixsum_build import PrefixSumBuild
        from plan.prefixsum_query import PrefixSumQuery
        from plan.prefixsum2d_build import PrefixSum2DBuild
        from plan.prefixsum2d_query import PrefixSum2DQuery
        from plan.choice_plan import AnyPlan

        if obj["type"] == "TableSource":
            p = Table(obj["name"])
        elif obj["type"] == "Projection":
            input = Plan.from_json(obj["input"])
            projs = [Expression.from_json(proj) for proj in obj["projs"]]
            p = Projection(input, projs)
        elif obj["type"] == "Filter":
            input = Plan.from_json(obj["input"])
            cond = Expression.from_json(obj["cond"])
            p = Filter(input, cond)
        elif obj["type"] == "Aggregate":
            input = Plan.from_json(obj["input"])
            groupbys = [Expression.from_json(groupby) for groupby in obj["groupbys"]]
            aggs = [Expression.from_json(agg) for agg in obj["aggs"]]
            p = Aggregate(input, groupbys, aggs)
        elif obj["type"] == "Network":
            input = Plan.from_json(obj["input"])
            p = Network(input)
        elif obj["type"] == "Cloud":
            input = Plan.from_json(obj["input"])
            p = Cloud(input)
        elif obj["type"] == "SCache":
            input = Plan.from_json(obj["input"])
            p = SCache(input)
        elif obj["type"] == "DCache":
            input = Plan.from_json(obj["input"])
            p = DCache(input)
        elif obj["type"] == "HashTableBuild":
            input = Plan.from_json(obj["input"])
            keys = [Expression.from_json(key) for key in obj["keys"]]
            p = HashTableBuild(input, keys)
        elif obj["type"] == "HashTableQuery":
            input = Plan.from_json(obj["input"])
            queries = [Expression.from_json(query) for query in obj["queries"]]
            p = HashTableQuery(input, queries)
        elif obj["type"] == "PrefixSumBuild":
            input = Plan.from_json(obj["input"])
            sum_col = Expression.from_json(obj["sum_col"])
            target_col = Expression.from_json(obj["target_col"])
            agg_col = Expression.from_json(obj["agg_col"])
            p = PrefixSumBuild(input, sum_col, target_col, agg_col)
        elif obj["type"] == "PrefixSumQuery":
            input = Plan.from_json(obj["input"])
            lower = Expression.from_json(obj["lower"])
            upper = Expression.from_json(obj["upper"])
            p = PrefixSumQuery(input, lower, upper)
        elif obj["type"] == "PrefixSum2DBuild":
            input = Plan.from_json(obj["input"])
            sum_col_x = Expression.from_json(obj["sum_col_x"])
            sum_col_y = Expression.from_json(obj["sum_col_y"])
            target_col = Expression.from_json(obj["target_col"])
            agg_col = Expression.from_json(obj["agg_col"])
            p = PrefixSum2DBuild(input, sum_col_x, sum_col_y, target_col, agg_col)
        elif obj["type"] == "PrefixSum2DQuery":
            input = Plan.from_json(obj["input"])
            x_lower = Expression.from_json(obj["lower_x"])
            x_upper = Expression.from_json(obj["upper_x"])
            y_lower = Expression.from_json(obj["lower_y"])
            y_upper = Expression.from_json(obj["upper_y"])
            p = PrefixSum2DQuery(input, x_lower, x_upper, y_lower, y_upper)
        elif obj["type"] == "RTreeBuild":
            input = Plan.from_json(obj["input"])
            keys = [Expression.from_json(key) for key in obj["keys"]]
            p = RTreeBuild(input, keys)
        elif obj["type"] == "RTreeQuery":
            input = Plan.from_json(obj["input"])
            lowers = [Expression.from_json(lower) for lower in obj["lowers"]]
            uppers = [Expression.from_json(upper) for upper in obj["uppers"]]
            p = RTreeQuery(input, lowers, uppers)
        elif obj["type"] == "AnyPlan":
            p = AnyPlan(obj["choice_id"], [Plan.from_json(c) for c in obj["choices"]])
        else:
            raise NotImplementedError(str(obj))

        p.id = obj["id"]
        return p

    def find_nodes(self, cond) -> list["Plan"]: raise NotImplementedError()
    def find_exprs(self, cond) -> list[Expression]: raise NotImplementedError()
    def clone(self): raise NotImplementedError()
    def bind(self, binding): raise NotImplementedError()
    def to_schema(self) -> list[C]: raise NotImplementedError()
    def to_cost(self, switchon) -> tuple[Cost, Statistics]: raise NotImplementedError()
    def to_str(self) -> str: raise NotImplementedError()
    def to_sql(self): raise NotImplementedError()
    def to_functional_str(self): raise NotImplementedError()
    def to_hash(self): raise NotImplementedError(str(self.__class__))
    def to_json(self): raise NotImplementedError(str(self.__class__))
