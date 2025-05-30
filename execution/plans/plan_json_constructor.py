import random
import time

random.seed(int(time.time()))
def random_id():
    return random.randint(0, 2000000000)

class Expression:
    def __add__(self, other): return Op("+", [self, other])
    def __sub__(self, other): return Op("-", [self, other])
    def __mul__(self, other): return Op("*", [self, other])
    def __truediv__(self, other): return Op("/", [self, other])
    def __pow__(self, other): return Op("^", [self, other])
    def __neg__(self): return Op("-", [self])
    def __pos__(self): return self
    def __eq__(self, other): return Op("=", [self, other])
    def __ne__(self, other): return Op("!=", [self, other])
    def __lt__(self, other): return Op("<", [self, other])
    def __le__(self, other): return Op("<=", [self, other])
    def __gt__(self, other): return Op(">", [self, other])
    def __ge__(self, other): return Op(">=", [self, other])
    def __not__(self): return Op("not", [self])
    def __and__(self, other): return Op("and", [self, other])
    def __or__(self, other): return Op("or", [self, other])
    def _in(self, other): return Op("in", [self, other])
    def _between(self, lower, upper): return Op("between", [self, lower, upper])

class Op(Expression):
    def __init__(self, op, operands):
        self.op = op
        self.operands = operands

    def to_json(self):
        return {
            "type": "Op",
            "op": self.op,
            "operands": [operand.to_json() for operand in self.operands]
        }

class V(Expression):
    def __init__(self, value):
        self.value = value

    def to_json(self):
        if isinstance(self.value, int):
            typ = "IntConst"
        elif isinstance(self.value, float):
            typ = "FloatConst"
        elif isinstance(self.value, str):
            typ = "StringConst"

        return { 
            "type": typ,
            "value": self.value
        }

class C(Expression):
    def __init__(self, table, column):
        self.table = table
        self.column = column

    def to_json(self):
        return {
            "type": "ColumnRef",
            "table": self.table,
            "column": self.column
        }

class Func(Expression):
    def __init__(self, func, args):
        self.func = func
        self.args = args

    def to_json(self):
        return {
            "type": "Func",
            "fname": self.func,
            "args": [arg.to_json() for arg in self.args]
        }

class List(Expression):
    def __init__(self, elements, begin="[", end="]", delim=","):
        self.elements = elements
        self.begin = begin
        self.end = end
        self.delim = delim

    def to_json(self):
        return {
            "type": "List",
            "elements": [value.to_json() for value in self.values],
            "begin": self.begin,
            "end": self.end,
            "delim": self.delim
        }

class Any(Expression):
    def __init__(self, id, choices):
        self.id = id
        self.choices = choices

    def to_json(self):
        return {
            "type": "AnyExpr",
            "id": self.id,
            "choices": [choice.to_json() for choice in self.choices]
        }

class Val(Expression):
    def __init__(self, id, value):
        self.id = id
        self.value = value

    def to_json(self):
        return {
            "type": "ValExpr",
            "id": self.id,
            "domain": self.value.to_json()
        }

class MultiExpr(Expression):
    def __init__(self, id, child, begin, end, delim):
        self.id = id
        self.child = child
        self.begin = begin
        self.end = end
        self.delim = delim

    def to_json(self):
        return {
            "type": "MultiExpr",
            "id": self.id,
            "child": self.child.to_json(),
            "begin": self.begin,
            "end": self.end,
            "delim": self.delim
        }

class Named:
    def __init__(self, name, expr):
        self.name = name
        self.expr = expr

    def to_json(self):
        return {
            "name": self.name,
            "expr": self.expr.to_json()
        }

class Plan:
    def project(self, projs):
        return Projection(random_id(), self, projs)
    def filter(self, cond):
        return Filter(random_id(), self, cond)
    def aggregate(self, groups, aggs):
        return Aggregate(random_id(), self, groups, aggs)
    def network(self):
        return Network(random_id(), self)
    def cloud(self):
        return Cloud(random_id(), self)
    def scache(self):
        return SCache(random_id(), self)
    def dcache(self):
        return DCache(random_id(), self)
    def hashtable_build(self, keys):
        return HashTableBuild(random_id(), self, keys)
    def hashtable_query(self, queries):
        return HashTableQuery(random_id(), self, queries)
    def prefixsum_build(self, sum_col, target_col, agg_col):
        return PrefixSumBuild(random_id(), self, sum_col, target_col, agg_col)
    def prefixsum_query(self, lower, upper):
        return PrefixSumQuery(random_id(), self, lower, upper)
    def prefixsum2d_build(self, sum_col_x, sum_col_y, target_col, agg_col):
        return PrefixSum2DBuild(random_id(), self, sum_col_x, sum_col_y, target_col, agg_col)
    def prefixsum2d_query(self, x_lower, x_upper, y_lower, y_upper):
        return PrefixSum2DQuery(random_id(), self, x_lower, x_upper, y_lower, y_upper)
    def rtree_build(self, keys):
        return RTreeBuild(random_id(), self, keys)
    def rtree_query(self, lowers, uppers):
        return RTreeQuery(random_id(), self, lowers, uppers)
    def to_json(self):
        raise NotImplementedError(str(self.__class__))

class Table(Plan):
    def __init__(self, name, id=None):
        if id is None:
            id = random_id()
        self.id = id
        self.name = name
    def to_json(self):
        return {
            "id": self.id,
            "type": "TableSource",
            "name": self.name
        }

class Projection(Plan):
    def __init__(self, id, input, projs):
        self.id = id
        self.input = input
        self.projs = projs

    def to_json(self):
        return  {
            "id": self.id,
            "type": "Projection",
            "input": self.input.to_json(),
            "projs": [proj.to_json() for proj in self.projs]
        }

class Filter(Plan):
    def __init__(self, id, input, cond):
        self.id = id
        self.input = input
        self.cond = cond

    def to_json(self):
        return {
            "id": self.id,
            "type": "Filter",
            "input": self.input.to_json(),
            "cond": self.cond.to_json()
        }

class Aggregate(Plan):
    def __init__(self, id, input, groupbys, aggs):
        self.id = id
        self.input = input
        self.groupbys = groupbys
        self.aggs = aggs
    def to_json(self):
        return {
            "id": self.id,
            "type": "Aggregate",
            "input": self.input.to_json(),
            "groupbys": [group.to_json() for group in self.groupbys],
            "aggs": [agg.to_json() for agg in self.aggs]
        }
        
class Network(Plan):
    def __init__(self, id, input):
        self.id = id
        self.input = input
    def to_json(self):
        return {
            "id": self.id,
            "type": "Network",
            "input": self.input.to_json(),
        }

class Cloud(Plan):
    def __init__(self, id, input):
        self.id = id
        self.input = input
    def to_json(self):
        return {
            "id": self.id,
            "type": "Cloud",
            "input": self.input.to_json(),
        }

class SCache(Plan):
    def __init__(self, id, input):
        self.id = id
        self.input = input
    def to_json(self):
        return {
            "id": self.id,
            "type": "SCache",
            "input": self.input.to_json(),
        }

class DCache(Plan):
    def __init__(self, id, input):
        self.id = id
        self.input = input
    def to_json(self):
        return {
            "id": self.id,
            "type": "DCache",
            "input": self.input.to_json(),
        }

class HashTableBuild(Plan):
    def __init__(self, id, input, keys):
        self.id = id
        self.input = input
        self.keys = keys
    def to_json(self):
        return {
            "id": self.id,
            "type": "HashTableBuild",
            "input": self.input.to_json(),
            "keys": [key.to_json() for key in self.keys]
        }

class HashTableQuery(Plan):
    def __init__(self, id, input, queries):
        self.id = id
        self.input = input
        self.queries = queries
    def to_json(self):
        return {
           "id": self.id,
           "type": "HashTableQuery",
           "input": self.input.to_json(),
           "queries": [query.to_json() for query in self.queries]
        }

class RTreeBuild(Plan):
    def __init__(self, id, input, keys):
        self.id = id
        self.input = input
        self.keys = keys
    def to_json(self):
        return {
            "id": self.id,
            "type": "RTreeBuild",
            "input": self.input.to_json(),
            "keys": [key.to_json() for key in self.keys]
        }

class RTreeQuery(Plan):
    def __init__(self, id, input, lowers, uppers):
        self.id = id
        self.input = input
        self.lowers = lowers 
        self.uppers = uppers
    def to_json(self):
        return {
           "id": self.id,
           "type": "RTreeQuery",
           "input": self.input.to_json(),
           "lowers": [lower.to_json() for lower in self.lowers],
           "uppers": [upper.to_json() for upper in self.uppers]
        }

class PrefixSumBuild(Plan):
    def __init__(self, id, input, sum_col, target_col, agg_col):
        self.id = id
        self.input = input
        self.sum_col = sum_col
        self.target_col = target_col
        self.agg_col = agg_col
    def to_json(self):
        return {
            "id": self.id,
            "type": "PrefixSumBuild",
            "input": self.input.to_json(),
            "sum_col": self.sum_col.to_json(),
            "target_col": self.target_col.to_json(),
            "agg_col": self.agg_col.to_json()
        }

class PrefixSumQuery(Plan):
    def __init__(self, id, input, lower, upper):
        self.id = id
        self.input = input
        self.lower = lower
        self.upper = upper
    def to_json(self):
        return {
            "id": self.id,
            "type": "PrefixSumQuery",
            "input": self.input.to_json(),
            "lower": self.lower.to_json(),
            "upper": self.upper.to_json()
        }

class PrefixSum2DBuild(Plan):
    def __init__(self, id, input, sum_col_x, sum_col_y, target_col, agg_col):
        self.id = id
        self.input = input
        self.sum_col_x = sum_col_x
        self.sum_col_y = sum_col_y
        self.target_col = target_col
        self.agg_col = agg_col
    def to_json(self):
        return {
            "id": self.id,
            "type": "PrefixSum2DBuild",
            "input": self.input.to_json(),
            "sum_col_x": self.sum_col_x.to_json(),
            "sum_col_y": self.sum_col_y.to_json(),
            "target_col": self.target_col.to_json(),
            "agg_col": self.agg_col.to_json()
        }

class PrefixSum2DQuery(Plan):
    def __init__(self, id, input, x_lower, x_upper, y_lower, y_upper):
        self.id = id
        self.input = input
        self.x_lower = x_lower
        self.x_upper = x_upper
        self.y_lower = y_lower
        self.y_upper = y_upper
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

class AnyPlan(Plan):
    def __init__(self, id, choice_id, choices):
        self.id = id
        self.choice_id = choice_id
        self.choices = choices

    def to_json(self):
        return {
            "type": "AnyPlan",
            "id": self.id,
            "choice_id": self.choice_id,
            "choices": [choice.to_json() for choice in self.choices]
        }
