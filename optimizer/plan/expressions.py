class Expression:
    def find_exprs(self, cond):
        raise NotImplementedError()

    def clone(self):
        raise NotImplementedError()

    def bind(self, binding):
        raise NotImplementedError()

    def __str__(self):
        raise NotImplementedError()

    def __hash__(self):
        return hash(str(self))

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

    @staticmethod
    def from_json(obj):
        if "type" not in obj:
            # must be Named
            return Named(obj["name"], Expression.from_json(obj["expr"]))
        if obj["type"] == "Op":
            return Op(obj["op"], [Expression.from_json(operand) for operand in obj["operands"]])
        elif obj["type"] == "IntConst":
            return V(int(obj["value"]))
        elif obj["type"] == "FloatConst":
            return V(float(obj["value"]))
        elif obj["type"] == "BoolConst":
            return V(bool(obj["value"]))
        elif obj["type"] == "StringConst":
            return V(str(obj["value"]))
        elif obj["type"] == "ColumnRef":
            return C(obj["table"], obj["column"])
        elif obj["type"] == "Func":
            return Func(obj["fname"], [Expression.from_json(arg) for arg in obj["args"]])
        elif obj["type"] == "List":
            return List([Expression.from_json(value) for value in obj["elements"]], obj["begin"], obj["end"], obj["delim"])
        elif obj["type"] == "AnyExpr":
            return Any(obj["id"], [Expression.from_json(choice) for choice in obj["choices"]])
        elif obj["type"] == "ValExpr":
            return Val(obj["id"], Expression.from_json(obj["domain"]))
        elif obj["type"] == "MultiExpr":
            return Multi(obj["id"], Expression.from_json(obj["child"]), obj["begin"], obj["end"], obj["delim"])
        else:
            raise NotImplementedError("Unknown expression type: " + obj["type"])


class Op(Expression):
    def __init__(self, op, operands):
        self.op = op
        self.operands = operands

    def find_exprs(self, cond):
        found = []
        for operand in self.operands:
            found += operand.find_exprs(cond)
        if cond(self):
            found.append(self)
        return found

    def clone(self):
        return Op(self.op, [operand.clone() for operand in self.operands])

    def bind(self, binding):
        return Op(self.op, [operand.bind(binding) for operand in self.operands])

    def __str__(self):
        if self.op in ["+", "-", "*", "/", "^", "=", "!=", "<", "<=", ">", ">=", "and", "or", "in"]:
            return "(" + str(self.operands[0]) + " " + self.op + " " + str(self.operands[1]) + ")"
        elif self.op == "between":
            return "(" + str(self.operands[0]) + " between " + str(self.operands[1]) + " and " + str(self.operands[2]) + ")"

    def to_json(self):
        return {
            "type": "Op",
            "op": self.op,
            "operands": [operand.to_json() for operand in self.operands]
        }

class V(Expression):
    def __init__(self, value):
        self.value = value

    def find_exprs(self, cond):
        return [self] if cond(self) else []

    def clone(self):
        return V(self.value)

    def bind(self, binding):
        return self

    def __str__(self) -> str:
        return str(self.value)

    def to_json(self):
        if isinstance(self.value, int):
            typ = "IntConst"
        elif isinstance(self.value, float):
            typ = "FloatConst"
        elif isinstance(self.value, bool):
            typ = "BoolConst"
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

    def find_exprs(self, cond):
        return [self] if cond(self) else []

    def clone(self):
        return C(self.table, self.column)

    def bind(self, binding):
        return self

    def __str__(self) -> str:
        return str(self.column)

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

    def find_exprs(self, cond):
        found = []
        for arg in self.args:
            found += arg.find_exprs(cond)
        if cond(self):
            found.append(self)
        return found

    def clone(self):
        return Func(self.func, [arg.clone() for arg in self.args])

    def bind(self, binding):
        return Func(self.func, [arg.bind(binding) for arg in self.args])

    def __str__(self):
        if self.func == "int":
            return "CAST(" + str(self.args[0]) + " AS INTEGER)"
        else:
            return self.func + "(" + ", ".join([str(arg) for arg in self.args]) + ")"

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

    def find_exprs(self, cond):
        found = []
        for element in self.elements:
            found += element.find_exprs(cond)
        if cond(self):
            found.append(self)
        return found

    def clone(self):
        return List([element.clone() for element in self.elements], self.begin, self.end, self.delim)

    def bind(self, binding):
        return List([element.bind(binding) for element in self.elements], self.begin, self.end, self.delim)

    def to_json(self):
        return {
            "type": "List",
            "elements": [value.to_json() for value in self.elements],
            "begin": self.begin,
            "end": self.end,
            "delim": self.delim
        }

class ChoiceExpr(Expression):
    def __init__(self, choice_id):
        self.choice_id = choice_id

    def find_exprs(self, cond):
        raise NotImplementedError()

    def clone(self):
        raise NotImplementedError()

    def bind(self, binding):
        raise NotImplementedError()

    def __str__(self):
        raise NotImplementedError()

class Any(ChoiceExpr):
    def __init__(self, choice_id, choices):
        super().__init__(choice_id)
        self.choices = choices

    def find_exprs(self, cond):
        found = []
        for choice in self.choices:
            found += choice.find_exprs(cond)
        if cond(self):
            found.append(self)
        return found

    def clone(self):
        return Any(self.choice_id, [choice.clone() for choice in self.choices])

    def bind(self, binding):
        if self.choice_id in binding:
            assert(isinstance(binding[self.choice_id], int))
            return self.choices[binding[self.choice_id]].bind(binding)
        else:
            return Any(self.choice_id, [choice.bind(binding) for choice in self.choices])

    def __str__(self):
        return f"ANY({self.choice_id})[{', '.join(str(choice) for choice in self.choices)}]"

    def to_json(self):
        return {
            "type": "AnyExpr",
            "id": self.choice_id,
            "choices": [choice.to_json() for choice in self.choices]
        }

class Val(ChoiceExpr):
    def __init__(self, choice_id, value):
        super().__init__(choice_id)
        self.value = value

    def find_exprs(self, cond):
        return [self] if cond(self) else []

    def clone(self):
        return Val(self.choice_id, self.value.clone())

    def bind(self, binding):
        if self.choice_id in binding:
            value = binding[self.choice_id]
            return V(value)
        else:
            return self.clone()

    def __str__(self):
        return f"VAL({self.choice_id})[{self.value}]"

    def to_json(self):
        return {
            "type": "ValExpr",
            "id": self.choice_id,
            "domain": self.value.to_json()
        }

class Multi(ChoiceExpr):
    def __init__(self, choice_id, child, begin, end, delim):
        super().__init__(choice_id)
        self.child = child
        self.begin = begin
        self.end = end
        self.delim = delim

    def find_exprs(self, cond):
        found = self.child.find_exprs(cond)
        if cond(self):
            found.append(self)
        return found

    def clone(self):
        return Multi(self.choice_id, self.child.clone(), self.begin, self.end, self.delim)

    def bind(self, binding):
        if self.choice_id in binding:
            assert(isinstance(binding[self.choice_id], list))
            return List([self.child.bind(binding) for binding in binding[self.choice_id]], self.begin, self.end, self.delim)
        else:
            return self.clone()

    def __str__(self):
        return f"MULTI({self.choice_id})[{str(self.child)}, {self.begin}{self.delim}{self.end}]"

    def to_json(self):
        return {
            "type": "MultiExpr",
            "id": self.choice_id,
            "child": self.child.to_json(),
            "begin": self.begin,
            "end": self.end,
            "delim": self.delim
        }

class Named(Expression):
    def __init__(self, name, expr):
        self.name = name
        self.expr = expr

    def find_exprs(self, cond):
        return self.expr.find_exprs(cond)

    def clone(self):
        return Named(self.name, self.expr.clone())

    def bind(self, binding):
        return Named(self.name, self.expr.bind(binding))

    def __str__(self):
        return f"{self.expr} AS {self.name}"

    def to_json(self):
        return {
            "name": self.name,
            "expr": self.expr.to_json()
        }
