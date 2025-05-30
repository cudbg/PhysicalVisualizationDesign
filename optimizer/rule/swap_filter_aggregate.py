from rule.rule_base import *

class SwapFilterAggregate(LogicalRule):

    def validate_condition(self, cond: Expression, dims: list[Expression]) -> bool:
        if any(map(lambda d: str(d) == str(cond), dims)):
            # if the expression is in the dims list, it is valid
            return True
        if not cond.find_exprs(lambda e: isinstance(e, C)):
            # if the expression does not contain any attribute, it is valid
            return True
        if isinstance(cond, Op):
            # if the expression is an operator, all its arguments are valid
            return all(map(lambda e: self.validate_condition(e, dims), cond.operands))
        if isinstance(cond, List):
            # if the expression is a List, all its elements are valid
            return all(map(lambda e: self.validate_condition(e, dims), cond.elements))
        if isinstance(cond, Any):
            # if the expression is an AnyExpr, all its choices are valid
            return all(map(lambda c: self.validate_condition(c, dims), cond.choices))
        if isinstance(cond, Val):
            # if the expression is an AnyExpr, all its choices are valid
            return True
        if isinstance(cond, Multi):
            # if the expression is a MultiExpr, its child is valid
            return self.validate_condition(cond.child, dims)
        if isinstance(cond, Named):
            return self.validate_condition(cond.expr, dims)
        else:
            return False

    def get_list_elems(self, groupby: Expression) -> list[Expression]:
        # return all possible elements in the variable list (by List, AnyExpr, MultiExpr)
        if isinstance(groupby, List):
            dims = []
            for elem in groupby.elements:
                dims += self.get_list_elems(elem)
            return dims
        elif isinstance(groupby, Any):
            dims = []
            for choice in groupby.choices:
                dims += self.get_list_elems(choice)
            return dims
        elif isinstance(groupby, Multi):
            return self.get_list_elems(groupby.child)
        elif isinstance(groupby, list):
            dims = []
            for g in groupby:
                dims += self.get_list_elems(g)
            return dims
        elif isinstance(groupby, Named):
            return [groupby.expr]
        else:
            raise NotImplementedError()

    def match(self, node: Plan) -> bool:
        # match Filter(Aggregate(?input, ?dim, ?agg), ?cond)
        #   or Aggregate(Filter(?input, ?cond), ?dim, ?agg)
        # can swap only if Filter is only about the aggregate dimensions
        if isinstance(node, Filter) and isinstance(node.input, Aggregate):
            return self.validate_condition(node.cond, self.get_list_elems(node.input.groupbys))
        elif isinstance(node, Aggregate) and isinstance(node.input, Filter):
            return self.validate_condition(node.input.cond, self.get_list_elems(node.groupbys)) or \
                (isinstance(node.input.cond, Op) and node.input.cond.op == "=" and
                 isinstance(node.input.cond.operands[0], C) and
                 self.validate_condition(node.input.cond.operands[1], self.get_list_elems(node.groupbys)))
        else:
            return False

    def apply(self, node: Plan):
        # swap Filter(Aggregate(?input, ?dim, ?agg), ?cond)
        # into Aggregate(Filter(?input, ?cond), ?dim, ?agg)
        # or vice versa
        if isinstance(node, Filter) and isinstance(node.input, Aggregate):
            new_node = Aggregate(Filter(node.input.input, node.cond),
                                 node.input.groupbys,
                                 node.input.aggs)
            new_node.set_parent(node.parent)
            return new_node
        elif isinstance(node, Aggregate) and isinstance(node.input, Filter):
            if self.validate_condition(node.input.cond, self.get_list_elems(node.groupbys)):
                new_node = Filter(Aggregate(node.input.input,
                                            node.groupbys,
                                            node.aggs),
                                  node.input.cond)
                new_node.set_parent(node.parent)
                return new_node
            else:
                assert(isinstance(node.input.cond, Op) and node.input.cond.op == "=" and isinstance(node.input.cond.operands[0], C))
                groupby = node.groupbys + [Named(node.input.cond.operands[0].column, node.input.cond.operands[0])]
                new_node = Projection(
                    Filter(Aggregate(node.input.input,
                                     groupby,
                                     node.aggs),
                           node.input.cond),
                    [Named(n.name, C("", n.name)) for n in node.groupbys + node.aggs])
                new_node.set_parent(node.parent)
                return new_node
