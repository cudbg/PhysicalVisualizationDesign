from rule.rule_base import *


class RTreeRule(DataStructureRule):

    def get_conditions(self, cond: Expression) -> list[Expression]:
        if isinstance(cond, Op) and cond.op == "and":
            return self.get_conditions(cond.operands[0]) + self.get_conditions(cond.operands[1])
        else:
            return [cond]

    def match_static(self, node: Plan) -> bool:
        # node must be Filter
        # its input must be static sub plan
        # then same as match_dynamic responding to all choice nodes in the filter condition
        if not isinstance(node, Filter): return False
        if node.input.has_choice_node(): return False
        ids = set([e.choice_id for e in node.cond.find_exprs(lambda e: isinstance(e, ChoiceExpr))])
        return self.match_dynamic(node, list(ids), False)

    def match_dynamic(self, node: Plan, choice_nodes: list[str], check_choice_input=True) -> bool:
        # node must be Filter
        # input does not contain the specified choice_nodes
        # the filter condition must be (C1 and C2 and ... and Cn)
        # where each Ci is (static key) = (variable value by choice_nodes not including column ref)
        if not isinstance(node, Filter): return False
        if not node.input.find_nodes(lambda n: isinstance(n, Cloud)): return False
        if node.input.find_nodes(lambda n: isinstance(n, ChoicePlan) and n.choice_id in choice_nodes): return False
        if node.input.find_exprs(lambda e: isinstance(e, ChoiceExpr) and e.choice_id in choice_nodes): return False
        if check_choice_input:
            if not node.input.find_nodes(lambda n: isinstance(n, ChoicePlan) and n.choice_id not in choice_nodes) and \
                    not node.input.find_exprs(lambda e: isinstance(e, ChoiceExpr) and e.choice_id not in choice_nodes): return False
        conds = self.get_conditions(node.cond)
        if len(conds) > 3: return False
        return all(map(
            lambda c: isinstance(c, Op) and c.op == "between" and \
                      "date" not in str(c.operands[0]).lower() and \
                      not c.operands[0].find_exprs(lambda e: isinstance(e, ChoiceExpr)) and \
                      not c.operands[1].find_exprs(lambda e: isinstance(e, C)) and \
                      not c.operands[2].find_exprs(lambda e: isinstance(e, C)) and \
                      (c.operands[1].find_exprs(lambda e: isinstance(e, ChoiceExpr) and e.choice_id in choice_nodes) or \
                       c.operands[2].find_exprs(lambda e: isinstance(e, ChoiceExpr) and e.choice_id in choice_nodes)),
            conds))

    def apply(self, node: Plan, scache: bool):
        assert(isinstance(node, Filter))
        assert(isinstance(node.input, Plan))
        conds = self.get_conditions(node.cond)
        keys = [c.operands[0] for c in conds]
        lowers = [c.operands[1] for c in conds]
        uppers = [c.operands[2] for c in conds]

        ds = node.input.rtree_build(keys)
        ds = ds.scache() if scache else ds.dcache()
        ds = ds.rtree_query(lowers, uppers)

        ds.set_parent(node.parent)
        return ds
