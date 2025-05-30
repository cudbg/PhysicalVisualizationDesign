from rule.rule_base import *


class PrefixSumRule(DataStructureRule):

    def get_choice_ids(self, node: Plan) -> set[str]:
        return set([e.choice_id for gb in node.groupbys for e in gb.find_exprs(lambda e: isinstance(e, ChoiceExpr))] + \
                   [e.choice_id for agg in node.aggs for e in agg.find_exprs(lambda e: isinstance(e, ChoiceExpr))] + \
                   [e.choice_id for e in node.input.cond.find_exprs(lambda e: isinstance(e, ChoiceExpr))])

    def match_static(self, node: Plan) -> bool:
        # node must be Aggregate
        # its input must be Filter
        if not isinstance(node, Aggregate): return False
        if not isinstance(node.input, Filter): return False
        ids = [n.choice_id for n in node.find_exprs(lambda e: isinstance(e, ChoiceExpr))]
        return self.match_dynamic(node, list(ids), False)

    def match_dynamic(self, node: Plan, choice_nodes: list[str], check_choice_input=True) -> bool:
        # node must be Aggregate with followed by Filter
        # Aggregate must contain one dimension and one aggregate of sum
        # Filter must be between
        if not isinstance(node, Aggregate): return False
        if not isinstance(node.input, Filter): return False
        if not node.input.input.find_nodes(lambda n: isinstance(n, Cloud)): return False
        if node.input.input.find_nodes(lambda n: isinstance(n, ChoicePlan) and n.choice_id in choice_nodes): return False
        if node.input.input.find_exprs(lambda e: isinstance(e, ChoiceExpr) and e.choice_id in choice_nodes): return False
        if check_choice_input:
            if not node.input.input.find_nodes(lambda n: isinstance(n, ChoicePlan) and n.choice_id not in choice_nodes) and \
                    not node.input.input.find_exprs(lambda e: isinstance(e, ChoiceExpr) and e.choice_id not in choice_nodes): return False
        if len(node.groupbys) != 1: return False
        if len(node.aggs) != 1: return False
        if not isinstance(node.aggs[0].expr, Func) or node.aggs[0].expr.func not in ["sum", "count"]: return False
        if not isinstance(node.input.cond, Op) or node.input.cond.op != "between": return False
        if not isinstance(node.input.cond.operands[0], C): return False
        if str(node.groupbys[0].expr) == str(node.input.cond.operands[0]): return False
        return True

    def apply(self, node: Plan, scache: bool):
        target_col = node.groupbys[0]
        agg = node.aggs[0]
        sum_col = Named(node.input.cond.operands[0].column, node.input.cond.operands[0])
        lower = node.input.cond.operands[1]
        upper = node.input.cond.operands[2]

        if agg.expr.func == "sum":
            agg_col = Named(agg.name, agg.expr.args[0])
        else:
            agg_col = Named(agg.name, V(1))

        ds = PrefixSumBuild(node.input.input, sum_col, target_col, agg_col)
        ds = ds.scache() if scache else ds.dcache()
        ds = ds.prefixsum_query(lower, upper)
        ds.set_parent(node.parent)
        return ds
