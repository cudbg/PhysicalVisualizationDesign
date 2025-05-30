from rule.rule_base import *


class MVRule(DataStructureRule):

    def match_static(self, node: Plan) -> bool:
        # node should not contain data structures
        if node.find_nodes(lambda n: isinstance(n, (ChoicePlan, SCache))): return False
        if not node.find_nodes(lambda n: isinstance(n, Cloud)): return False
        if node.find_exprs(lambda e: isinstance(e, ChoiceExpr)): return False
        return True

    def match_dynamic(self, node: Plan, choice_nodes: list[str]) -> bool:
        return False

    def apply(self, node: Plan, scache: bool):
        assert(scache)
        parent = node.parent
        ds = SCache(node)
        ds.set_parent(parent)
        return ds
