from rule.rule_base import *


class MergeSortFilter(LogicalRule):

    def get_conditions(self, cond: Expression) -> list[Expression]:
        if isinstance(cond, Op) and cond.op == "and":
            return self.get_conditions(cond.operands[0]) + self.get_conditions(cond.operands[1])
        else:
            return [cond]

    def match(self, node):
        if isinstance(node, Filter) and isinstance(node.input, Filter):
            return True

        if isinstance(node, Filter) and isinstance(node.cond, Op) and node.cond.op == "and":
            conds = self.get_conditions(node.cond)
            sorted_conds = sorted(conds, key=lambda c: str(c))
            if conds != sorted_conds:
                return True
        return False

    def apply(self, node):
        if isinstance(node, Filter) and isinstance(node.input, Filter):
            new_cond = Op("and", [node.input.cond, node.cond])
            new_node = Filter(node.input.input, new_cond)
            new_node.set_parent(node.parent)
        else:
            conds = self.get_conditions(node.cond)
            sorted_conds = sorted(conds, key=lambda c: str(c))
            new_cond = Op("and", sorted_conds)
            new_node = Filter(node.input, new_cond)
            new_node.set_parent(node.parent)
        return new_node
