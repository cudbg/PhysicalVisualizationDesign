from rule.rule_base import *


class SplitFilter(LogicalRule):
    def match(self, node):
        # match Filter(?input, And(?cond1, ?cond2))
        return isinstance(node, Filter) and isinstance(node.cond, Op) and node.cond.op == "and"

    def apply(self, node):
        # split Filter(?input, And(?cond1, ?cond2))
        # into Filter(Filter(?input, ?cond1), ?cond2)
        cond1, cond2 = node.cond.operands
        new_node = Filter(Filter(node.input, cond2), cond1)
        new_node.set_parent(node.parent)
        return new_node
