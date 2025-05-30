from rule.rule_base import *


class SwapFilter(LogicalRule):
    def match(self, node):
        # match Filter(Filter(?input, ?cond1), ?cond2)
        return isinstance(node, Filter) and isinstance(node.input, Filter)

    def apply(self, node):
        # swap Filter(Filter(?input, ?cond2), ?cond1)
        # into Filter(Filter(?input, ?cond1), ?cond2)
        cond1, cond2 = node.cond, node.input.cond
        new_node = Filter(Filter(node.input.input, cond1), cond2)
        new_node.set_parent(node.parent)
        return new_node
