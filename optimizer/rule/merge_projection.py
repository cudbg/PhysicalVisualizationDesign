from rule.rule_base import *


class MergeProjection(LogicalRule):

    def match(self, node):
        if isinstance(node, Projection) and isinstance(node.input, Projection):
            cur_proj = set([str(p) for p in node.projs])
            input_proj = set([str(p) for p in node.input.projs])
            if cur_proj.issubset(input_proj):
                return True
        return False

    def apply(self, node):
        new_node = Projection(node.input.input, node.projs)
        new_node.set_parent(node.parent)
        return new_node
