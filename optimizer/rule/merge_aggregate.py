from rule.rule_base import *


class MergeAggregate(LogicalRule):

    def match(self, node):
        if isinstance(node, Aggregate) and isinstance(node.input, Aggregate):
            cur_grps = set([str(p) for p in node.groupbys])
            input_grps = set([str(p) for p in node.input.groupbys])
            cur_aggs = set([str(p) for p in node.aggs])
            input_aggs = set([str(p) for p in node.input.aggs])
            if cur_grps.issubset(input_grps) and cur_aggs.issubset(input_aggs):
                return True
        return False

    def apply(self, node):
        new_node = Aggregate(node.input.input, node.groupbys, node.aggs)
        new_node.set_parent(node.parent)
        return new_node
