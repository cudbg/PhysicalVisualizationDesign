from rule.rule_base import LogicalRule, DataStructureRule
from rule.split_filter import SplitFilter
from rule.swap_filter import SwapFilter
from rule.swap_filter_aggregate import SwapFilterAggregate
from rule.mv_rule import MVRule
from rule.hashtable_rule import HashTableRule
from rule.rtree_rule import RTreeRule
from rule.prefixsum_rule import PrefixSumRule
from rule.prefixsum2d_rule import PrefixSum2DRule
from rule.merge_aggregate import MergeAggregate
from rule.merge_projection import MergeProjection
from rule.merge_sort_filter import MergeSortFilter

logical_rules = [
    SplitFilter(),
    SwapFilter(),
    SwapFilterAggregate()
]

cleanup_rules = [
    MergeAggregate(),
    MergeProjection(),
    MergeSortFilter()
]

datastructure_rules = [
    MVRule(),
    HashTableRule(),
    RTreeRule(),
    PrefixSumRule(),
    PrefixSum2DRule(),
]
