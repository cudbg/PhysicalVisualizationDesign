from plan import *
from rule import *
import config

class SearchCandPlan:

    def __init__(self, plan: Plan, iact: Interaction):
        self.plan = plan
        self.iact = iact
        self.searched = set()
        self.candidates = {}
        self.unpruned_candidates = 0

    def search(self):
        self.search_logical_plan(self.plan)
        return list(self.candidates.values())

    def already_searched(self, plan):
        h = hash(plan)
        if h in self.searched:
            return True
        self.searched.add(h)
        return False

    def search_logical_plan(self, plan):
        # check if the plan has been searched
        if self.already_searched(plan): return

        if config.debug_search:
            print("=="*50)
            print("search logical plan")
            print("=="*50)
            print(plan)
            print("\n\n\n")

        # consider entering the next stage
        self.determine_cloud_position(plan)

        nodes = plan.find_nodes(lambda n: True)
        for node in nodes:
            for rule in logical_rules:
                if rule.match(node):
                    new_node = rule.apply(node)
                    if node == plan:
                        self.search_logical_plan(new_node)
                    else:
                        self.search_logical_plan(plan)
                    node.sync_children()
                    node.set_parent(new_node.parent)

    def determine_cloud_position(self, plan):
        if config.debug_search:
            print("=="*50)
            print("determine cloud position")
            print("=="*50)
            print(plan)
            print("\n\n\n")

        # add cloud node in candidate plan
        # assume the plan is a chain
        node = plan
        while node.find_exprs(lambda e: isinstance(e, ChoiceExpr)):
            node = node.input
        parent = node.parent
        new_node = Cloud(node)
        if parent is None:
            self.search_static_mapping(new_node)
        else:
            new_node.set_parent(parent)
            self.search_static_mapping(plan)
        node.sync_children()
        node.set_parent(parent)

    def search_static_mapping(self, plan):
        if config.debug_search:
            print("=="*50)
            print("search static mapping")
            print("=="*50)
            print(plan)
            print("\n\n\n")

        for node in plan.find_nodes(lambda n: True):
            for rule in datastructure_rules:
                if rule.match_static(node):
                    new_node = rule.apply(node, scache=True)
                    if node == plan:
                        self.search_dynamic_mapping(new_node)
                    else:
                        self.search_dynamic_mapping(plan)
                    node.sync_children()
                    node.set_parent(new_node.parent)

    def search_dynamic_mapping(self, plan):
        if config.debug_search:
            print("=="*50)
            print("search dynamic mapping")
            print("=="*50)
            print(plan)
            print("\n\n\n")

        # we can skip dynamic mapping
        self.determine_network_position(plan)

        # search for places of dynamic data structures
        node = plan
        while not isinstance(node, SCache):
            for rule in datastructure_rules:
                if rule.match_dynamic(node, self.iact.choice_nodes):
                    new_node = rule.apply(node, scache=False)
                    if node == plan:
                        self.determine_network_position(new_node)
                    else:
                        self.determine_network_position(plan)
                    node.sync_children()
                    node.set_parent(new_node.parent)
            node = node.input

    def determine_network_position(self, plan):
        if config.debug_search:
            print("=="*50)
            print("determine network position")
            print("=="*50)
            print(plan)
            print("\n\n\n")

        node = plan
        while not (node.parent and isinstance(node.parent.node, Cloud)):
            while node in [SCache, DCache]:
                node = node.input
            # RTree serialization is not supported
            if not isinstance(node, RTreeBuild):
                parent = node.parent
                new_node = Network(node)
                if parent is None:
                    self.finalize_candidate(new_node)
                else:
                    new_node.set_parent(parent)
                    self.finalize_candidate(plan)
                node.sync_children()
                node.set_parent(parent)
            node = node.input

    def finalize_candidate(self, plan):
        if config.debug_search:
            print("=="*50)
            print("finalize candidate")
            print("=="*50)
            print(plan)
            print("\n\n\n")

        cand = plan.clone()
        while True:
            not_changed = True
            nodes = cand.find_nodes(lambda n: True)
            # all nodes in the plan
            for node in nodes:
                # try all possible transformations
                for rule in cleanup_rules:
                    if rule.match(node):
                        # can apply rule on the node n
                        new_node = rule.apply(node)
                        if node == cand:
                            cand = new_node
                        not_changed = False
            if not_changed:
                break

        self.unpruned_candidates += 1

        cost = cand.cost(False)[0]
        switchon = cand.cost(True)[0]
        if cost.upper_latency * config.avg_alpha <= self.iact.latency.latency and switchon.upper_latency * config.avg_alpha <= self.iact.latency.switch_on_latency:
            key = []
            for n in cand.find_nodes(lambda n: isinstance(n, SCache)):
                key.append(n.sig())
            for n in cand.find_nodes(lambda n: isinstance(n, DCache)):
                key.append(n.sig())
            key = tuple(sorted(key))
            if key in self.candidates:
                last_cand = self.candidates[key]
                if (cand.cost(False)[0].upper_latency, cand.cost(True)[0].upper_latency, cand.cost(False)[0].avg_latency) < \
                        (last_cand.cost(False)[0].upper_latency, last_cand.cost(True)[0].upper_latency, last_cand.cost(False)[0].avg_latency):
                    self.candidates[key] = cand
            else:
                self.candidates[key] = cand
