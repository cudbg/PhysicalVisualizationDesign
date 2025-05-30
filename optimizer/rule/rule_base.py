from plan import *


class LogicalRule:
    def match(self, node):
        raise NotImplementedError()

    def apply(self, node):
        raise NotImplementedError()


class DataStructureRule:

    def match_static(self, node: Plan) -> bool:
        raise NotImplementedError()

    def match_dynamic(self, node: Plan, choice_nodes):
        raise NotImplementedError()

    def apply(self, node: Plan, scache: bool):
        raise NotImplementedError()
