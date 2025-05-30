from plan.plan_base import *


class ChoicePlan(Plan):
    def __init__(self, choice_id) -> None:
        super().__init__()
        self.choice_id = choice_id


class AnyPlan(ChoicePlan):
    def __init__(self, choice_id, choices):
        super().__init__(choice_id)
        self.choices = choices
        for i, choice in enumerate(self.choices):
            choice.set_parent(Parent(self, ("choices", i)))

    def sync_children(self):
        for i, choice in enumerate(self.choices):
            if choice.parent.node != self or choice.parent.index != ("choices", i):
                choice.set_parent(Parent(self, ("choices", i)))

    def find_nodes(self, cond):
        found = []
        for choice in self.choices:
            found += choice.find_nodes(cond)
        if cond(self):
            found.append(self)
        return found

    def find_exprs(self, cond):
        found = []
        for choice in self.choices:
            found += choice.find_exprs(cond)
        if cond(self):
            found.append(self)
        return found

    def clone(self):
        return AnyPlan(self.choice_id, [choice.clone() for choice in self.choices])

    def bind(self, binding):
        if self.choice_id in binding:
            assert(isinstance(binding[self.choice_id], int))
            return self.choices[binding[self.choice_id]].bind(binding)
        else:
            return AnyPlan(self.choice_id, [choice.bind(binding) for choice in self.choices])

    def to_str(self) -> str:
        return f"AnyPlan({self.choice_id})[\n" + "\n,\n".join(str(choice) for choice in self.choices) + "\n]"

    def to_hash(self):
        return hash(("AnyPlan", self.choice_id, tuple(hash(choice) for choice in self.choices)))

    def to_schema(self) -> list[C]:
        return self.choices[0].schema()

    def to_json(self):
        return {
            "type": "AnyPlan",
            "id": self.id,
            "choice_id": self.choice_id,
            "choices": [choice.to_json() for choice in self.choices]
        }
