#include <utility>

#include "plan.h"
#include "expression.h"
#include "binding.h"

namespace pvd
{
    AnyPlan::AnyPlan(int id, const std::string &cid, std::vector<std::shared_ptr<Plan>> choices)
        : ChoicePlan(id, cid), choices(choices) {
        metrics.id = id;
        metrics.node = "AnyPlan";
    }

    std::vector<std::shared_ptr<Plan>> AnyPlan::input_plans() const
    {
        return choices;
    }

    void AnyPlan::execute(const BindingMap& binding, execute_callback_t cb)
    {
        auto child = choices[binding.at(choice_id).get_index()];
        child->execute(binding, cb);
    }

    void AnyPlan::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        useful_binding.emplace(choice_id, binding.at(choice_id));
        auto child = choices[binding.at(choice_id).get_index()];
        child->pick_useful_binding(binding, useful_binding);
    }

    void AnyPlan::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        throw std::runtime_error("AnyPlan::get_all_choice_nodes should not be called");
    }

    std::string AnyPlan::to_sql(const BindingMap& binding) const {
        auto child = choices[binding.at(choice_id).get_index()];
        return child->to_sql(binding);
    }

    std::string AnyPlan::to_string() const
    {
        std::string res = "AnyPlan[" + std::to_string(id) + "]{" + choice_id + "}\n";
        for (auto &child : choices)
        {
            res += "----\n" + child->to_string();
        }
        res += "====\n";
        return res;
    }
}
