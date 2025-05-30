#include <utility>

#include "plan.h"
#include "expression.h"
#include "binding.h"

namespace pvd
{
    Filter::Filter(int id, std::shared_ptr<Plan> input,
                   std::shared_ptr<Expression> cond_expr)
            : AceroPlan(id, input), cond_expr(std::move(cond_expr)) {
        metrics.id = id;
        metrics.node = "Filter";
    }

    std::vector<std::shared_ptr<Plan>> Filter::input_plans() const
    {
        return {input};
    }

    ac::Declaration Filter::build_plan(const BindingMap& binding, ac::Declaration input_plan)
    {
        auto cond = this->cond_expr->to_arrow_expr(binding);
        //std::cout << "Filter condition: " << cond.ToString() << std::endl;
        auto option = ac::FilterNodeOptions{cond};
        auto acero_plan = ac::Declaration("filter", {input_plan}, option);
        //std::cout << "Filter plan: " << ac::DeclarationToString(acero_plan).ValueOrDie() << std::endl;
        return acero_plan;
    }

    void Filter::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
        cond_expr->pick_useful_binding(binding, useful_binding);
    }

    void Filter::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        input->get_all_choice_nodes(choice_nodes);
        cond_expr->get_all_choice_nodes(choice_nodes);
    }

    std::string Filter::to_sql(const BindingMap& binding) const
    {
        return "SELECT * FROM (" + input->to_sql(binding) + ") WHERE " + cond_expr->bind(binding)->to_string();
    }

    std::string Filter::to_string() const
    {
        return "Filter[" + std::to_string(id) + "]{" + cond_expr->to_string() + "}\n|\n" + input->to_string();
    }
}
