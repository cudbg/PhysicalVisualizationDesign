#include "plan.h"
#include "expression.h"
#include "binding.h"

namespace pvd
{
    Projection::Projection(int id,
                           std::shared_ptr<Plan> input,
                           std::vector<std::shared_ptr<Expression>> proj_exprs,
                           std::vector<std::string> proj_names)
            : AceroPlan(id, input), proj_exprs(proj_exprs), proj_names(proj_names) {
        metrics.id = id;
        metrics.node = "Projection";
    }

    std::vector<std::shared_ptr<Plan>> Projection::input_plans() const
    {
        return {input};
    }

    ac::Declaration Projection::build_plan(const BindingMap& binding, ac::Declaration input_plan)
    {
        auto arrow_projs = std::vector<cp::Expression>{};
        for (auto& expr : proj_exprs) {
            arrow_projs.push_back(expr->to_arrow_expr(binding));
        }
        auto option = ac::ProjectNodeOptions{arrow_projs, proj_names};
        auto acero_plan = ac::Declaration("project", {input_plan}, option);
        return acero_plan;
    }

    void Projection::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
        for (auto& expr : proj_exprs) {
            expr->pick_useful_binding(binding, useful_binding);
        }
    }

    void Projection::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        input->get_all_choice_nodes(choice_nodes);
        for (auto& expr : proj_exprs) {
            expr->get_all_choice_nodes(choice_nodes);
        }
    }

    std::string Projection::to_sql(const BindingMap& binding) const
    {
        std::string select_str;
        for (int i = 0; i < proj_exprs.size(); i++) {
            if (i > 0) select_str += ", ";
            select_str += proj_exprs[i]->bind(binding)->to_string() + " AS " + proj_names[i];
        }
        return "SELECT " + select_str + " FROM (" + input->to_sql(binding) + ")";
    }

    std::string Projection::to_string() const
    {
        std::string projs;
        for (int i = 0; i < proj_exprs.size(); i++) {
            projs += proj_names[i] + "=" + proj_exprs[i]->to_string();
            if (i < proj_exprs.size() - 1) {
                projs += ", ";
            }
        }
        return "Projection[" + std::to_string(id) + "]{" + projs + "}\n|\n" + input->to_string();
    }
}
