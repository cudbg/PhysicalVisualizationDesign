#include <utility>

#include "plan.h"
#include "expression.h"
#include "binding.h"


namespace pvd
{
    Aggregate::Aggregate(int id,
                         std::shared_ptr<Plan> input,
                         std::vector<std::shared_ptr<Expression>> groupby_exprs,
                         std::vector<std::string> groupby_names,
                         std::vector<std::shared_ptr<Expression>> aggregate_exprs,
                         std::vector<std::string> aggregate_names)
            : AceroPlan(id, std::move(input)), groupby_exprs(groupby_exprs), aggregate_exprs(aggregate_exprs),
              groupby_names(groupby_names), aggregate_names(aggregate_names) {
        metrics.id = id;
        metrics.node = "Aggregate";
    }

    std::vector<std::shared_ptr<Plan>> Aggregate::input_plans() const
    {
        return {input};
    }

    ac::Declaration Aggregate::build_plan(const BindingMap& binding, ac::Declaration input_plan)
    {
        std::vector<cp::Expression> proj_columns;
        std::vector<std::string> proj_names;

        for (auto& expr : this->groupby_exprs) {
            proj_columns.push_back(expr->to_arrow_expr(binding));
        }
        proj_names.insert(proj_names.end(), this->groupby_names.begin(), this->groupby_names.end());

        for (auto& expr : this->aggregate_exprs) {
            if (auto func = std::dynamic_pointer_cast<Func>(expr)) {
                if (func->arguments.size() == 1) {
                    proj_columns.push_back(func->arguments[0]->to_arrow_expr(binding));
                }
                else if (func->fname == "count") {
                    proj_columns.push_back(cp::literal(1));
                }
                else {
                    throw std::runtime_error("Aggregate::to_acero_plan: aggregate function must have one argument");
                }
            }
            else {
                throw std::runtime_error("Aggregate::to_acero_plan: aggregate_exprs must be Func");
            }
        }
        proj_names.insert(proj_names.end(), this->aggregate_names.begin(), this->aggregate_names.end());

        auto proj_option = ac::ProjectNodeOptions{proj_columns, proj_names};
        auto proj_plan = ac::Declaration("project", {input_plan}, proj_option);

        int n_groupby = this->groupby_exprs.size();
        int n_aggregate = this->aggregate_exprs.size();

        std::vector<arrow::Aggregate> aggregates;
        std::vector<arrow::FieldRef> keys;

        for (int i = 0; i < n_groupby; i++) {
            keys.emplace_back(this->groupby_names[i]);
        }
        for (int i = 0; i < n_aggregate; i++) {
            auto func = std::dynamic_pointer_cast<Func>(this->aggregate_exprs[i]);
            std::shared_ptr<cp::FunctionOptions> options;
            std::string acero_fname;
            if (func->fname == "count" || func->fname == "sum") {
                options = std::make_shared<cp::ScalarAggregateOptions>();
                acero_fname = "hash_sum";
            }
            else if (func->fname == "avg") {
                options = std::make_shared<cp::ScalarAggregateOptions>();
                acero_fname = "hash_mean";
            }
            else {
                throw std::runtime_error("aggregate function not supported");
            }
            aggregates.emplace_back(acero_fname, options, this->aggregate_names[i], this->aggregate_names[i]);
        }

        auto aggregate_options = ac::AggregateNodeOptions{/*aggregates=*/aggregates, keys};
        ac::Declaration aggregate{
                "aggregate", {std::move(proj_plan)}, std::move(aggregate_options)};

        return aggregate;
    }

    void Aggregate::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
        for (auto& expr : groupby_exprs) {
            expr->pick_useful_binding(binding, useful_binding);
        }
        for (auto& expr : aggregate_exprs) {
            expr->pick_useful_binding(binding, useful_binding);
        }
    }

    void Aggregate::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        input->get_all_choice_nodes(choice_nodes);
        for (auto expr : groupby_exprs) {
            expr->get_all_choice_nodes(choice_nodes);
        }
        for (auto expr : aggregate_exprs) {
            expr->get_all_choice_nodes(choice_nodes);
        }
    }

    std::string Aggregate::to_sql(const BindingMap& binding) const
    {
        std::string select_str;
        for (int i = 0; i < groupby_exprs.size(); i++) {
            select_str += groupby_exprs[i]->bind(binding)->to_string() + " AS " + groupby_names[i] + ", ";
        }
        for (int i = 0; i < aggregate_exprs.size(); i++) {
            if (i > 0) select_str += ", ";
            select_str += aggregate_exprs[i]->bind(binding)->to_string() + " AS " + aggregate_names[i];
        }
        std::string groupby_str;
        for (int i = 0; i < groupby_exprs.size(); i++) {
            if (i > 0) groupby_str += ", ";
            groupby_str += groupby_exprs[i]->bind(binding)->to_string();
        }
        return "SELECT " + select_str + " FROM (" + input->to_sql(binding) + ") GROUP BY " + groupby_str;
    }

    std::string Aggregate::to_string() const
    {
        std::string groups;
        std::string aggs;
        for (auto& expr : groupby_exprs) {
            groups += expr->to_string() + ", ";
        }
        for (auto& expr : aggregate_exprs) {
            aggs += expr->to_string() + ", ";
        }
        return "Aggregate[" + std::to_string(id) + "]{" + groups + " -> " + aggs + "}\n|\n" + input->to_string();
    }
}
