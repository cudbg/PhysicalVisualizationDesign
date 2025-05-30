#include "expression.h"

#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{
    AnyExpr::AnyExpr(const std::string& id, std::vector<std::shared_ptr<Expression>> choices) : _id(id), choices(choices) {}
    AnyExpr::AnyExpr(const std::string& id, std::initializer_list<std::shared_ptr<Expression>> choices) : _id(id), choices(choices) {}

    std::string AnyExpr::choice_id() const { return _id; }

    void AnyExpr::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        choice_nodes[_id] = std::make_shared<AnyExpr>(_id, choices);
        for (auto choice : choices)
        {
            choice->get_all_choice_nodes(choice_nodes);
        }
    }

    std::shared_ptr<Expression> AnyExpr::bind(const BindingMap& binding) const
    {
        return choices[binding.at(choice_id()).get_int()]->bind(binding);
    }

    std::vector<Binding> AnyExpr::all_choices()
    {
        std::vector<Binding> result;
        for (int i = 0; i < choices.size(); i++)
        {
            result.push_back(Binding(Binding::Kind::Index, i));
        }
        return result;
    }

    std::shared_ptr<Expression> AnyExpr::evaluate(const BindingMap& binding) const
    {
        return choices[binding.at(choice_id()).get_int()]->evaluate(binding);
    }

    cp::Expression AnyExpr::to_arrow_expr() const
    {
        throw std::runtime_error("try to convert an expression with choice nodes to arrow expression");
    }

    void AnyExpr::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        useful_binding.emplace(choice_id(), binding.at(choice_id()));
        for (auto choice : choices)
        {
            choice->pick_useful_binding(binding, useful_binding);
        }
    }

    std::string AnyExpr::to_string() const
    {
        std::string result = "ANY[" + _id + "]{ ";
        for (auto choice : choices)
        {
            result += choice->to_string() + ", ";
        }
        result += " }";
        return result;
    }
}
