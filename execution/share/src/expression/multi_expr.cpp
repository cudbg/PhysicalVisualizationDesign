#include "expression.h"

#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{
    MultiExpr::MultiExpr(const std::string& id,
                         const std::string& begin,
                         const std::string& end,
                         const std::string& delim,
                         std::shared_ptr<Expression> child) : _id(id), begin(begin), end(end), delim(delim), child(child) {}

    std::string MultiExpr::choice_id() const { return _id; }

    void MultiExpr::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        throw std::runtime_error("try to get choice nodes from multi");
    }

    std::shared_ptr<Expression> MultiExpr::bind(const BindingMap& binding) const
    {
        auto bind = binding.at(choice_id());
        if (bind.is_sub_bindings()) {
            std::vector<std::shared_ptr<Expression>> new_elements;

            for (int i = 0; i < bind.get_sub_binding_num(); ++i)
            {
                auto sub_binding = bind.get_sub_binding(i);
                new_elements.push_back(child->bind(sub_binding));
            }

            return std::make_shared<List>(begin, end, delim, new_elements);
        }
        else {
            throw std::runtime_error("invalid binding type for MultiExpr");
        }
    }

    std::vector<Binding> MultiExpr::all_choices()
    {
        throw std::runtime_error("try to get all choice from multi");
    }

    std::shared_ptr<Expression> MultiExpr::evaluate(const BindingMap& binding) const
    {
        auto bind = binding.at(choice_id());
        if (bind.is_sub_bindings()) {
            std::vector<std::shared_ptr<Expression>> new_elements;

            for (int i = 0; i < bind.get_sub_binding_num(); ++i)
            {
                auto sub_binding = bind.get_sub_binding(i);
                new_elements.push_back(child->evaluate(sub_binding));
            }
            return std::make_shared<List>(begin, end, delim, new_elements);
        }
        else {
            throw std::runtime_error("invalid binding type for MultiExpr");
        }
    }

    void MultiExpr::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        useful_binding.emplace(choice_id(), binding.at(choice_id()));
        child->pick_useful_binding(binding, useful_binding);
    }

    cp::Expression MultiExpr::to_arrow_expr() const
    {
        throw std::runtime_error("try to convert an expression with choice nodes to arrow expression");
    }

    std::string MultiExpr::to_string() const
    {
        return "MULTI[" + _id + "]{ " + child->to_string() + " }";
    }
}
