#include "expression.h"

#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{
    List::List(std::vector<std::shared_ptr<Expression>> elems) : begin("["), end("]"), delim(","), elements(elems) {}
    List::List(std::initializer_list<std::shared_ptr<Expression>> elems) : begin("["), end("]"), delim(","), elements(elems) {}
    List::List(const std::string& begin,
               const std::string& end,
               const std::string& delim,
               std::vector<std::shared_ptr<Expression>> elems) : begin(begin), end(end), delim(delim), elements(elems) {}
    List::List(const std::string& begin,
               const std::string& end,
               const std::string& delim,
               std::initializer_list<std::shared_ptr<Expression>> elems) : begin(begin), end(end), delim(delim), elements(elems) {}

    void List::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        for (auto element : elements)
        {
            element->get_all_choice_nodes(choice_nodes);
        }
    }

    std::shared_ptr<Expression> List::bind(const BindingMap& binding) const
    {
        std::vector<std::shared_ptr<Expression>> new_elements;
        for (auto element : elements)
        {
            new_elements.push_back(element->bind(binding));
        }
        return std::make_shared<List>(begin, end, delim, new_elements);
    }

    std::shared_ptr<Expression> List::evaluate(const BindingMap& binding) const
    {
        std::vector<std::shared_ptr<Expression>> new_elements;
        for (auto element : elements)
        {
            new_elements.push_back(element->evaluate(binding));
        }
        return std::make_shared<List>(begin, end, delim, new_elements);
    }

    void List::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        for (auto element : elements)
        {
            element->pick_useful_binding(binding, useful_binding);
        }
    }

    cp::Expression List::to_arrow_expr() const
    {
        throw std::runtime_error("try to convert a list to arrow expression");
    }

    std::string List::to_string() const
    {
        std::string result = begin;
        for (auto element : elements)
        {
            result += element->to_string() + delim;
        }

        result += end;
        return result;
    }
}
