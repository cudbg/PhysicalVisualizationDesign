#include "expression.h"

#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{
    Func::Func(const std::string& fname, std::vector<std::shared_ptr<Expression>> arguments) : fname(fname), arguments(arguments) {}
    Func::Func(const std::string& fname, std::initializer_list<std::shared_ptr<Expression>> arguments) : fname(fname), arguments(arguments) {}

    void Func::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        for (auto operand : arguments)
        {
            operand->get_all_choice_nodes(choice_nodes);
        }
    }

    std::shared_ptr<Expression> Func::bind(const BindingMap& binding) const
    {
        std::vector<std::shared_ptr<Expression>> new_arguments;
        for (auto operand : arguments)
        {
            new_arguments.push_back(operand->bind(binding));
        }
        return std::make_shared<Func>(fname, new_arguments);
    }

    std::shared_ptr<Expression> Func::evaluate(const BindingMap& binding) const
    {
        // TODO: evaluate different functions
        std::vector<std::shared_ptr<Expression>> args;
        for (auto operand : arguments)
        {
            args.push_back(operand->evaluate(binding));
        }
        return std::make_shared<Func>(fname, args);
    }

    void Func::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        for (auto operand : arguments)
        {
            operand->pick_useful_binding(binding, useful_binding);
        }
    }

    cp::Expression Func::to_arrow_expr() const
    {
        std::vector<cp::Expression> arrow_arguments;
        for (auto argument : arguments)
        {
            arrow_arguments.push_back(argument->to_arrow_expr());
        }
        if (fname == "int")
        {
            auto cast_options = cp::CastOptions::Unsafe(ar::TypeHolder(ar::int64()));
            return cp::call("cast", arrow_arguments, cast_options);
        }
        else {
            return cp::call(fname, arrow_arguments);
        }
    }

    std::string Func::to_string() const
    {
        if (fname == "int") {
            return "CAST(" + arguments[0]->to_string() + " AS INTEGER)";
        }
        std::string result = fname + "(";
        for (int i = 0; i < arguments.size(); i++)
        {
            if (i > 0) result += ", ";
            result += arguments[i]->to_string();
        }
        if (fname == "count" && arguments.size() == 0)
        {
            result += "*";
        }
        result += ")";
        return result;
    }
}
