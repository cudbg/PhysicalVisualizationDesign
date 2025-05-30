#include "expression.h"

#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{
    StringConst::StringConst(const std::string& value) : value(value) {}

    void StringConst::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) {}

    std::shared_ptr<Expression> StringConst::bind(const BindingMap& binding) const
    {
        return std::make_shared<StringConst>(value);
    }

    void StringConst::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        // do nothing
    }

    std::shared_ptr<Expression> StringConst::evaluate(const BindingMap& binding) const
    {
        return std::make_shared<StringConst>(value);
    }

    std::shared_ptr<arrow::Scalar> StringConst::to_arrow_scalar() const
    {
        return arrow::MakeScalar(value);
    }

    cp::Expression StringConst::to_arrow_expr() const
    {
        return cp::literal(value);
    }

    std::string StringConst::to_string() const
    {
        return value;
    }
}
