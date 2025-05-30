#include "expression.h"

#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{
    IntConst::IntConst(int64_t value) : value(value) {}

    void IntConst::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) {}

    std::shared_ptr<Expression> IntConst::bind(const BindingMap& binding) const
    {
        return std::make_shared<IntConst>(value);
    }

    void IntConst::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        // do nothing
    }

    std::shared_ptr<Expression> IntConst::evaluate(const BindingMap& binding) const
    {
        return std::make_shared<IntConst>(value);
    }

    std::shared_ptr<arrow::Scalar> IntConst::to_arrow_scalar() const
    {
        return arrow::MakeScalar(value);
    }

    cp::Expression IntConst::to_arrow_expr() const
    {
        return cp::literal(value);
    }

    std::string IntConst::to_string() const
    {
        return std::to_string(value);
    }
}