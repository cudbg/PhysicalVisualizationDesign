#include "expression.h"

#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{
    BoolConst::BoolConst(bool value) : value(value) {}

    void BoolConst::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) {}

    std::shared_ptr<Expression> BoolConst::bind(const BindingMap& binding) const
    {
        return std::make_shared<BoolConst>(value);
    }

    void BoolConst::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        // do nothing
    }

    std::shared_ptr<Expression> BoolConst::evaluate(const BindingMap& binding) const
    {
        return std::make_shared<BoolConst>(value);
    }

    std::shared_ptr<arrow::Scalar> BoolConst::to_arrow_scalar() const
    {
        return arrow::MakeScalar(value);
    }

    cp::Expression BoolConst::to_arrow_expr() const
    {
        return cp::literal(value);
    }

    std::string BoolConst::to_string() const
    {
        return std::to_string(value);
    }
}
