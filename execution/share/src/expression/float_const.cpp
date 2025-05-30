#include "expression.h"

#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{
    FloatConst::FloatConst(double value) : value(value) {}

    void FloatConst::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) {}

    std::shared_ptr<Expression> FloatConst::bind(const BindingMap& binding) const
    {
        return std::make_shared<FloatConst>(value);
    }

    void FloatConst::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        // do nothing
    }

    std::shared_ptr<Expression> FloatConst::evaluate(const BindingMap& binding) const
    {
        return std::make_shared<FloatConst>(value);
    }

    std::shared_ptr<arrow::Scalar> FloatConst::to_arrow_scalar() const
    {
        return arrow::MakeScalar(value);
    }

    cp::Expression FloatConst::to_arrow_expr() const
    {
        return cp::literal(value);
    }

    std::string FloatConst::to_string() const
    {
        return std::to_string(value);
    }
}
