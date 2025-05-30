#include "expression.h"

#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{
    cp::Expression Expression::to_arrow_expr(const BindingMap& binding) const
    {
        auto bound_expr = bind(binding);
        return bound_expr->to_arrow_expr();
    }
}
