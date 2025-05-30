#include "expression.h"

#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{
    ColumnRef::ColumnRef(const std::string& col, const std::string& table) : col(col), table(table) {}

    void ColumnRef::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) {}

    std::shared_ptr<Expression> ColumnRef::bind(const BindingMap& binding) const
    {
        return std::make_shared<ColumnRef>(col, table);
    }

    void ColumnRef::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        // do nothing
    }

    std::shared_ptr<Expression> ColumnRef::evaluate(const BindingMap& binding) const
    {
        return std::make_shared<ColumnRef>(col, table);
    }

    std::shared_ptr<arrow::Scalar> ColumnRef::to_arrow_scalar() const
    {
        throw std::runtime_error("try to convert a column reference to arrow scalar");
    }

    cp::Expression ColumnRef::to_arrow_expr() const
    {
        return cp::field_ref(col);
    }

    std::string ColumnRef::to_string() const
    {
        return col;
    }
}
