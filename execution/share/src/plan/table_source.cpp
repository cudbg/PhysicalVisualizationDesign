#include "plan.h"
#include "expression.h"
#include "binding.h"
#include "cloud_api.h"
#include "arrow_utils.h"

namespace pvd
{
    TableSource::TableSource(int id, std::string name) : Plan(id), table_name(name) {}

    std::vector<std::shared_ptr<Plan>> TableSource::input_plans() const
    {
        return {};
    }

    void TableSource::execute(const BindingMap& binding, execute_callback_t cb)
    {
        throw std::runtime_error("TableSource::execute not implemented");
    }

    void TableSource::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        // do nothing
    }

    void TableSource::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        // do nothing
    }

    std::string TableSource::to_sql(const BindingMap& binding) const
    {
        return "SELECT * FROM " + table_name;
    }

    std::string TableSource::to_string() const
    {
        return "TableSource[" + std::to_string(id) + "]{" + table_name + "}";
    }
}
