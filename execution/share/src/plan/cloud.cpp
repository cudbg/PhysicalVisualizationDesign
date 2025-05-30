#include "plan.h"
#include "expression.h"
#include "binding.h"
#include "cloud_api.h"

namespace pvd
{
    Cloud::Cloud(int id, std::shared_ptr<Plan> input)
            : Plan(id), input(input) {
        metrics.id = id;
        metrics.node = "cloud";
    }

    std::vector<std::shared_ptr<Plan>> Cloud::input_plans() const
    {
        return {input};
    }

    void Cloud::execute(const BindingMap& binding, execute_callback_t cb)
    {
        metrics.record_input(nullptr);
        cloud->query(to_sql(binding), [this, cb](std::shared_ptr<ar::Table> table) {
            metrics.record_output(table);
            cb(std::make_shared<TableData>(table));
        });
    }

    void Cloud::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
    }

    void Cloud::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        input->get_all_choice_nodes(choice_nodes);
    }

    std::string Cloud::to_sql(const BindingMap& binding) const
    {
        return input->to_sql(binding);
    }

    std::string Cloud::to_string() const
    {
        return "Cloud[" + std::to_string(id) + "]\n|\n" + input->to_string();
    }
}
