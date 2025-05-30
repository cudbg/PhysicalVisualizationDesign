#include "plan.h"
#include "binding.h"

namespace pvd
{
    DCache::DCache(int id, std::shared_ptr<Plan> input) :
            Plan(id), input(input), data(nullptr), current_binding({}) {
        metrics.id = id;
        metrics.node = "DCache";
    }

    std::vector<std::shared_ptr<Plan>> DCache::input_plans() const
    {
        return {input};
    }

    void DCache::execute(const BindingMap& binding, execute_callback_t cb)
    {
        BindingMap useful_binding;
        input->pick_useful_binding(binding, useful_binding);

        if (data == nullptr || current_binding != useful_binding) {
            this->current_binding = useful_binding;
            input->execute(binding, [this, cb](std::shared_ptr<SerialData> output) {
                metrics.record_input(nullptr);
                metrics.record_output(nullptr, output->size());
                //std::cout << "Return from record_output" << std::endl;
                this->data = std::move(output);
                //std::cout << "Return from DCache" << std::endl;
                cb(this->data);
            });
        }
        else {
            cb(data);
        }
    }

    void DCache::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
    }

    void DCache::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        input->get_all_choice_nodes(choice_nodes);
    }

    std::string DCache::to_sql(const BindingMap& binding) const
    {
        return input->to_sql(binding);
    }

    std::string DCache::to_string() const
    {
        return "DCache[" + std::to_string(id) + "]\n" + "|\n" + input->to_string();
    }
}
