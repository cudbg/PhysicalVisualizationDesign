#include "plan.h"
#include "binding.h"


namespace pvd
{
    SCache::SCache(int id, std::shared_ptr<Plan> input) : Plan(id), input(input) {
        metrics.id = id;
        metrics.node = "SCache";
    }

    std::vector<std::shared_ptr<Plan>> SCache::input_plans() const
    {
        return {input};
    }

    std::vector<BindingMap> get_all_binding(
            int current,
            std::vector<std::string> &ids,
            std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> choices)
    {
        std::vector<BindingMap> bindings;
        if (current == ids.size()) {
            bindings.emplace_back();
        }
        else {
            auto sub_bindings = get_all_binding(current + 1, ids, choices);
            auto choice = choices[ids[current]];
            for (auto& binding : choice->all_choices()) {
                for (auto& sub_binding : sub_bindings) {
                    auto new_binding = sub_binding;
                    new_binding.insert(std::make_pair(ids[current], binding));
                    bindings.emplace_back(std::move(new_binding));
                }
            }
        }
        return bindings;
    }

    void SCache::_cache_data(build_callback_t cb, std::shared_ptr<std::vector<BindingMap>> bindings, int i)
    {
        //std::cout << "SCache Caching " << i << "/" << bindings->size() << std::endl;

        if (i == bindings->size()) {
            cb();
        }
        else {
            input->execute(bindings->at(i), [this, bindings, cb, i](std::shared_ptr<SerialData> output) {
                this->data[hash_binding(bindings->at(i))] = output;
                metrics.record_input(nullptr);
                metrics.record_output(nullptr, output->size());
                _cache_data(cb, bindings, i + 1);
            });
        }
    }

    void SCache::cache_data(build_callback_t cb)
    {
        //std::cout << "SCache Caching" << std::endl;
        std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> choices;
        input->get_all_choice_nodes(choices);
        std::vector<std::string> choice_ids;
        for (auto& choice : choices) {
            choice_ids.push_back(choice.first);
        }
        auto all_bindings = std::make_shared<std::vector<BindingMap>>(get_all_binding(0, choice_ids, choices));

        _cache_data(cb, all_bindings, 0);
    }

    void SCache::execute(const BindingMap& binding, execute_callback_t cb)
    {
        BindingMap useful_binding;
        input->pick_useful_binding(binding, useful_binding);
        auto hash = hash_binding(useful_binding);
        //std::cout << "SCache Executed" << std::endl;
        cb(data.at(hash));
    }

    void SCache::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
    }

    void SCache::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        input->get_all_choice_nodes(choice_nodes);
    }

    std::string SCache::to_sql(const BindingMap& binding) const
    {
        return input->to_sql(binding);
    }

    std::string SCache::to_string() const
    {
        return "SCache[" + std::to_string(id) + "]\n" + "|\n" + input->to_string();
    }
}
