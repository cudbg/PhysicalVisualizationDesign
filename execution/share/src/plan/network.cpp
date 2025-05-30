#include "plan.h"
#include "expression.h"
#include "binding.h"
#include "network.h"
#include "arrow_utils.h"

namespace pvd
{
    Network::Network(int id, std::shared_ptr<Plan> input)
            : Plan(id), input(input) {
        metrics.id = id;
        metrics.node = "Network";
    }

    std::vector<std::shared_ptr<Plan>> Network::input_plans() const
    {
        return {input};
    }

    void Network::execute(const BindingMap& binding, execute_callback_t cb)
    {
        json binding_json;
        binding_to_json(binding, binding_json);
        std::string binding_str = binding_json.dump();
        Query query = {Query::Execute, input->id, static_cast<int64_t>(binding_str.size() + 1)};

        metrics.record_input(nullptr);
        SENDER->send(query, (void*)binding_str.c_str(), [this, cb](Reply reply) {
            auto builder = ar::BufferBuilder();
            std::cout << "Network::execute: " << reply.size << std::endl;
            { auto _ = builder.Append((void*) reply.data, reply.size); }
            auto buffer = builder.Finish().ValueOrDie();
            auto reader = std::make_shared<ar::io::BufferReader>(buffer);
            auto node = input;
            while (std::dynamic_pointer_cast<DCache>(node) || std::dynamic_pointer_cast<SCache>(node)) {
                node = node->input_plans()[0];
            }
            if (auto hashtable = std::dynamic_pointer_cast<HashTableBuild>(node)) {
                auto ht_impl = std::make_shared<HashTableImpl>();
                ht_impl->deserialize(reader);
                metrics.record_output(nullptr, ht_impl->size());
                cb(ht_impl);
            } else if (auto prefixsum = std::dynamic_pointer_cast<PrefixSumBuild>(node)) {
                auto prefixsum_impl = std::make_shared<PrefixSumImpl>();
                prefixsum_impl->deserialize(reader);
                metrics.record_output(nullptr, prefixsum_impl->size(), prefixsum_impl->target_col_data->table->num_rows(),
                                      prefixsum_impl->sum_col_data->table->num_rows());
                cb(prefixsum_impl);
            } else if (auto prefixsum2d = std::dynamic_pointer_cast<PrefixSum2DBuild>(node)) {
                auto prefixsum2d_impl = std::make_shared<PrefixSum2DImpl>();
                prefixsum2d_impl->deserialize(reader);
                metrics.record_output(nullptr, prefixsum2d_impl->size(), prefixsum2d_impl->target_col_data->table->num_rows(),
                                      prefixsum2d_impl->sum_col_x_data->table->num_rows() * prefixsum2d_impl->sum_col_y_data->table->num_rows());
                cb(prefixsum2d_impl);
            } else {
                auto table = std::make_shared<TableData>();
                table->deserialize(reader);
                metrics.record_output(table->table);
                cb(table);
            }
        });
    }

    void Network::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
    }

    void Network::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        input->get_all_choice_nodes(choice_nodes);
    }

    std::string Network::to_sql(const BindingMap& binding) const
    {
        return "TODO TO SQL"; // TODO
    }

    std::string Network::to_string() const
    {
        return "Network[" + std::to_string(id) + "]\n|\n" + input->to_string();
    }
}
