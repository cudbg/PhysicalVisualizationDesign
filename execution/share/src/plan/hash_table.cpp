#include "arrow_utils.h"
#include "plan.h"
#include "expression.h"
#include "binding.h"
#include "cloud_api.h"

namespace ar = ar;
namespace cp = ar::compute;
namespace ac = ar::acero;

namespace pvd
{
    /*
     *  HashTableImpl
     */

    std::shared_ptr<TableData> HashTableImpl::query(uint64_t key)
    {
        if (table->find(key) == table->end()) {
            //std::cout << "Query key not found: " << key << std::endl;
            //std::cout << (unsigned long long)empty_table.get() << std::endl;
            return empty_table;
        }
        else {
            return table->at(key);
        }
    }

    void HashTableImpl::serialize(std::shared_ptr<ar::io::BufferOutputStream> out)
    {
        uint64_t table_size = table->size();
        { auto _ = out->Write(reinterpret_cast<const uint8_t*>(&table_size), sizeof(table_size)); }
        for (const auto& kv : *table) {
            { auto _ = out->Write(reinterpret_cast<const uint8_t*>(&kv.first), sizeof(kv.first)); }
            kv.second->serialize(out);
        }
    }

    void HashTableImpl::deserialize(std::shared_ptr<ar::io::BufferReader> buffer)
    {
        uint64_t table_size = *reinterpret_cast<const uint64_t*>(buffer->Read(sizeof(uint64_t)).ValueOrDie()->data());
        table = std::make_shared<HashTable>();
        while (table_size--) {
            uint64_t key = *reinterpret_cast<const uint64_t*>(buffer->Read(sizeof(uint64_t)).ValueOrDie()->data());
            auto table_data = std::make_shared<TableData>();
            table_data->deserialize(buffer);
            table->emplace(key, table_data);
        }
        empty_table = std::make_shared<TableData>(empty_table_from_schema(table->begin()->second->table->schema()));
    }

    uint64_t HashTableImpl::size()
    {
        uint64_t total_size = 0;
        for (const auto& [key, data] : *table) {
            total_size += sizeof(key);
            total_size += data->size();
        }
        return total_size;
    }

    /*
     *  HashTableBuild
     */

    std::vector<std::shared_ptr<Plan>> HashTableBuild::input_plans() const
    {
        return {input};
    }

    void HashTableBuild::execute(const BindingMap& binding, execute_callback_t cb)
    {
        input->execute(binding, [this, cb](std::shared_ptr<SerialData> data) {
            std::shared_ptr<TableData> table = std::dynamic_pointer_cast<TableData>(data);
            metrics.record_input(table->table);

            std::vector<ar::Expression> ar_keys;
            for (auto key : this->keys) {
                ar_keys.push_back(key->to_arrow_expr());
            }

            auto table_source_option = ac::TableSourceNodeOptions{table->table, MAX_BATCH_SIZE};
            auto source = ac::Declaration("table_source", {}, table_source_option);
            auto proj_option = ac::ProjectNodeOptions{ar_keys};
            auto calc_keys = ac::Declaration("project", {source}, proj_option);
            auto keys_value = ac::DeclarationToTable(calc_keys).ValueOrDie();

            std::unordered_map<uint64_t, std::vector<int64_t>> tmp_ht;

            for (int64_t i = 0; i < keys_value->num_rows(); i++) {
                std::vector<std::shared_ptr<ar::Scalar>> array;
                for (int j = 0; j < this->keys.size(); j++) {
                    array.push_back(keys_value->column(j)->GetScalar(i).ValueOrDie());
                }
                uint64_t key_hash = hash_scalars(array);
                if (tmp_ht.contains(key_hash)) {
                    tmp_ht[key_hash].push_back(i);
                }
                else {
                    tmp_ht[key_hash] = {i};
                }
            }

            auto hashtable = std::make_shared<HashTableImpl::HashTable>();

            for (auto& [key_hash, key_indices] : tmp_ht) {
                hashtable->emplace(key_hash, table->select_rows(key_indices));
            }
            auto empty_table = std::make_shared<TableData>(empty_table_from_schema(table->table->schema()));
            auto ht_impl = std::make_shared<HashTableImpl>(hashtable, empty_table);
            metrics.record_output(nullptr, ht_impl->size());
            cb(ht_impl);
        });
    }

    void HashTableBuild::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
        for (auto& key : keys) {
            key->pick_useful_binding(binding, useful_binding);
        }
    }

    std::string HashTableBuild::to_sql(const BindingMap& binding) const
    {
        throw std::runtime_error("to_sql not implemented for HashTableBuild");
    }

    std::string HashTableBuild::to_string() const
    {
        std::string keys_str;
        for (auto& key : keys) {
            keys_str += key->to_string() + ",";
        }
        return "HashTableBuild[" + std::to_string(id) + "]{keys=" + keys_str + "}\n|\n" + input->to_string();
    }

    void HashTableBuild::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        for (auto& key : keys) {
            key->get_all_choice_nodes(choice_nodes);
        }
        input->get_all_choice_nodes(choice_nodes);
    }


    /*
     *  HashTableQuery
     */

    std::vector<std::shared_ptr<Plan>> HashTableQuery::input_plans() const
    {
        return {input};
    }

    void HashTableQuery::execute(const BindingMap& binding, execute_callback_t cb)
    {
        input->execute(binding, [this, binding, cb](std::shared_ptr<SerialData> data) {
            //std::cout << "Executing HashTableQuery" << std::endl;
            std::shared_ptr<HashTableImpl> hashtable = std::dynamic_pointer_cast<HashTableImpl>(data);
            //std::cout << "Executing HashTableQuery " << hashtable->table->size() << std::endl;
            metrics.record_input(nullptr);

            std::vector<std::shared_ptr<ar::Scalar>> query;
            for (auto q : this->queries) {
                auto val = q->evaluate(binding);
                //std::cout << "Query value: " << val->to_string() << std::endl;
                auto literal = std::dynamic_pointer_cast<Literal>(val);
                if (literal == nullptr) {
                    throw std::runtime_error("query is not a literal");
                }
                query.push_back(literal->to_arrow_scalar());
            }
            auto query_hash = hash_scalars(query);
            //std::cout << "Query hash: " << query_hash << std::endl;
            auto table = hashtable->query(query_hash);
            metrics.record_output(table->table);
            cb(table);
        });
    }

    void HashTableQuery::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
        for (auto& query : queries) {
            query->pick_useful_binding(binding, useful_binding);
        }
    }

    std::string HashTableQuery::to_sql(const BindingMap& binding) const
    {
        throw std::runtime_error("to_sql not implemented for HashTableQuery");
    }

    std::string HashTableQuery::to_string() const
    {
        std::string query;
        for (int i = 0; i < queries.size(); i++) {
            query += "(" + queries[i]->to_string() + ")";
        }
        return "HashTableQuery[" + std::to_string(id) + "]{" + query + "}\n|\n" + input->to_string();
    }

    void HashTableQuery::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        for (auto& query : queries) {
            query->get_all_choice_nodes(choice_nodes);
        }
        input->get_all_choice_nodes(choice_nodes);
    }
} // namespace pvd
