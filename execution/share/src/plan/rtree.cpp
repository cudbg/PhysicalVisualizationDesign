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
     *  RTreeImpl
     */

    std::shared_ptr<TableData> RTreeImpl::query(std::vector<double> lowers, std::vector<double> uppers) {
        std::vector<int64_t> rows;
        auto callback = [&rows](int64_t row) {
            rows.push_back(row);
            return true;
        };
        switch (dim) {
            case 1: {
                std::get<std::shared_ptr<RTree1D>>(rtree)->Search(lowers.data(), uppers.data(), callback);
                break;
            }
            case 2: {
                std::get<std::shared_ptr<RTree2D>>(rtree)->Search(lowers.data(), uppers.data(), callback);
                break;
            }
            case 3: {
                std::get<std::shared_ptr<RTree3D>>(rtree)->Search(lowers.data(), uppers.data(), callback);
                break;
            }
            default: {
                throw std::runtime_error("RTreeImpl: invalid dimension");
            }
        }

        return table->select_rows(rows);
    }

    void RTreeImpl::serialize(std::shared_ptr<ar::io::BufferOutputStream> out)
    {
        throw std::runtime_error("RTreeImpl: serialize not implemented");
    }

    void RTreeImpl::deserialize(std::shared_ptr<ar::io::BufferReader> buffer)
    {
        throw std::runtime_error("RTreeImpl: deserialize not implemented");
    }

    uint64_t RTreeImpl::size()
    {
        uint64_t total_size = 0;
        total_size += table->size();
        total_size += sizeof(dim);
        switch (dim) {
            case 1: {
                total_size += std::get<std::shared_ptr<RTree1D>>(rtree)->size();
                break;
            }
            case 2: {
                total_size += std::get<std::shared_ptr<RTree2D>>(rtree)->size();
                break;
            }
            case 3: {
                total_size += std::get<std::shared_ptr<RTree3D>>(rtree)->size();
                break;
            }
            default: {
                throw std::runtime_error("RTreeImpl: invalid dimension");
            }
        }
        return total_size;
    }

    /*
     *  RTreeBuild
     */

    std::vector<std::shared_ptr<Plan>> RTreeBuild::input_plans() const
    {
        return {input};
    }

    void RTreeBuild::execute(const BindingMap& binding, execute_callback_t cb)
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

            RTreeImpl::RTree_T rtree;
            int dim = keys.size();
            if (dim == 1) {
                rtree = std::make_shared<RTreeImpl::RTree1D>();
            }
            else if (dim == 2) {
                rtree = std::make_shared<RTreeImpl::RTree2D>();
            }
            else if (dim == 3) {
                rtree = std::make_shared<RTreeImpl::RTree3D>();
            }
            else {
                throw std::runtime_error("RTreeBuild: invalid dimension");
            }

            for (int64_t i = 0; i < keys_value->num_rows(); i++) {
                std::vector<double> key;

                for (int j = 0; j < this->keys.size(); j++) {
                    auto scalar = keys_value->column(j)->GetScalar(i).ValueOrDie()->CastTo(ar::float64()).ValueOrDie();
                    key.push_back(static_cast<const ar::DoubleScalar &>(*scalar).value);
                }
                if (dim == 1) {
                    std::get<std::shared_ptr<RTreeImpl::RTree1D>>(rtree)->Insert(key.data(), key.data(), i);
                } else if (dim == 2) {
                    std::get<std::shared_ptr<RTreeImpl::RTree2D>>(rtree)->Insert(key.data(), key.data(), i);
                } else {
                    std::get<std::shared_ptr<RTreeImpl::RTree3D>>(rtree)->Insert(key.data(), key.data(), i);
                }
            }

            auto rtree_impl = std::make_shared<RTreeImpl>(table, dim, rtree);
            metrics.record_output(nullptr, rtree_impl->size());
            cb(rtree_impl);
        });
    }

    void RTreeBuild::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
        for (auto& key : keys) {
            key->pick_useful_binding(binding, useful_binding);
        }
    }

    std::string RTreeBuild::to_sql(const BindingMap& binding) const
    {
        throw std::runtime_error("to_sql not implemented for RTreeBuild");
    }

    std::string RTreeBuild::to_string() const
    {
        std::string keys_str;
        for (auto& key : keys) {
            keys_str += key->to_string() + ",";
        }
        return "RTreeBuild[" + std::to_string(id) + "]{keys=" + keys_str + "}\n|\n" + input->to_string();
    }

    void RTreeBuild::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        for (auto& key : keys) {
            key->get_all_choice_nodes(choice_nodes);
        }
        input->get_all_choice_nodes(choice_nodes);
    }


    /*
     *  RTreeQuery
     */

    std::vector<std::shared_ptr<Plan>> RTreeQuery::input_plans() const
    {
        return {input};
    }

    void RTreeQuery::execute(const BindingMap& binding, execute_callback_t cb)
    {
        input->execute(binding, [this, binding, cb](std::shared_ptr<SerialData> data) {
            std::shared_ptr<RTreeImpl> rtree = std::dynamic_pointer_cast<RTreeImpl>(data);
            metrics.record_input(rtree->table->table);

            std::vector<double> lowers, uppers;
            for (auto q : this->lowers) {
                auto val = q->evaluate(binding);
                auto literal = std::dynamic_pointer_cast<Literal>(val);
                if (literal == nullptr) {
                    throw std::runtime_error("lower is not a literal");
                }
                auto scalar = literal->to_arrow_scalar()->CastTo(ar::float64()).ValueOrDie();
                lowers.push_back(static_cast<const ar::DoubleScalar &>(*scalar).value);
            }
            for (auto q : this->uppers) {
                auto val = q->evaluate(binding);
                auto literal = std::dynamic_pointer_cast<Literal>(val);
                if (literal == nullptr) {
                    throw std::runtime_error("upper is not a literal");
                }
                auto scalar = literal->to_arrow_scalar()->CastTo(ar::float64()).ValueOrDie();
                uppers.push_back(static_cast<const ar::DoubleScalar &>(*scalar).value);
            }
            auto table = rtree->query(lowers, uppers);
            metrics.record_output(table->table);
            cb(table);
        });
    }

    void RTreeQuery::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
        for (auto& query : lowers) {
            query->pick_useful_binding(binding, useful_binding);
        }
        for (auto& query : uppers) {
            query->pick_useful_binding(binding, useful_binding);
        }
    }

    std::string RTreeQuery::to_sql(const BindingMap& binding) const
    {
        throw std::runtime_error("to_sql not implemented for RTreeQuery");
    }

    std::string RTreeQuery::to_string() const
    {
        std::string lowers_str, uppers_str;
        for (int i = 0; i < lowers.size(); i++) {
            lowers_str += "(" + lowers[i]->to_string() + ")";
        }
        for (int i = 0; i < uppers.size(); i++) {
            uppers_str += "(" + uppers[i]->to_string() + ")";
        }
        return "RTreeQuery[" + std::to_string(id) + "]{lowers=" + lowers_str + "; uppers=" + uppers_str + "}\n|\n" + input->to_string();
    }

    void RTreeQuery::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        for (auto& query : lowers) {
            query->get_all_choice_nodes(choice_nodes);
        }
        for (auto& query : uppers) {
            query->get_all_choice_nodes(choice_nodes);
        }
        input->get_all_choice_nodes(choice_nodes);
    }
} // namespace pvd
