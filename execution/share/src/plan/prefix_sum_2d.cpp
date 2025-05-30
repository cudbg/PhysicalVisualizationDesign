#include "arrow_utils.h"
#include "plan.h"
#include "expression.h"
#include "binding.h"
#include "cloud_api.h"

#include <iostream>

namespace ar = ar;
namespace cp = ar::compute;
namespace ac = ar::acero;

namespace pvd
{
    /*
     *  PrefixSum2DImpl
     */

    // lower_than == true: find the last element that is less than value
    // lower_than == false: find the last element that is less than or equal to value
    int64_t PrefixSum2DImpl::get_sum_col_idx(std::shared_ptr<TableData> sum_col_data, std::shared_ptr<ar::Scalar> value, bool lower_than)
    {
        CmpScalar cmp_scalar;

        int64_t left = -1;
        int64_t right = sum_col_data->table->num_rows();
        auto data = sum_col_data->table->column(0);
        while (right - left > 1) {
            int64_t mid = (left + right) / 2;
            auto mid_val = data->GetScalar(mid).ValueOrDie();
            if (lower_than) {
                if (cmp_scalar(mid_val, value)) {
                    left = mid;
                }
                else {
                    right = mid;
                }
            }
            else {
                if (cmp_scalar(value, mid_val)) {
                    right = mid;
                }
                else {
                    left = mid;
                }
            }
        }
        return left;
    }

    std::shared_ptr<TableData> PrefixSum2DImpl::query(std::shared_ptr<ar::Scalar> lower_x, std::shared_ptr<ar::Scalar> upper_x,
                                     std::shared_ptr<ar::Scalar> lower_y, std::shared_ptr<ar::Scalar> upper_y)
    {
        ar::DoubleBuilder avg_col;
        int64_t lower_x_idx = get_sum_col_idx(sum_col_x_data, lower_x, true);
        int64_t upper_x_idx = get_sum_col_idx(sum_col_x_data, upper_x, false);
        int64_t lower_y_idx = get_sum_col_idx(sum_col_y_data, lower_y, true);
        int64_t upper_y_idx = get_sum_col_idx(sum_col_y_data, upper_y, false);
        int64_t num_rows = target_col_data->table->num_rows();
        int64_t y_size = sum_col_y_data->table->num_rows();

        /*
         * ll ------ lu
         * |         |     uu - ul - lu + ll
         * ul ------ uu
         */
        int64_t uu = upper_x_idx * y_size + upper_y_idx;
        int64_t ul = upper_x_idx * y_size + lower_y_idx;
        int64_t lu = lower_x_idx * y_size + upper_y_idx;
        int64_t ll = lower_x_idx * y_size + lower_y_idx;

        for (int64_t i = 0; i < num_rows; i++) {
            double val;
            if (upper_x_idx == -1 || upper_y_idx == -1) {
                val = 0;
            }
            else if (lower_x_idx == -1) {
                if (lower_y_idx == -1) {
                    val = prefix_sum->at(uu * num_rows + i);
                }
                else {
                    val = prefix_sum->at(uu * num_rows + i) - prefix_sum->at(ul * num_rows + i);
                }
            }
            else {
                if (lower_y_idx == -1) {
                    val = prefix_sum->at(uu * num_rows + i) - prefix_sum->at(lu * num_rows + i);
                }
                else {
                    val = prefix_sum->at(uu * num_rows + i) - prefix_sum->at(ul * num_rows + i) -\
                            prefix_sum->at(lu * num_rows + i) + prefix_sum->at(ll * num_rows + i);
                }
            }
            { auto _ = avg_col.Append(val); }
        }
        auto avg_col_array = avg_col.Finish().ValueOrDie();
        auto table = ar::Table::Make(output_schema,
                                     {target_col_data->table->column(0),
                                      std::make_shared<ar::ChunkedArray>(avg_col_array)});
        return std::make_shared<TableData>(table);
    }

    void PrefixSum2DImpl::serialize(std::shared_ptr<ar::io::BufferOutputStream> out) {

        int64_t sum_col_x_size = sum_col_x_data->table->num_rows();
        { auto _ = out->Write(reinterpret_cast<const uint8_t *>(&sum_col_x_size), sizeof(sum_col_x_size)); }
        int64_t sum_col_y_size = sum_col_y_data->table->num_rows();
        { auto _ = out->Write(reinterpret_cast<const uint8_t *>(&sum_col_y_size), sizeof(sum_col_y_size)); }
        int64_t target_col_size = target_col_data->table->num_rows();
        { auto _ = out->Write(reinterpret_cast<const uint8_t *>(&target_col_size), sizeof(target_col_size)); }
        for (int64_t i = 0; i < sum_col_x_size * sum_col_y_size * target_col_size; i++) {
            { auto _ = out->Write(reinterpret_cast<const uint8_t *>(&prefix_sum->at(i)), sizeof(prefix_sum->at(i))); }
        }
        auto name = output_schema->field(1)->name();
        int64_t size = name.size();
        { auto _ = out->Write(reinterpret_cast<const uint8_t *>(&size), sizeof(size)); }
        { auto _ = out->Write(reinterpret_cast<const uint8_t *>(name.c_str()), size + 1); }
        sum_col_x_data->serialize(out);
        sum_col_y_data->serialize(out);
        target_col_data->serialize(out);
    }

    void PrefixSum2DImpl::deserialize(std::shared_ptr<ar::io::BufferReader> buffer)
    {
        int64_t sum_col_x_size = *reinterpret_cast<const int64_t *>(buffer->Read(sizeof(int64_t)).ValueOrDie()->data());
        int64_t sum_col_y_size = *reinterpret_cast<const int64_t *>(buffer->Read(sizeof(int64_t)).ValueOrDie()->data());
        int64_t target_col_size = *reinterpret_cast<const int64_t *>(buffer->Read(sizeof(int64_t)).ValueOrDie()->data());
        prefix_sum = std::make_shared<std::vector<double>>();
        for (int64_t i = 0; i < sum_col_x_size * sum_col_y_size * target_col_size; i++) {
            prefix_sum->push_back(*reinterpret_cast<const double *>(buffer->Read(sizeof(double)).ValueOrDie()->data()));
        }
        int64_t size = *reinterpret_cast<const int64_t *>(buffer->Read(sizeof(int64_t)).ValueOrDie()->data());
        std::string agg_col_name(reinterpret_cast<const char *>(buffer->Read(size + 1).ValueOrDie()->data()));
        sum_col_x_data = std::make_shared<TableData>();
        sum_col_x_data->deserialize(buffer);
        sum_col_y_data = std::make_shared<TableData>();
        sum_col_y_data->deserialize(buffer);
        target_col_data = std::make_shared<TableData>();
        target_col_data->deserialize(buffer);

        auto target_field = target_col_data->table->schema()->field(0)->Copy();
        output_schema = std::make_shared<ar::Schema>(ar::Schema({target_field, ar::field(agg_col_name, ar::float64())}));
    }

    uint64_t PrefixSum2DImpl::size()
    {
        uint64_t total_size = 0;
        total_size += target_col_data->size();
        total_size += sum_col_x_data->size();
        total_size += sum_col_y_data->size();
        total_size += prefix_sum->size() * sizeof(double);
        return total_size;
    }

    /*
     *  PrefixSum2DBuild
     */

    std::vector<std::shared_ptr<Plan>> PrefixSum2DBuild::input_plans() const
    {
        return {input};
    }

    void PrefixSum2DBuild::execute(const BindingMap& binding, execute_callback_t cb)
    {
        input->execute(binding, [this, binding, cb](std::shared_ptr<SerialData> data) {
            std::shared_ptr<TableData> table = std::dynamic_pointer_cast<TableData>(data);
            metrics.record_input(table->table);
            auto table_source_option = ac::TableSourceNodeOptions{table->table, MAX_BATCH_SIZE};
            auto source = ac::Declaration("table_source", {}, table_source_option);

            std::vector<cp::Expression> proj_columns = {sum_col_x->bind(binding)->to_arrow_expr(),
                                                        sum_col_y->bind(binding)->to_arrow_expr(),
                                                        target_col->bind(binding)->to_arrow_expr(),
                                                        agg_col->bind(binding)->to_arrow_expr()};
            std::vector<std::string> proj_names = {sum_col_x_name, sum_col_y_name, target_col_name, agg_col_name};

            auto proj_option = ac::ProjectNodeOptions{proj_columns, proj_names};
            auto proj_plan = ac::Declaration("project", {source}, proj_option);

            std::vector<arrow::FieldRef> keys = {sum_col_x_name, sum_col_y_name, target_col_name};
            auto options = std::make_shared<cp::ScalarAggregateOptions>();

            auto aggregate_options = ac::AggregateNodeOptions{{{"hash_sum", options, agg_col_name, agg_col_name}}, keys};

            ac::Declaration aggregate{"aggregate", {std::move(proj_plan)}, std::move(aggregate_options)};

            auto aggregate_table = ac::DeclarationToTable(aggregate).ValueOrDie();

            auto sum_col_x_data = aggregate_table->column(0);
            auto sum_col_y_data = aggregate_table->column(1);
            auto target_col_data = aggregate_table->column(2);
            auto agg_col_data = aggregate_table->column(3);

            auto sum_x_builder = arrow::MakeBuilder(sum_col_x_data->type()).ValueOrDie();
            auto sum_y_builder = arrow::MakeBuilder(sum_col_y_data->type()).ValueOrDie();
            auto target_builder = arrow::MakeBuilder(target_col_data->type()).ValueOrDie();
            std::unordered_map<uint64_t, int64_t> sum_col_x_idx;
            std::unordered_map<uint64_t, int64_t> sum_col_y_idx;
            std::unordered_map<uint64_t, int64_t> target_col_idx;
            std::map<std::pair<uint64_t, uint64_t>, double> values;

            std::unordered_set<uint64_t> scalar_set;
            std::vector<std::shared_ptr<ar::Scalar>> scalar_values;
            CmpScalar cmp_scalar;

            // sort sum_col_x_data
            scalar_set.clear();
            scalar_values.clear();
            for (int64_t i = 0; i < sum_col_x_data->length(); ++i) {
                auto scalar = sum_col_x_data->GetScalar(i).ValueOrDie();
                auto hash = scalar->hash();
                if (scalar_set.contains(hash)) continue;
                scalar_set.insert(hash);
                scalar_values.push_back(scalar);
            }
            int64_t total_x = scalar_values.size();
            std::sort(scalar_values.begin(), scalar_values.end(), cmp_scalar);

            for (int64_t i = 0; i < total_x; ++i) {
                sum_col_x_idx[scalar_values[i]->hash()] = i;
                sum_x_builder->AppendScalar(*scalar_values[i]);
            }

            // sort sum_col_y_data
            scalar_set.clear();
            scalar_values.clear();
            for (int64_t i = 0; i < sum_col_y_data->length(); ++i) {
                auto scalar = sum_col_y_data->GetScalar(i).ValueOrDie();
                auto hash = scalar->hash();
                if (scalar_set.contains(hash)) continue;
                scalar_set.insert(hash);
                scalar_values.push_back(scalar);
            }
            std::sort(scalar_values.begin(), scalar_values.end(), cmp_scalar);

            int64_t total_y = scalar_values.size();
            for (int64_t i = 0; i < total_y; ++i) {
                sum_col_y_idx[scalar_values[i]->hash()] = i;
                sum_y_builder->AppendScalar(*scalar_values[i]);
            }

            int64_t total_target = 0;
            for (int64_t i = 0; i < target_col_data->length(); i++) {
                auto value = target_col_data->GetScalar(i).ValueOrDie();
                auto hash = value->hash();
                if (!target_col_idx.contains(hash)) {
                    target_col_idx[hash] = total_target++;
                    target_builder->AppendScalar(*value);
                }
            }

            auto prefix_sum = std::make_shared<std::vector<double>>(total_x * total_y * total_target, 0);

            for (int64_t i = 0; i < sum_col_x_data->length(); i++) {
                uint64_t sum_x_hash = sum_col_x_data->GetScalar(i).ValueOrDie()->hash();
                uint64_t sum_y_hash = sum_col_y_data->GetScalar(i).ValueOrDie()->hash();
                uint64_t target_hash = target_col_data->GetScalar(i).ValueOrDie()->hash();
                int64_t sum_x_idx = sum_col_x_idx[sum_x_hash];
                int64_t sum_y_idx = sum_col_y_idx[sum_y_hash];
                int64_t target_idx = target_col_idx[target_hash];

                auto agg_val = agg_col_data->GetScalar(i).ValueOrDie();
                auto val = dynamic_pointer_cast<ar::DoubleScalar>(agg_val->CastTo(ar::float64()).ValueOrDie())->value;
                prefix_sum->at(sum_x_idx * total_y * total_target + sum_y_idx * total_target + target_idx) = val;
            }

            for (int64_t i = 0; i < total_x; i++) {
                for (int64_t j = 0; j < total_y; j++) {
                    for (int64_t k = 0; k < total_target; k++) {
                        if (i == 0 && j == 0) continue;
                        if (i == 0 && j > 0) {
                            prefix_sum->at(j * total_target + k) += prefix_sum->at((j - 1) * total_target + k);
                        } else if (i > 0 && j == 0) {
                            prefix_sum->at(i * total_y * total_target + k) += prefix_sum->at(
                                    (i - 1) * total_y * total_target + k);
                        } else {
                            prefix_sum->at(i * total_y * total_target + j * total_target + k) = \
                                prefix_sum->at(i * total_y * total_target + j * total_target + k) + \
                                prefix_sum->at((i - 1) * total_y * total_target + j * total_target + k) + \
                                prefix_sum->at(i * total_y * total_target + (j - 1) * total_target + k) - \
                                prefix_sum->at((i - 1) * total_y * total_target + (j - 1) * total_target + k);
                        }
                    }
                }
            }

            auto sum_col_x_array = sum_x_builder->Finish().ValueOrDie();
            auto _sum_col_x = std::make_shared<TableData>(
                    ar::Table::Make(ar::schema({aggregate_table->schema()->field(0)}), {sum_col_x_array}));

            auto sum_col_y_array = sum_y_builder->Finish().ValueOrDie();
            auto _sum_col_y = std::make_shared<TableData>(
                    ar::Table::Make(ar::schema({aggregate_table->schema()->field(1)}), {sum_col_y_array}));

            auto target_col_array = target_builder->Finish().ValueOrDie();
            auto _target_col = std::make_shared<TableData>(
                                    ar::Table::Make(ar::schema({aggregate_table->schema()->field(2)}), {target_col_array}));

            auto prefix_sum_impl = std::make_shared<PrefixSum2DImpl>(_sum_col_x, _sum_col_y, _target_col, agg_col_name, prefix_sum);
            metrics.record_output(nullptr, prefix_sum_impl->size(), _target_col->table->num_rows(), _sum_col_x->table->num_rows() * _sum_col_y->table->num_rows());
            cb(prefix_sum_impl);
        });
    }

    void PrefixSum2DBuild::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
        sum_col_x->pick_useful_binding(binding, useful_binding);
        sum_col_y->pick_useful_binding(binding, useful_binding);
        target_col->pick_useful_binding(binding, useful_binding);
        agg_col->pick_useful_binding(binding, useful_binding);
    }

    std::string PrefixSum2DBuild::to_sql(const BindingMap& binding) const
    {
        throw std::runtime_error("to_sql not implemented for PrefixSum2DBuild");
    }

    std::string PrefixSum2DBuild::to_string() const
    {
        return "PrefixSum2DBuild[" + std::to_string(id) + "]{sum_x=" + sum_col_x_name + "; sum_y=" + sum_col_y_name + "; target=" + target_col_name + "; agg=" + agg_col_name + "}\n|\n" + input->to_string();
    }

    void PrefixSum2DBuild::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        input->get_all_choice_nodes(choice_nodes);
        sum_col_x->get_all_choice_nodes(choice_nodes);
        sum_col_y->get_all_choice_nodes(choice_nodes);
        target_col->get_all_choice_nodes(choice_nodes);
        agg_col->get_all_choice_nodes(choice_nodes);
    }

    /*
     *  PrefixSum2DQuery
     */

    std::vector<std::shared_ptr<Plan>> PrefixSum2DQuery::input_plans() const
    {
        return {input};
    }

    void PrefixSum2DQuery::execute(const BindingMap& binding, execute_callback_t cb)
    {
        input->execute(binding, [this, binding, cb](std::shared_ptr<SerialData> data) {
            std::shared_ptr<PrefixSum2DImpl> prefix_sum = std::dynamic_pointer_cast<PrefixSum2DImpl>(data);
            metrics.record_input(nullptr, prefix_sum->target_col_data->table->num_rows(),
                                 prefix_sum->sum_col_x_data->table->num_rows() * prefix_sum->sum_col_y_data->table->num_rows());

            auto lower_x_val = lower_x->evaluate(binding);
            auto upper_x_val = upper_x->evaluate(binding);
            auto lower_y_val = lower_y->evaluate(binding);
            auto upper_y_val = upper_y->evaluate(binding);
            auto lower_x_literal = std::dynamic_pointer_cast<Literal>(lower_x_val);
            auto upper_x_literal = std::dynamic_pointer_cast<Literal>(upper_x_val);
            auto lower_y_literal = std::dynamic_pointer_cast<Literal>(lower_y_val);
            auto upper_y_literal = std::dynamic_pointer_cast<Literal>(upper_y_val);
            if (lower_x_literal == nullptr || upper_x_literal == nullptr || lower_y_literal == nullptr || upper_y_literal == nullptr) {
                throw std::runtime_error("lower or upper is not a literal");
            }
            auto table = prefix_sum->query(
                lower_x_literal->to_arrow_scalar(),
                upper_x_literal->to_arrow_scalar(),
                lower_y_literal->to_arrow_scalar(),
                upper_y_literal->to_arrow_scalar());
            metrics.record_output(table->table);
            cb(table);
        });
    }

    void PrefixSum2DQuery::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        input->pick_useful_binding(binding, useful_binding);
        lower_x->pick_useful_binding(binding, useful_binding);
        upper_x->pick_useful_binding(binding, useful_binding);
        lower_y->pick_useful_binding(binding, useful_binding);
        upper_y->pick_useful_binding(binding, useful_binding);
    }

    std::string PrefixSum2DQuery::to_sql(const BindingMap& binding) const
    {
        throw std::runtime_error("to_sql not implemented for PrefixSum2DQuery");
    }

    std::string PrefixSum2DQuery::to_string() const
    {
        return "PrefixSum2DQuery[" + std::to_string(id) + "]{lower_x=" + lower_x->to_string() + "; upper_x=" + upper_x->to_string() +  "; lower_y=" + lower_y->to_string() + "; upper_y=" + upper_y->to_string()+ "}\n|\n" + input->to_string();
    }

    void PrefixSum2DQuery::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        input->get_all_choice_nodes(choice_nodes);
        lower_x->get_all_choice_nodes(choice_nodes);
        upper_x->get_all_choice_nodes(choice_nodes);
        lower_y->get_all_choice_nodes(choice_nodes);
        upper_y->get_all_choice_nodes(choice_nodes);
    }
} // namespace pvd
