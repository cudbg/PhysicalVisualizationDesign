#pragma once

#include <iostream>
#include <vector>
#include <memory>
#include <variant>
#include <map>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>

#include "data.h"
#include "binding.h"
#include "expression.h"
#include "network.h"
#include "json.h"
#include "arrow_utils.h"
#include "rtree.h"
#include "metrics.h"

namespace ar = arrow;
namespace cp = arrow::compute;
namespace ac = arrow::acero;
using json = nlohmann::json;

namespace pvd
{
    typedef std::function<void(ac::Declaration plan)> compile_callback_t;
    typedef std::function<void(std::shared_ptr<SerialData> table)> execute_callback_t;
    typedef std::function<void()> build_callback_t;

    class Plan
    {
    protected:
        Metrics metrics;
    public:
        /*
         * The global id of the plan operator
         */
        const int id;
        /* return all the input plans of this plan
         * Plan:
         *      projection
         *          filter
         *             ....
         * projection->input_plans() = {filter}
         */
        virtual std::vector<std::shared_ptr<Plan>> input_plans() const = 0;
        /*
         * compile the plan to acero plan
         * pipeline breaker:
         *      if data structure, query data structure and return a TableSource
         *      if networking roundtrip, wait server return a TableSource
         */
        virtual void execute(const BindingMap& binding, execute_callback_t cb) = 0;
        /*
         * construct a new binding_map that only contains the bindings used in the current plan
         */
        virtual void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) = 0;

        // convert the plan to SQL query. Used for querying cloud DB
        virtual std::string to_sql(const BindingMap& binding) const = 0;
        virtual std::string to_string() const = 0;
        virtual void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) = 0;

        Plan(int id);
        // Precompute all SCache
        void initialize(build_callback_t cb);
        bool at_server() const;
        /*
         * execute a subplan rooted at [id] using the binding
         */
        void execute_subplan(const BindingMap& binding, int id, execute_callback_t cb);
    protected:
        void _initialize(build_callback_t cb, std::vector<std::shared_ptr<Plan>> inputs);
    };

    class AceroPlan : public Plan
    {
    protected:
        std::shared_ptr<Plan> input;
    public:
        virtual ac::Declaration build_plan(const BindingMap& binding, ac::Declaration input_plan) = 0;
        void compile(const BindingMap& binding, compile_callback_t cb);
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        AceroPlan(int id, std::shared_ptr<Plan> input) : Plan(id), input(input) {};
    };

    class Projection : public AceroPlan
    {
    private:
        // projection expressions
        std::vector<std::shared_ptr<Expression>> proj_exprs;
        // projection names for arrow tables
        std::vector<std::string> proj_names;

    public:
        Projection(int id,
                   std::shared_ptr<Plan> input,
                   std::vector<std::shared_ptr<Expression>> proj_exprs,
                   std::vector<std::string> proj_names);
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        ac::Declaration build_plan(const BindingMap& binding, ac::Declaration input_plan) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    class Filter : public AceroPlan
    {
        // Boolean
        std::shared_ptr<Expression> cond_expr;
    public:
        Filter(int id, std::shared_ptr<Plan> input,
               std::shared_ptr<Expression> cond_expr);
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        ac::Declaration build_plan(const BindingMap& binding, ac::Declaration input_plan) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    class Aggregate : public AceroPlan
    {
        std::vector<std::shared_ptr<Expression>> groupby_exprs;
        std::vector<std::string> groupby_names;
        std::vector<std::shared_ptr<Expression>> aggregate_exprs;
        std::vector<std::string> aggregate_names;
    public:
        Aggregate(int id,
                  std::shared_ptr<Plan> input,
                  std::vector<std::shared_ptr<Expression>> groupby_exprs,
                  std::vector<std::string> groupby_names,
                  std::vector<std::shared_ptr<Expression>> aggregate_exprs,
                  std::vector<std::string> aggregate_names);
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        ac::Declaration build_plan(const BindingMap& binding, ac::Declaration input_plan) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    class TableSource : public Plan
    {
        std::string table_name;
    public:
        TableSource(int id, std::string name);
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    class Network : public Plan
    {
        std::shared_ptr<Plan> input;
    public:
        Network(int id, std::shared_ptr<Plan> input);
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    class Cloud : public Plan
    {
        std::shared_ptr<Plan> input;
    public:
        Cloud(int id, std::shared_ptr<Plan> input);
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    class SCache : public Plan
    {
        std::shared_ptr<Plan> input;
        std::unordered_map<uint64_t, std::shared_ptr<SerialData>> data;
    public:
        SCache(int id, std::shared_ptr<Plan> input);
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
        void cache_data(build_callback_t cb);
    private:
        void _cache_data(build_callback_t cb, std::shared_ptr<std::vector<BindingMap>> bindings, int i);
    };

    class DCache : public Plan
    {
        std::shared_ptr<Plan> input;
        std::shared_ptr<SerialData> data;
        BindingMap current_binding;
    public:
        DCache(int id, std::shared_ptr<Plan> input);
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    struct HashTableImpl : public SerialData
    {
        typedef std::unordered_map<uint64_t, std::shared_ptr<TableData>> HashTable;

        std::shared_ptr<TableData> empty_table;
        std::shared_ptr<HashTable> table;

        HashTableImpl() : table(nullptr), empty_table(nullptr) {}
        HashTableImpl(std::shared_ptr<HashTable> table, std::shared_ptr<TableData> empty_table) :
            table(std::move(table)), empty_table(std::move(empty_table)) {}

        std::shared_ptr<TableData> query(uint64_t key);

        void serialize(std::shared_ptr<ar::io::BufferOutputStream> out) override;
        void deserialize(std::shared_ptr<ar::io::BufferReader> buffer) override;
        uint64_t size() override;
    };

    class HashTableBuild : public Plan
    {
        std::shared_ptr<Plan> input;
        // key_1 == query_1 && key_2 == query_2 && ...
        std::vector<std::shared_ptr<Expression>> keys;
    public:
        HashTableBuild(int id, std::shared_ptr<Plan> input, std::vector<std::shared_ptr<Expression>> keys)
                : Plan(id), input(std::move(input)), keys(std::move(keys)) {
            metrics.id = id;
            metrics.node = "HashTableBuild";
        }
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    class HashTableQuery: public Plan
    {
        std::shared_ptr<Plan> input;
        // key_1 == query_1 && key_2 == query_2 && ...
        std::vector<std::shared_ptr<Expression>> queries;
    public:
        HashTableQuery(int id, std::shared_ptr<Plan> input,
                       std::vector<std::shared_ptr<Expression>> queries)
                : Plan(id), input(input), queries(queries) {
            metrics.id = id;
            metrics.node = "HashTableQuery";
        };
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    struct RTreeImpl : public SerialData
    {
        typedef RTree<int, double, 1> RTree1D;
        typedef RTree<int, double, 2> RTree2D;
        typedef RTree<int, double, 3> RTree3D;
        typedef std::variant<std::shared_ptr<RTree1D>, std::shared_ptr<RTree2D>, std::shared_ptr<RTree3D>> RTree_T;

        // The input table
        std::shared_ptr<TableData> table;
        // dimension of the RTree
        int dim;
        // The RTree index
        RTree_T rtree;

        RTreeImpl() : table(nullptr), rtree(std::shared_ptr<RTree1D>(nullptr)) {}
        RTreeImpl(std::shared_ptr<TableData> table, int dim, RTree_T rtree) :
                table(std::move(table)), dim(dim), rtree(std::move(rtree)) {}

        std::shared_ptr<TableData> query(std::vector<double> lowers, std::vector<double> uppers);

        void serialize(std::shared_ptr<ar::io::BufferOutputStream> out) override;
        void deserialize(std::shared_ptr<ar::io::BufferReader> buffer) override;
        uint64_t size() override;
    };


    class RTreeBuild : public Plan
    {
        std::shared_ptr<Plan> input;
        // lowers_1 <= key_1 <= uppers_1 && ...
        std::vector<std::shared_ptr<Expression>> keys;
    public:
        RTreeBuild(int id, std::shared_ptr<Plan> input, std::vector<std::shared_ptr<Expression>> keys)
                : Plan(id), input(std::move(input)), keys(std::move(keys)) {
            metrics.id = id;
            metrics.node = "RTreeBuild";
        }
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    class RTreeQuery: public Plan
    {
        std::shared_ptr<Plan> input;
        // lowers_1 <= key_1 <= uppers_1 && ...
        std::vector<std::shared_ptr<Expression>> lowers;
        std::vector<std::shared_ptr<Expression>> uppers;
    public:
        RTreeQuery(int id, std::shared_ptr<Plan> input,
                   std::vector<std::shared_ptr<Expression>> lowers,
                   std::vector<std::shared_ptr<Expression>> uppers)
                : Plan(id), input(input), lowers(lowers), uppers(uppers) {
            metrics.id = id;
            metrics.node = "RTreeQuery";
        };
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    struct PrefixSumImpl : public SerialData
    {
        std::shared_ptr<TableData> target_col_data;
        std::shared_ptr<TableData> sum_col_data;
        std::shared_ptr<std::vector<double>> prefix_sum;
        std::shared_ptr<ar::Schema> output_schema;

        PrefixSumImpl() = default;
        PrefixSumImpl(std::shared_ptr<TableData> sum_col_data,
                      std::shared_ptr<TableData> target_col_data,
                      std::string agg_col_name,
                      std::shared_ptr<std::vector<double>> prefix_sum)
                : sum_col_data(std::move(sum_col_data)),
                  target_col_data(std::move(target_col_data)),
                  prefix_sum(std::move(prefix_sum))
        {
            auto target_field = this->target_col_data->table->schema()->field(0);
            ar::FieldVector fields = {target_field->Copy(), ar::field(agg_col_name, ar::float64())};
            output_schema = std::make_shared<ar::Schema>(fields);
        }

        int64_t get_sum_col_idx(std::shared_ptr<ar::Scalar> value, bool lower_than);
        std::shared_ptr<TableData> query(std::shared_ptr<ar::Scalar> lower, std::shared_ptr<ar::Scalar> upper);
        void serialize(std::shared_ptr<ar::io::BufferOutputStream> out) override;
        void deserialize(std::shared_ptr<ar::io::BufferReader> buffer) override;
        uint64_t size() override;
    };

    class PrefixSumBuild : public Plan
    {
        std::shared_ptr<Plan> input;
        std::shared_ptr<Expression> sum_col;
        std::shared_ptr<Expression> target_col;
        std::shared_ptr<Expression> agg_col;
        std::string sum_col_name;
        std::string target_col_name;
        std::string agg_col_name;
    public:
        PrefixSumBuild(int id,
                       std::shared_ptr<Plan> input,
                       std::shared_ptr<Expression> sum_col, std::string  sum_col_name,
                       std::shared_ptr<Expression> target_col, std::string target_col_name,
                       std::shared_ptr<Expression> agg_col, std::string agg_col_name)
                : Plan(id), input(input), sum_col(sum_col), sum_col_name(sum_col_name),
                  target_col(target_col), target_col_name(target_col_name),
                  agg_col(agg_col), agg_col_name(agg_col_name) {
            metrics.id = id;
            metrics.node = "PrefixSumBuild";
        }
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    class PrefixSumQuery: public Plan
    {
        std::shared_ptr<Plan> input;
        // lower <= sum_col <= upper
        std::shared_ptr<Expression> lower, upper;
    public:
        PrefixSumQuery(int id, std::shared_ptr<Plan> input,
                       std::shared_ptr<Expression> lower,
                       std::shared_ptr<Expression> upper) : Plan(id), input(std::move(std::move(input))), lower(lower), upper(upper) {
            metrics.id = id;
            metrics.node = "PrefixSumQuery";
        }
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    struct PrefixSum2DImpl : public SerialData
    {
        std::shared_ptr<TableData> target_col_data;
        std::shared_ptr<TableData> sum_col_x_data;
        std::shared_ptr<TableData> sum_col_y_data;
        std::shared_ptr<std::vector<double>> prefix_sum;
        std::shared_ptr<ar::Schema> output_schema;

        PrefixSum2DImpl() = default;
        PrefixSum2DImpl(std::shared_ptr<TableData> sum_col_x_data,
                        std::shared_ptr<TableData> sum_col_y_data,
                        std::shared_ptr<TableData> target_col_data,
                        std::string agg_col_name,
                        std::shared_ptr<std::vector<double>> prefix_sum)
                : sum_col_x_data(std::move(sum_col_x_data)),
                  sum_col_y_data(std::move(sum_col_y_data)),
                  target_col_data(std::move(target_col_data)),
                  prefix_sum(std::move(prefix_sum))
        {
            auto target_field = this->target_col_data->table->schema()->field(0);
            output_schema = std::make_shared<ar::Schema>(ar::Schema({target_field, ar::field(agg_col_name, ar::float64())}));
        }

        int64_t get_sum_col_idx(std::shared_ptr<TableData> sum_col_data, std::shared_ptr<ar::Scalar> value, bool lower_than);
        std::shared_ptr<TableData> query(std::shared_ptr<ar::Scalar> lower_x, std::shared_ptr<ar::Scalar> upper_x,
                                         std::shared_ptr<ar::Scalar> lower_y, std::shared_ptr<ar::Scalar> upper_y);
        void serialize(std::shared_ptr<ar::io::BufferOutputStream> out) override;
        void deserialize(std::shared_ptr<ar::io::BufferReader> buffer) override;
        uint64_t size() override;
    };

    class PrefixSum2DBuild : public Plan
    {
        std::shared_ptr<Plan> input;
        std::shared_ptr<Expression> sum_col_x;
        std::shared_ptr<Expression> sum_col_y;
        std::shared_ptr<Expression> target_col;
        std::shared_ptr<Expression> agg_col;
        std::string sum_col_x_name;
        std::string sum_col_y_name;
        std::string target_col_name;
        std::string agg_col_name;
    public:
        PrefixSum2DBuild(int id,
                         std::shared_ptr<Plan> input,
                         std::shared_ptr<Expression> sum_col_x, std::string  sum_col_x_name,
                         std::shared_ptr<Expression> sum_col_y, std::string  sum_col_y_name,
                         std::shared_ptr<Expression> target_col, std::string target_col_name,
                         std::shared_ptr<Expression> agg_col, std::string agg_col_name)
                : Plan(id), input(input),
                  sum_col_x(sum_col_x), sum_col_x_name(sum_col_x_name),
                  sum_col_y(sum_col_y), sum_col_y_name(sum_col_y_name),
                  target_col(target_col), target_col_name(target_col_name),
                  agg_col(agg_col), agg_col_name(agg_col_name) {
            metrics.id = id;
            metrics.node = "PrefixSum2DBuild";
        }
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    class PrefixSum2DQuery: public Plan
    {
        std::shared_ptr<Plan> input;
        // lower <= sum_col <= upper
        std::shared_ptr<Expression> lower_x, upper_x, lower_y, upper_y;
    public:
        PrefixSum2DQuery(int id, std::shared_ptr<Plan> input,
                         std::shared_ptr<Expression> lower_x, std::shared_ptr<Expression> upper_x,
                         std::shared_ptr<Expression> lower_y, std::shared_ptr<Expression> upper_y
        ) : Plan(id), input(std::move(input)),
            lower_x(std::move(lower_x)), upper_x(upper_x),
            lower_y(lower_y), upper_y(upper_y) {
            metrics.id = id;
            metrics.node = "PrefixSum2DQuery";
        }

        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    /*
     * Choice Node
     */

    class ChoicePlan : public Plan
    {
    public:
        std::string choice_id;
        ChoicePlan(int id, const std::string &cid) : Plan(id), choice_id(cid) {}
    };

    class AnyPlan : public ChoicePlan
    {
        std::vector<std::shared_ptr<Plan>> choices;
    public:

        AnyPlan(int id, const std::string &cid, std::vector<std::shared_ptr<Plan>> choices);
        std::vector<std::shared_ptr<Plan>> input_plans() const override;
        void execute(const BindingMap& binding, execute_callback_t cb) override;
        void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
        std::string to_sql(const BindingMap& binding) const override;
        std::string to_string() const override;
        void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    };

    std::shared_ptr<Plan> parse_json_plan(const json& plan);
}
