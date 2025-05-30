#include "plan.h"
#include "expression.h"
#include <iostream>

namespace pvd
{
    std::unordered_map<int, std::shared_ptr<Plan>> id_to_plan;

    std::shared_ptr<Plan> parse_json_plan(const json& plan)
    {
        int id = plan["id"];
        if (id_to_plan.find(id) != id_to_plan.end()) {
            return id_to_plan[id];
        }
        std::shared_ptr<Plan> p;
        if (plan["type"] == "Projection") {
            auto input = parse_json_plan(plan["input"]);
            auto names = std::vector<std::string>();
            auto exprs = std::vector<std::shared_ptr<Expression>>();
            for (auto proj : plan["projs"]) {
                exprs.push_back(parse_json_expression(proj["expr"]));
                names.push_back(proj["name"]);
            }
            p = std::make_shared<Projection>(id, input, exprs, names);
        }
        else if (plan["type"] == "Filter") {
            auto input = parse_json_plan(plan["input"]);
            auto expr = parse_json_expression(plan["cond"]);
            p = std::make_shared<Filter>(id, input, expr);
        }
        else if (plan["type"] == "Aggregate") {
            auto input = parse_json_plan(plan["input"]);
            auto groupby_exprs = std::vector<std::shared_ptr<Expression>>();
            auto groupby_names = std::vector<std::string>();
            auto aggregate_exprs = std::vector<std::shared_ptr<Expression>>();
            auto aggregate_names = std::vector<std::string>();
            for (auto gb : plan["groupbys"]) {
                groupby_exprs.push_back(parse_json_expression(gb["expr"]));
                groupby_names.push_back(gb["name"]);
            }
            for (auto agg : plan["aggs"]) {
                aggregate_exprs.push_back(parse_json_expression(agg["expr"]));
                aggregate_names.push_back(agg["name"]);
            }
            p = std::make_shared<Aggregate>(id, input, groupby_exprs, groupby_names, aggregate_exprs, aggregate_names);
        }
        else if (plan["type"] == "TableSource") {
            std::string name = plan["name"];
            p = std::make_shared<TableSource>(id, name);
        }
        else if (plan["type"] == "Network") {
            auto input = parse_json_plan(plan["input"]);
            p = std::make_shared<Network>(id, input);
        }
        else if (plan["type"] == "Cloud") {
            auto input = parse_json_plan(plan["input"]);
            p = std::make_shared<Cloud>(id, input);
        }
        else if (plan["type"] == "SCache") {
            auto input = parse_json_plan(plan["input"]);
            p = std::make_shared<SCache>(id, input);
        }
        else if (plan["type"] == "DCache") {
            auto input = parse_json_plan(plan["input"]);
            p = std::make_shared<DCache>(id, input);
        }
        else if (plan["type"] == "HashTableBuild") {
            auto input = parse_json_plan(plan["input"]);
            auto keys = std::vector<std::shared_ptr<Expression>>();
            for (auto key : plan["keys"]) {
                keys.push_back(parse_json_expression(key));
            }
            p = std::make_shared<HashTableBuild>(id, input, keys);
        }
        else if (plan["type"] == "HashTableQuery") {
            auto input = parse_json_plan(plan["input"]);
            auto keys = std::vector<std::shared_ptr<Expression>>();
            auto queries = std::vector<std::shared_ptr<Expression>>();
            for (auto query : plan["queries"]) {
                queries.push_back(parse_json_expression(query));
            }
            p = std::make_shared<HashTableQuery>(id, input, queries);
        }
        else if (plan["type"] == "RTreeBuild") {
            auto input = parse_json_plan(plan["input"]);
            auto keys = std::vector<std::shared_ptr<Expression>>();
            for (auto key : plan["keys"]) {
                keys.push_back(parse_json_expression(key));
            }
            p = std::make_shared<RTreeBuild>(id, input, keys);
        }
        else if (plan["type"] == "RTreeQuery") {
            auto input = parse_json_plan(plan["input"]);
            auto keys = std::vector<std::shared_ptr<Expression>>();
            auto lowers = std::vector<std::shared_ptr<Expression>>();
            auto uppers = std::vector<std::shared_ptr<Expression>>();
            for (auto query : plan["lowers"]) {
                lowers.push_back(parse_json_expression(query));
            }
            for (auto query : plan["uppers"]) {
                uppers.push_back(parse_json_expression(query));
            }
            p = std::make_shared<RTreeQuery>(id, input, lowers, uppers);
        }
        else if (plan["type"] == "PrefixSumBuild") {
            auto input = parse_json_plan(plan["input"]);
            std::string sum_col_name = plan["sum_col"]["name"];
            std::string target_col_name = plan["target_col"]["name"];
            std::string agg_col_name = plan["agg_col"]["name"];
            auto sum_col = parse_json_expression(plan["sum_col"]["expr"]);
            auto target_col = parse_json_expression(plan["target_col"]["expr"]);
            auto agg_col = parse_json_expression(plan["agg_col"]["expr"]);
            p = std::make_shared<PrefixSumBuild>(id, input,
                                                 sum_col, sum_col_name,
                                                 target_col, target_col_name,
                                                 agg_col, agg_col_name);
        }
        else if (plan["type"] == "PrefixSumQuery") {
            auto input = parse_json_plan(plan["input"]);
            auto lower = parse_json_expression(plan["lower"]);
            auto upper = parse_json_expression(plan["upper"]);
            p = std::make_shared<PrefixSumQuery>(id, input, lower, upper);
        }
        else if (plan["type"] == "PrefixSum2DBuild") {
            auto input = parse_json_plan(plan["input"]);
            std::string sum_col_x_name = plan["sum_col_x"]["name"];
            std::string sum_col_y_name = plan["sum_col_y"]["name"];
            std::string target_col_name = plan["target_col"]["name"];
            std::string agg_col_name = plan["agg_col"]["name"];
            auto sum_col_x = parse_json_expression(plan["sum_col_x"]["expr"]);
            auto sum_col_y = parse_json_expression(plan["sum_col_y"]["expr"]);
            auto target_col = parse_json_expression(plan["target_col"]["expr"]);
            auto agg_col = parse_json_expression(plan["agg_col"]["expr"]);
            p = std::make_shared<PrefixSum2DBuild>(id, input,
                                                   sum_col_x, sum_col_x_name,
                                                   sum_col_y, sum_col_y_name,
                                                   target_col, target_col_name,
                                                   agg_col, agg_col_name);
        }
        else if (plan["type"] == "PrefixSum2DQuery") {
            auto input = parse_json_plan(plan["input"]);
            auto lower_x = parse_json_expression(plan["lower_x"]);
            auto upper_x = parse_json_expression(plan["upper_x"]);
            auto lower_y = parse_json_expression(plan["lower_y"]);
            auto upper_y = parse_json_expression(plan["upper_y"]);
            p = std::make_shared<PrefixSum2DQuery>(id, input, lower_x, upper_x, lower_y, upper_y);
        }
        else if (plan["type"] == "AnyPlan") {
            std::string cid = plan["choice_id"];
            std::vector<std::shared_ptr<Plan>> choices;
            for (auto choice : plan["choices"]) {
                choices.push_back(parse_json_plan(choice));
            }
            p = std::make_shared<AnyPlan>(id, cid, choices);
        }
        else {
            throw std::runtime_error("Unknown plan type");
        }
        id_to_plan[id] = p;
        return p;
    }
}
