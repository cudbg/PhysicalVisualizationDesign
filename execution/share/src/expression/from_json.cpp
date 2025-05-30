#include "expression.h"

#include <iostream>
#include <memory>
#include <arrow/compute/api.h>

namespace pvd
{

    Op::Operator str_to_op(const std::string& op, int num_operands)
    {
        if (op == "-" && num_operands == 1) {
            return Op::Operator::Neg;
        }
        else if (op == "+" && num_operands == 2) {
            return Op::Operator::Add;
        }
        else if (op == "-" && num_operands == 2) {
            return Op::Operator::Sub;
        }
        else if (op == "*" && num_operands == 2) {
            return Op::Operator::Mul;
        }
        else if (op == "/" && num_operands == 2) {
            return Op::Operator::Div;
        }
        else if (op == "^" && num_operands == 2) {
            return Op::Operator::Pow;
        }
        else if (op == "=" && num_operands == 2) {
            return Op::Operator::Eq;
        }
        else if (op == "!=" && num_operands == 2) {
            return Op::Operator::Ne;
        }
        else if (op == ">=" && num_operands == 2) {
            return Op::Operator::Ge;
        }
        else if (op == ">" && num_operands == 2) {
            return Op::Operator::Gt;
        }
        else if (op == "<=" && num_operands == 2) {
            return Op::Operator::Le;
        }
        else if (op == "<" && num_operands == 2) {
            return Op::Operator::Lt;
        }
        else if (op == "in" && num_operands == 2) {
            return Op::Operator::In;
        }
        else if (op == "between" && num_operands == 3) {
            return Op::Operator::Between;
        }
        else if (op == "not" && num_operands == 1) {
            return Op::Operator::Not;
        }
        else if (op == "and" && num_operands == 2) {
            return Op::Operator::And;
        }
        else if (op == "or" && num_operands == 2) {
            return Op::Operator::Or;
        }
        else {
            throw std::runtime_error("Invalid operator");
        }
    }

    std::shared_ptr<Expression> parse_json_expression(const json& expr)
    {
        std::shared_ptr<Expression> result;
        if (expr["type"] == "IntConst") {
            int val = expr["value"].template get<int>();
            result = std::make_shared<IntConst>(val);
        }
        else if (expr["type"] == "BoolConst") {
            bool val = expr["value"].template get<bool>();
            result = std::make_shared<BoolConst>(val);
        }
        else if (expr["type"] == "FloatConst") {
            float val = expr["value"].template get<float>();
            result = std::make_shared<FloatConst>(val);
        }
        else if (expr["type"] == "StringConst") {
            std::string val = expr["value"].template get<std::string>();
            result = std::make_shared<StringConst>(val);
        }
        else if (expr["type"] == "ColumnRef") {
            std::string col = expr["column"].template get<std::string>();
            std::string table = expr["table"].template get<std::string>();
            result = std::make_shared<ColumnRef>(col, table);
        }
        else if (expr["type"] == "Op") {
            std::vector<std::shared_ptr<Expression>> operands;
            for (auto operand : expr["operands"]) {
                operands.push_back(parse_json_expression(operand));
            }
            auto op = str_to_op(expr["op"].template get<std::string>(), operands.size());
            result = std::make_shared<Op>(op, operands);
        }
        else if (expr["type"] == "Func") {
            auto fname = expr["fname"].template get<std::string>();
            std::vector<std::shared_ptr<Expression>> args;
            for (auto arg : expr["args"]) {
                args.push_back(parse_json_expression(arg));
            }
            result = std::make_shared<Func>(fname, args);
        }
        else if (expr["type"] == "List") {
            std::vector<std::shared_ptr<Expression>> elements;
            for (auto element : expr["elements"]) {
                elements.push_back(parse_json_expression(element));
            }
            result = std::make_shared<List>(expr["begin"].template get<std::string>(),
                                          expr["end"].template get<std::string>(),
                                          expr["delim"].template get<std::string>(),
                                          elements);
        }
        else if (expr["type"] == "AnyExpr") {
            auto id = expr["id"].template get<std::string>();
            std::vector<std::shared_ptr<Expression>> choices;
            for (auto choice : expr["choices"]) {
                choices.push_back(parse_json_expression(choice));
            }
            result = std::make_shared<AnyExpr>(id, choices);
        }
        else if (expr["type"] == "MultiExpr") {
            auto id = expr["id"].template get<std::string>();
            auto begin = expr["begin"].template get<std::string>();
            auto end = expr["end"].template get<std::string>();
            auto delim = expr["delim"].template get<std::string>();
            auto child = parse_json_expression(expr["child"]);
            result = std::make_shared<MultiExpr>(id, begin, end, delim, child);
        }
        else if (expr["type"] == "ValExpr") {
            auto id = expr["id"].template get<std::string>();
            auto domain = parse_json_expression(expr["domain"]);
            result = std::make_shared<ValExpr>(id, domain);
        }
        else {
            throw std::runtime_error("Invalid expression type");
        }
        return result;
    }
}
