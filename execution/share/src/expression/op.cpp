#include "expression.h"

#include <memory>
#include <iostream>
#include <arrow/compute/api.h>

namespace pvd
{
    Op::Op(Operator op, std::vector<std::shared_ptr<Expression>> operands) : op(op), operands(operands) {}
    Op::Op(Operator op, std::initializer_list<std::shared_ptr<Expression>> operands) : op(op), operands(operands) {}

    void Op::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        for (auto operand : operands)
        {
            operand->get_all_choice_nodes(choice_nodes);
        }
    }

    std::shared_ptr<Expression> Op::bind(const BindingMap& binding) const
    {
        std::vector<std::shared_ptr<Expression>> new_operands;
        for (auto operand : operands)
        {
            new_operands.push_back(operand->bind(binding));
        }
        return std::make_shared<Op>(op, new_operands);
    }

    void Op::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        for (auto operand : operands)
        {
            operand->pick_useful_binding(binding, useful_binding);
        }
    }

    static float get_float(std::shared_ptr<Expression> val)
    {
        auto int_val = std::dynamic_pointer_cast<IntConst>(val);
        auto float_val = std::dynamic_pointer_cast<FloatConst>(val);
        if (int_val) return int_val->value;
        else if (float_val) return float_val->value;
        else throw std::runtime_error("Invalid operand type");
    }

    /* both val1 and val2 may be int or float
     * if both are int, return int
     * if at least one is float, return float
     */
    static std::shared_ptr<Literal> evaluate_two_numerical(std::shared_ptr<Expression> val1,
                                                           std::shared_ptr<Expression> val2,
                                                           std::function<int(int, int)> int_op,
                                                           std::function<float(float, float)> float_op)
    {
        auto int_val1 = std::dynamic_pointer_cast<IntConst>(val1);
        auto int_val2 = std::dynamic_pointer_cast<IntConst>(val2);
        if (int_val1 && int_val2)
        {
            // both are int, return int
            return std::make_shared<IntConst>(int_op(int_val1->value, int_val2->value));
        }
        else {
            float float_val1 = get_float(val1);
            float float_val2 = get_float(val2);
            return std::make_shared<FloatConst>(float_op(float_val1, float_val2));
        }
    }

    // const can be int, float, string
    static std::shared_ptr<Literal> compare_two_const(std::shared_ptr<Expression> val1,
                                                      std::shared_ptr<Expression> val2,
                                                      std::function<bool(int, int)> int_op,
                                                      std::function<bool(float, float)> float_op,
                                                      std::function<bool(std::string, std::string)> string_op)
    {
        auto int_val1 = std::dynamic_pointer_cast<IntConst>(val1);
        auto int_val2 = std::dynamic_pointer_cast<IntConst>(val2);
        auto string_val1 = std::dynamic_pointer_cast<StringConst>(val1);
        auto string_val2 = std::dynamic_pointer_cast<StringConst>(val2);

        if (int_val1 && int_val2)
        {
            // both are int
            return std::make_shared<BoolConst>(int_op(int_val1->value, int_val2->value));
        }
        else if (string_val1 && string_val2)
        {
            // both are string
            return std::make_shared<BoolConst>(string_op(string_val1->value, string_val2->value));
        }
        else {
            // at least one is float, else int
            float float_val1 = get_float(val1);
            float float_val2 = get_float(val2);
            return std::make_shared<BoolConst>(float_op(float_val1, float_val2));
        }
    }

    std::shared_ptr<Expression> Op::evaluate(const BindingMap& binding) const
    {
        std::vector<std::shared_ptr<Expression>> args;
        for (auto operand : operands)
        {
            args.push_back(operand->evaluate(binding));
        }
        switch (op)
        {
            case Op::Operator::Neg: {
                auto bool_val = std::dynamic_pointer_cast<BoolConst>(args[0]);
                return std::make_shared<BoolConst>(!bool_val->value);
            }
            case Op::Operator::Add: {
                return evaluate_two_numerical(args[0], args[1],
                                              [](int a, int b) { return a + b; },
                                              [](float a, float b) { return a + b; });
            }
            case Op::Operator::Sub: {
                return evaluate_two_numerical(args[0], args[1],
                                              [](int a, int b) { return a - b; },
                                              [](float a, float b) { return a - b; });
            }
            case Op::Operator::Mul: {
                return evaluate_two_numerical(args[0], args[1],
                                              [](int a, int b) { return a * b; },
                                              [](float a, float b) { return a * b; });
            }
            case Op::Operator::Div: {
                // TODO: check divide by zero
                // TODO: should we always return float?
                return evaluate_two_numerical(args[0], args[1],
                                              [](int a, int b) { return a / b; },
                                              [](float a, float b) { return a / b; });
            }
            case Op::Operator::Pow: {
                return evaluate_two_numerical(args[0], args[1],
                                              [](int a, int b) { return std::pow(a, b); },
                                              [](float a, float b) { return std::pow(a, b); });
            }
            case Op::Operator::Eq: {
                return compare_two_const(args[0], args[1],
                                         [](int a, int b) { return a == b; },
                                         [](float a, float b) { return a == b; },
                                         [](std::string a, std::string b) { return a == b; });
            }
            case Op::Operator::Ne: {
                return compare_two_const(args[0], args[1],
                                         [](int a, int b) { return a != b; },
                                         [](float a, float b) { return a != b; },
                                         [](std::string a, std::string b) { return a != b; });
            }
            case Op::Operator::Ge: {
                return compare_two_const(args[0], args[1],
                                         [](int a, int b) { return a >= b; },
                                         [](float a, float b) { return a >= b; },
                                         [](std::string a, std::string b) { return a >= b; });
            }
            case Op::Operator::Gt: {
                return compare_two_const(args[0], args[1],
                                         [](int a, int b) { return a > b; },
                                         [](float a, float b) { return a > b; },
                                         [](std::string a, std::string b) { return a > b; });
            }
            case Op::Operator::Le: {
                return compare_two_const(args[0], args[1],
                                         [](int a, int b) { return a <= b; },
                                         [](float a, float b) { return a <= b; },
                                         [](std::string a, std::string b) { return a <= b; });
            }
            case Op::Operator::Lt: {
                return compare_two_const(args[0], args[1],
                                         [](int a, int b) { return a < b; },
                                         [](float a, float b) { return a < b; },
                                         [](std::string a, std::string b) { return a < b; });
            }
            case Op::Operator::In: {
                auto string_val = std::dynamic_pointer_cast<StringConst>(args[0]);
                auto list_val = std::dynamic_pointer_cast<List>(args[1]);
                if (string_val) {
                    // all elements in the list must be string
                    std::string val = string_val->value;
                    for (auto elem : list_val->elements) {
                        auto string_elem = std::dynamic_pointer_cast<StringConst>(elem);
                        if (!string_elem) throw std::runtime_error("Invalid operand type");
                        auto elem_val = string_elem->value;
                        if (val == elem_val) return std::make_shared<BoolConst>(true);
                    }
                    return std::make_shared<BoolConst>(false);
                }
                else {
                    // numerical values
                    auto val = get_float(args[0]);
                    for (auto elem : list_val->elements) {
                        auto elem_val = get_float(elem);
                        if (val == elem_val) return std::make_shared<BoolConst>(true);
                    }
                    return std::make_shared<BoolConst>(false);
                }
            }
            case Op::Operator::Between: {
                auto val = get_float(args[0]);
                auto lower = get_float(args[1]);
                auto upper = get_float(args[2]);
                return std::make_shared<BoolConst>(lower <= val && val <= upper);
            }
            case Op::Operator::Not: {
                auto bool_val = std::dynamic_pointer_cast<BoolConst>(args[0]);
                if (!bool_val) throw std::runtime_error("Invalid operand type");
                return std::make_shared<BoolConst>(!bool_val->value);
            }
            case Op::Operator::And: {
                auto bool_val1 = std::dynamic_pointer_cast<BoolConst>(args[0]);
                auto bool_val2 = std::dynamic_pointer_cast<BoolConst>(args[1]);
                if (!bool_val1 || !bool_val2) throw std::runtime_error("Invalid operand type");
                return std::make_shared<BoolConst>(bool_val1->value && bool_val2->value);
            }
            case Op::Operator::Or: {
                auto bool_val1 = std::dynamic_pointer_cast<BoolConst>(args[0]);
                auto bool_val2 = std::dynamic_pointer_cast<BoolConst>(args[1]);
                if (!bool_val1 || !bool_val2) throw std::runtime_error("Invalid operand type");
                return std::make_shared<BoolConst>(bool_val1->value || bool_val2->value);
            }
            default:
                throw std::runtime_error("Invalid operator");
        }
    }

    cp::Expression Op::to_arrow_expr() const
    {
        switch (op)
        {
            case Operator::Neg:
                return cp::call("negate", {operands[0]->to_arrow_expr()});
            case Operator::Add:
                return cp::call("add", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Sub:
                return cp::call("subtract", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Mul:
                return cp::call("multiply", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Div:
                return cp::call("divide", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Pow:
                return cp::call("power", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Eq:
                return cp::call("equal", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Ne:
                return cp::call("not_equal", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Ge:
                return cp::call("greater_equal", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Gt:
                return cp::call("greater", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Le:
                return cp::call("less_equal", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Lt:
                return cp::call("less", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::In:
                return cp::call("is_in", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Between: // lower <= value <= upper
                //std::cout << "value: " << operands[0]->to_arrow_expr().ToString() << std::endl;
                //std::cout << "lower: " << operands[1]->to_arrow_expr().ToString() << std::endl;
                //std::cout << "upper: " << operands[2]->to_arrow_expr().ToString() << std::endl;
                return cp::call("and",
                                {cp::call("greater_equal", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()}),
                                 cp::call("less_equal", {operands[0]->to_arrow_expr(), operands[2]->to_arrow_expr()})});
            case Operator::Not:
                return cp::call("invert", {operands[0]->to_arrow_expr()});
            case Operator::And:
                return cp::call("and", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            case Operator::Or:
                return cp::call("or", {operands[0]->to_arrow_expr(), operands[1]->to_arrow_expr()});
            default:
                throw std::runtime_error("Invalid operator");
        }
    }

    std::string Op::to_string() const
    {
        switch (op)
        {
            case Operator::Neg:
                return "-" + operands[0]->to_string();
            case Operator::Add:
                return operands[0]->to_string() + " + " + operands[1]->to_string();
            case Operator::Sub:
                return operands[0]->to_string() + " - " + operands[1]->to_string();
            case Operator::Mul:
                return operands[0]->to_string() + " * " + operands[1]->to_string();
            case Operator::Div:
                return operands[0]->to_string() + " / " + operands[1]->to_string();
            case Operator::Pow:
                return operands[0]->to_string() + " ^ " + operands[1]->to_string();
            case Operator::Eq:
                return operands[0]->to_string() + " = " + operands[1]->to_string();
            case Operator::Ne:
                return operands[0]->to_string() + " != " + operands[1]->to_string();
            case Operator::Ge:
                return operands[0]->to_string() + " >= " + operands[1]->to_string();
            case Operator::Gt:
                return operands[0]->to_string() + " > " + operands[1]->to_string();
            case Operator::Le:
                return operands[0]->to_string() + " <= " + operands[1]->to_string();
            case Operator::Lt:
                return operands[0]->to_string() + " < " + operands[1]->to_string();
            case Operator::In:
                return operands[0]->to_string() + " in " + operands[1]->to_string();
            case Operator::Between:
                return operands[0]->to_string() + " between " + operands[1]->to_string() + " and " +
                       operands[2]->to_string();
            case Operator::Not:
                return "not " + operands[0]->to_string();
            case Operator::And:
                return operands[0]->to_string() + " and " + operands[1]->to_string();
            case Operator::Or:
                return operands[0]->to_string() + " or " + operands[1]->to_string();
            default:
                throw std::runtime_error("Invalid operator");
        }
    }
}
