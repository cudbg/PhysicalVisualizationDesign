#pragma once

#include <string>
#include <vector>
#include <variant>
#include <memory>

#include <arrow/compute/expression.h>

#include "binding.h"
#include "json.h"

namespace ar = arrow;
namespace cp = arrow::compute;
using json = nlohmann::json;

namespace pvd {

class ChoiceExpr;

class Expression
{
public:
    virtual std::shared_ptr<Expression> bind(const BindingMap& binding) const = 0;
    virtual std::shared_ptr<Expression> evaluate(const BindingMap& binding) const = 0;
    virtual cp::Expression to_arrow_expr() const = 0;
    virtual void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) = 0;
    virtual void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) = 0;
    virtual std::string to_string() const = 0;

    /*
     * 1. bind the expression with the binding
     * 2. convert the bound expression to arrow expression
     */
    cp::Expression to_arrow_expr(const BindingMap& binding) const;
};

/*
 * Literals
 */

class Literal : public Expression {
public:
    virtual std::shared_ptr<arrow::Scalar> to_arrow_scalar() const = 0;
};

class IntConst : public Literal
{
public:
    int64_t value;

    IntConst(int64_t value);
    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    std::shared_ptr<arrow::Scalar> to_arrow_scalar() const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

class BoolConst : public Literal
{
public:
    bool value;

    BoolConst(bool value);
    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    std::shared_ptr<arrow::Scalar> to_arrow_scalar() const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

class FloatConst : public Literal
{
public:
    double value;

    FloatConst(double value);
    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    std::shared_ptr<arrow::Scalar> to_arrow_scalar() const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

class StringConst : public Literal
{
public:
    std::string value;

    StringConst(const std::string& value);
    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    std::shared_ptr<arrow::Scalar> to_arrow_scalar() const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

class ColumnRef : public Literal
{
public:
    std::string col;
    std::string table;

    ColumnRef(const std::string& col, const std::string& table);
    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    std::shared_ptr<arrow::Scalar> to_arrow_scalar() const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

/*
 * Expression Operations
 */

class Op : public Expression
{
public:
    enum Operator { Neg, Add, Sub, Mul, Div, Pow, Eq, Ne, Ge, Gt, Le, Lt, In, Between, Not, And, Or };

    Operator op;
    std::vector<std::shared_ptr<Expression>> operands;

    Op(Operator op, std::vector<std::shared_ptr<Expression>> operands);
    Op(Operator op, std::initializer_list<std::shared_ptr<Expression>> operands);
    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

class Func : public Expression
{
public:
    std::string fname;
    std::vector<std::shared_ptr<Expression>> arguments;

    Func(const std::string& fname, std::vector<std::shared_ptr<Expression>> operands);
    Func(const std::string& fname, std::initializer_list<std::shared_ptr<Expression>> operands);
    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

class List : public Expression
{
public:
    std::string begin;
    std::string end;
    std::string delim;
    std::vector<std::shared_ptr<Expression>> elements;

    /* default list format [a,b,c] begin=[ end=] delim=, */
    List(std::vector<std::shared_ptr<Expression>> elems);
    List(std::initializer_list<std::shared_ptr<Expression>> elems);
    /* custom list format */
    List(const std::string& begin,
         const std::string& end,
         const std::string& delim,
         std::vector<std::shared_ptr<Expression>> elems);
    List(const std::string& begin,
         const std::string& end,
         const std::string& delim,
         std::initializer_list<std::shared_ptr<Expression>> elems);

    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

/*
 * Expression Choice Nodes
 */

class ChoiceExpr : public Expression
{
public:
    virtual std::string choice_id() const = 0;
    virtual std::vector<Binding> all_choices() = 0;
};

class AnyExpr : public ChoiceExpr
{
public:
    std::string _id;
    std::vector<std::shared_ptr<Expression>> choices;

    AnyExpr(const std::string& id, std::vector<std::shared_ptr<Expression>> choices);
    AnyExpr(const std::string& id, std::initializer_list<std::shared_ptr<Expression>> choices);
    std::string choice_id() const override;
    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::vector<Binding> all_choices() override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

class ValExpr : public ChoiceExpr
{
public:
    std::string _id;
    std::shared_ptr<Expression> domain;

    ValExpr(std::string  id, std::shared_ptr<Expression> domain);
    std::string choice_id() const override;
    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::vector<Binding> all_choices() override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

class MultiExpr : public ChoiceExpr
{
public:
    std::string _id;
    std::string begin;
    std::string end;
    std::string delim;
    std::shared_ptr<Expression> child;

    MultiExpr(const std::string& id,
              const std::string& begin,
              const std::string& end,
              const std::string& delim,
              std::shared_ptr<Expression> child);
    std::string choice_id() const override;
    void get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes) override;
    std::vector<Binding> all_choices() override;
    std::shared_ptr<Expression> bind(const BindingMap& binding) const override;
    std::shared_ptr<Expression> evaluate(const BindingMap& binding) const override;
    void pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding) override;
    cp::Expression to_arrow_expr() const override;
    std::string to_string() const override;
};

std::shared_ptr<Expression> parse_json_expression(const json& expr);

}
