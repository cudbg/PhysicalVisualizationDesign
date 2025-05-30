#include "expression.h"
#include "cloud_api.h"

#include <memory>
#include <arrow/compute/api.h>
#include <iostream>
#include <utility>
#include <condition_variable>

namespace pvd
{
    ValExpr::ValExpr(std::string id, std::shared_ptr<Expression> domain) : _id(std::move(id)), domain(domain) {}

    std::string ValExpr::choice_id() const { return _id; }

    void ValExpr::get_all_choice_nodes(std::unordered_map<std::string, std::shared_ptr<ChoiceExpr>> &choice_nodes)
    {
        choice_nodes[_id] = std::make_shared<ValExpr>(_id, domain);
    }

    std::vector<Binding> ValExpr::all_choices()
    {
        std::vector<Binding> result;

        if (auto func = std::dynamic_pointer_cast<Func>(domain)) {
            if (func->fname == "domain") {
                if (auto col = std::dynamic_pointer_cast<ColumnRef>(func->arguments[0])) {
                    cloud->query(
                        "SELECT DISTINCT " + col->col + " FROM " + col->table,
                        [&result](std::shared_ptr<ar::Table> table) {
                            for (int i = 0; i < table->num_rows(); i++) {
                                auto val = table->column(0)->GetScalar(i).ValueOrDie();
                                switch (val->type->id()) {
                                    case ar::Type::INT64: {
                                        int64_t int_val = dynamic_pointer_cast<ar::Int64Scalar>(val)->value;
                                        result.push_back(Binding(Binding::Kind::Int, int_val));
                                        break;
                                    }
                                    case ar::Type::DOUBLE: {
                                        float float_val = dynamic_pointer_cast<ar::FloatScalar>(val)->value;
                                        result.push_back(Binding(float_val));
                                        break;
                                    }
                                    case ar::Type::STRING: {
                                        std::string str_val = dynamic_pointer_cast<ar::StringScalar>(val)->ToString();
                                        result.push_back(Binding(str_val));
                                        break;
                                    }
                                    default:
                                        throw std::runtime_error("unsupported scalar type");
                                }
                            }
                        });
                    return result;
                }
            }
        }
        throw std::runtime_error("domain of is not a domain function");
    }

    std::shared_ptr<Expression> ValExpr::bind(const BindingMap& binding) const
    {
        auto bind = binding.at(choice_id());
        if (bind.is_int()) {
            return std::make_shared<IntConst>(bind.get_int());
        }
        else if (bind.is_float()) {
            return std::make_shared<FloatConst>(bind.get_float());
        }
        else if (bind.is_bool()) {
            return std::make_shared<BoolConst>(bind.get_bool());
        }
        else if (bind.is_string()) {
            return std::make_shared<StringConst>(bind.get_string());
        }
        else {
            throw std::runtime_error("invalid binding type for ValExpr");
        }
    }

    std::shared_ptr<Expression> ValExpr::evaluate(const BindingMap& binding) const
    {
        auto bind = binding.at(choice_id());
        if (bind.is_int()) {
            return std::make_shared<IntConst>(bind.get_int());
        }
        else if (bind.is_float()) {
            return std::make_shared<FloatConst>(bind.get_float());
        }
        else if (bind.is_bool()) {
            return std::make_shared<BoolConst>(bind.get_bool());
        }
        else if (bind.is_string()) {
            return std::make_shared<StringConst>(bind.get_string());
        }
        else {
            throw std::runtime_error("invalid binding type for ValExpr");
        }
    }


    void ValExpr::pick_useful_binding(const BindingMap& binding, BindingMap& useful_binding)
    {
        useful_binding.emplace(choice_id(), binding.at(choice_id()));
    }

    cp::Expression ValExpr::to_arrow_expr() const
    {
        throw std::runtime_error("try to convert an expression with choice nodes to arrow expression");
    }

    std::string ValExpr::to_string() const
    {
        return "VAL[" + _id + "]{ " + domain->to_string() + " }";
    }
}
