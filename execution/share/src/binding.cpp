#include <stdexcept>

#include "binding.h"
#include "arrow_utils.h"

namespace pvd
{

    // identify binding type
    bool Binding::is_index() const
    {
        return kind == Kind::Index;
    }

    bool Binding::is_int() const
    {
        return kind == Kind::Int;
    }

    bool Binding::is_float() const
    {
        return kind == Kind::Float;
    }

    bool Binding::is_bool() const
    {
        return kind == Kind::Bool;
    }

    bool Binding::is_string() const
    {
        return kind == Kind::String;
    }

    bool Binding::is_sub_bindings() const
    {
        return kind == Kind::Multi;
    }

    // read binding value
    int Binding::get_index() const
    {
        if (is_index()) {
            return std::get<int>(_value);
        }
        else {
            throw std::runtime_error("get_index with non-index _value");
        }
    }

    int Binding::get_int() const
    {
        if (is_int()) {
            return std::get<int>(_value);
        }
        else {
            throw std::runtime_error("get_int with non-int _value");
        }
    }

    float Binding::get_float() const
    {
        if (is_float()) {
            return std::get<float>(_value);
        }
        else {
            throw std::runtime_error("get_float with non-float _value");
        }
    }

    bool Binding::get_bool() const
    {
        if (is_bool()) {
            return std::get<bool>(_value);
        }
        else {
            throw std::runtime_error("get_bool with non-bool _value");
        }
    }

    const std::string& Binding::get_string() const
    {
        if (is_string()) {
            return std::get<std::string>(_value);
        }
        else {
            throw std::runtime_error("get_string with non-string _value");
        }
    }

    int Binding::get_sub_binding_num() const
    {
        if (is_sub_bindings()) {
            auto& bindings = std::get<std::vector<BindingMap>>(_value);
            return (int)bindings.size();
        }
        else {
            throw std::runtime_error("get_sub_binding_num with non-sub-binding _value");
        }
    }

    const BindingMap& Binding::get_sub_binding(int i) const
    {
        if (is_sub_bindings()) {
            auto& bindings = std::get<std::vector<BindingMap>>(_value);
            return bindings.at(i);
        }
        else {
            throw std::runtime_error("get_sub_binding with non-sub-binding _value");
        }
    }

    // serialize/deserialize
    void Binding::to_json(json& binding)
    {
        if (is_index()) {
            binding["type"] = "Index";
            binding["value"] = get_index();
        }
        else if (is_int()) {
            binding["type"] = "Int";
            binding["value"] = get_int();
        }
        else if (is_float()) {
            binding["type"] = "Float";
            binding["value"] = get_float();
        }
        else if (is_bool()) {
            binding["type"] = "Bool";
            binding["value"] = get_bool();
        }
        else if (is_string()) {
            binding["type"] = "String";
            binding["value"] = get_string();
        }
        else if (is_sub_bindings()) {
            binding["type"] = "Multi";
            for (int i = 0; i < get_sub_binding_num(); i++) {
                binding["value"][i] = json();
                binding_to_json(const_cast<BindingMap&>(get_sub_binding(i)), binding["value"][i]);
            }
        }
        else {
            throw std::runtime_error("Invalid binding type");
        }
    }

    std::string Binding::to_string()
    {
        if (is_index()) {
            return "Index: " + std::to_string(get_index());
        }
        else if (is_int()) {
            return "Int: " + std::to_string(get_int());
        }
        else if (is_float()) {
            return "Float: " + std::to_string(get_float());
        }
        else if (is_bool()) {
            return "Bool: " + std::to_string(get_bool());
        }
        else if (is_string()) {
            return "String: " + get_string();
        }
        else if (is_sub_bindings()) {
            return "TODO: convert sub binding to string";
        }
    }

    void binding_to_json(const BindingMap& binding, json& binding_json)
    {
        for (auto [key, value] : binding) {
            binding_json[key] = json();
            value.to_json(binding_json[key]);
        }
    }

    BindingMap parse_json_binding(const json& binding)
    {
        BindingMap result;
        for (auto& [key, value] : binding.items()) {
            if (value["type"] == "Index") {
                result.emplace(key, Binding(Binding::Kind::Index, value["value"].get<int>()));
            }
            else if (value["type"] == "Int") {
                result.emplace(key, Binding(Binding::Kind::Int, value["value"].get<int>()));
            }
            else if (value["type"] == "Float") {
                result.emplace(key, Binding(value["value"].get<float>()));
            }
            else if (value["type"] == "Bool") {
                result.emplace(key, Binding(value["value"].get<bool>()));
            }
            else if (value["type"] == "String") {
                result.emplace(key, Binding(value["value"].get<std::string>()));
            }
            else if (value["type"] == "Multi") {
                std::vector<BindingMap> sub_bindings;
                for (auto& sub_binding : value["value"]) {
                    sub_bindings.push_back(parse_json_binding(sub_binding));
                }
                result.emplace(key, Binding(sub_bindings));
            }
            else {
                throw std::runtime_error("Invalid binding type");
            }
        }
        return result;
    }

    bool Binding::operator==(const Binding& another) const
    {
        if (kind != another.kind) {
            return false;
        }
        if (is_index()) {
            return get_index() == another.get_index();
        }
        else if (is_int()) {
            return get_int() == another.get_int();
        }
        else if (is_float()) {
            return get_float() == another.get_float();
        }
        else if (is_bool()) {
            return get_bool() == another.get_bool();
        }
        else if (is_string()) {
            return get_string() == another.get_string();
        }
        else if (is_sub_bindings()) {
            if (get_sub_binding_num() != another.get_sub_binding_num()) {
                return false;
            }
            for (int i = 0; i < get_sub_binding_num(); i++) {
                if (get_sub_binding(i) != another.get_sub_binding(i)) {
                    return false;
                }
            }
            return true;
        }
        else {
            throw std::runtime_error("Invalid binding type");
        }
    }

    uint64_t hash_binding(const BindingMap& binding) {
        std::vector<std::string> ids;
        for (auto &[key, value]: binding) {
            ids.push_back(key);
        }
        std::sort(ids.begin(), ids.end());

        std::vector<std::shared_ptr<ar::Scalar>> scalars;
        for (auto id: ids) {
            auto &value = binding.at(id);
            scalars.push_back(std::make_shared<ar::StringScalar>(id));
            if (value.is_index()) {
                scalars.push_back(std::make_shared<ar::Int64Scalar>(value.get_index()));
            } else if (value.is_int()) {
                scalars.push_back(std::make_shared<ar::Int64Scalar>(value.get_int()));
            } else if (value.is_float()) {
                scalars.push_back(std::make_shared<ar::FloatScalar>(value.get_float()));
            } else if (value.is_bool()) {
                scalars.push_back(std::make_shared<ar::BooleanScalar>(value.get_bool()));
            } else if (value.is_string()) {
                scalars.push_back(std::make_shared<ar::StringScalar>(value.get_string()));
            } else if (value.is_sub_bindings()) {
                std::vector<std::shared_ptr<ar::Scalar>> sub_scalars;
                for (int i = 0; i < value.get_sub_binding_num(); i++) {
                    sub_scalars.push_back(std::make_shared<ar::Int64Scalar>(hash_binding(value.get_sub_binding(i))));
                }
                scalars.push_back(std::make_shared<ar::Int64Scalar>(hash_scalars(sub_scalars)));
            } else {
                throw std::runtime_error("Invalid binding type");
            }
        }
        return hash_scalars(scalars);
    }
}
