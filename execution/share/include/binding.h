#pragma once

#include <map>
#include <vector>
#include <variant>
#include <string>

#include "json.h"

using json = nlohmann::json;

namespace pvd 
{

class Binding;
typedef std::map<std::string, Binding> BindingMap;

class Binding
{
public:
    enum Kind { Index, Int, Float, Bool, String, Multi };

    Kind kind;
    std::variant<int, float, bool, std::string, std::vector<BindingMap> > _value;

    // constructors
    Binding(Kind kind, int value) : kind(kind), _value(value) {}
    Binding(float value) : kind(Kind::Float), _value(value) {}
    Binding(bool value) : kind(Kind::Bool), _value(value) {}
    Binding(const std::string& value) : kind(Kind::String), _value(value) {}
    Binding(const std::vector<BindingMap> &value) : kind(Kind::String), _value(value) {}

    // identify binding type
    // for AnyNode
    bool is_index() const;
    // for ValNode
    bool is_int() const;
    bool is_float() const;
    bool is_bool() const;
    bool is_string() const;
    // for Multi
    bool is_sub_bindings() const;

    // read binding value
    int get_index() const;
    int get_int() const;
    float get_float() const;
    bool get_bool() const;
    const std::string& get_string() const;
    int get_sub_binding_num() const;
    const BindingMap& get_sub_binding(int i) const;

    // serialize/deserialize
    void to_json(json& binding);
    std::string to_string();

    // default copy constructors and assignments
    Binding(const Binding&) = default;
    Binding(Binding&&) = default;
    Binding &operator=(const Binding&) = default;
    Binding &operator=(Binding&&) = default;

    bool operator==(const Binding& another) const;
};

void binding_to_json(const BindingMap& binding, json& binding_json);
BindingMap parse_json_binding(const json& binding);
uint64_t hash_binding(const BindingMap& binding);

}
