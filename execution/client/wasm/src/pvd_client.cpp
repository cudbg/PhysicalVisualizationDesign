#include "pvd_client.h"
#include "json.h"
#include <iostream>

using json = nlohmann::json;

namespace pvd
{
    /*
     * Plan root id -> Plan
     */
    int current_id = 0;
    std::shared_ptr<Plan> current_plan = nullptr;

    void register_new_plan(const std::string& plan_json, std::function<void(std::string plan_str)> cb)
    {
        std::cout << "Registering new plan" << std::endl;
        std::cout << plan_json << std::endl;
        // parse the json string to a plan
        json json_obj = json::parse(plan_json);
        auto plan = parse_json_plan(json_obj);

        // register the plan with the root id
        current_id = plan->id;
        current_plan = plan;

        // send the plan json to the server and initialize the server side plan
        Query query = {Query::Init, plan->id, plan_json.size() + 1};
        SENDER->send(query, (void*)plan_json.c_str(), 
                    [plan, cb](Reply reply) { 
                        plan->initialize([plan, cb]() {
                            cb(plan->to_string());
                        });
                    });
    }

    void execute_plan(int node_id, const std::string& binding, execute_callback_t cb)
    {
        // node_id should be the root id of the plan to execute
        if (current_id != node_id) {
            throw std::runtime_error("Plan id " + std::to_string(node_id) + " is not registered");
        }

        // look up the plan and execute it
        auto plan = current_plan;
        auto binding_json = json::parse(binding);
        auto bind = parse_json_binding(binding_json);
        plan->execute_subplan(bind, node_id, cb);
    }
}
