#include "plan.h"

namespace pvd 
{
    Plan::Plan(int id) : id(id) {};

    void Plan::execute_subplan(const BindingMap& binding, int id, execute_callback_t cb)
    {
        if (this->id != id) {
            // not executing the current node, executing one of the child node
            for (auto& input : input_plans()) {
                // try executing each child node
                // will only truly execute if the id matches
                // will return nullptr if the id is not found
                input->execute_subplan(binding, id, cb);
            }
        }
        else {
            execute(binding, [cb](std::shared_ptr<SerialData> data) { cb(data); });
        }
    }

    void Plan::_initialize(build_callback_t cb, std::vector<std::shared_ptr<Plan>> inputs)
    {
        if (inputs.size() == 0) {
            if (!SENDER && !at_server()) {
                cb();
                return;
            }
            // no more inputs to initialize
            if (auto scache = dynamic_cast<SCache*>(this))  {
                scache->cache_data(cb);
            }
            else {
                cb();
            }
        }
        else {
            auto input = inputs[0];
            auto other_inputs = std::vector<std::shared_ptr<Plan>>(inputs.begin() + 1, inputs.end());
            input->initialize([this, cb, other_inputs]() {
                this->_initialize(cb, other_inputs);
            });
        }
    }

    bool Plan::at_server() const {
        if (dynamic_cast<const Network*>(this)) {
            return false;
        }
        else {
            if (input_plans().empty()) {
                return true;
            }
            return input_plans()[0]->at_server();
        }
    }

    void Plan::initialize(build_callback_t cb)
    {
        // SENDER is nullptr <=> this is server-Side
        if (SENDER && dynamic_cast<Network*>(this)) {
            cb();
            return;
        }
        std::vector<std::shared_ptr<Plan>> inputs = input_plans();
        _initialize(cb, inputs);
    }
}
