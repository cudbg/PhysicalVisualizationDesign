#define ASIO_STANDALONE

#include <websocketpp/config/asio_no_tls.hpp>
#include <websocketpp/server.hpp>
#include <iostream>
#include <random>
#include <arrow/c/bridge.h>

#include "duckdb.hpp"

#include "network.h"
#include "plan.h"
#include "binding.h"
#include "cloud_api.h"
#include "metrics.h"

class LocalDuckdb : public pvd::CloudApi {
public:
    void query(std::string sql, std::function<void(std::shared_ptr<arrow::Table>)> cb) override
    {
        duckdb::DuckDB db("../../data/pvd.db");
        duckdb::Connection con(db);
        duckdb_arrow result;
        duckdb_query_arrow((duckdb_connection)&con, sql.c_str(), &result);

        std::cout << "Query " << sql << std::endl;

        ArrowSchema result_schema;
        duckdb_arrow_schema result_schema_ptr = (duckdb_arrow_schema)&result_schema;
        duckdb_query_arrow_schema(result, &result_schema_ptr);

        auto arrow_schema = arrow::ImportSchema(&result_schema).ValueOrDie();

        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;

        while (true) {
            auto result_array = new ArrowArray();
            duckdb_query_arrow_array(result, (duckdb_arrow_array*)&result_array);
            auto batch_result = arrow::ImportRecordBatch(result_array, arrow_schema);
            if (batch_result.ok()) {
                batches.push_back(batch_result.ValueOrDie());
            } else {
                delete result_array;
                break;
            }
            delete result_array;
        }

        auto table = arrow::Table::FromRecordBatches(batches).ValueOrDie();
        cb(table);
    }
};


typedef websocketpp::server<websocketpp::config::asio> webserver;
webserver server;

// The global SENDER is always nullptr in server
pvd::QuerySender* pvd::SENDER = nullptr;

pvd::CloudApi* pvd::cloud = new LocalDuckdb();

std::ofstream pvd::log_file("pvd_log.txt", std::ios::out | std::ios::app);

// plan root id -> plan
//std::map<int, std::shared_ptr<pvd::Plan>> registered_plans;
int cur_plan_id = 0;
std::shared_ptr<pvd::Plan> cur_plan;
// plan node id -> plan root id
std::map<int, int> node_to_root;

void recursive_set_node_to_root(std::shared_ptr<pvd::Plan> plan, int root, std::map<int, int>& node_to_root) {
    node_to_root[plan->id] = root;
    for (auto input: plan->input_plans()) {
        recursive_set_node_to_root(input, root, node_to_root);
    }
}

void on_message(websocketpp::connection_hdl hdl, webserver::message_ptr msg) {
    // received the binary data
    const char* data = msg->get_payload().data();
    // the first 4-bytes of the data denotes the query id
    int32_t query_id = *(int32_t*)data;
    // the following sizeof(Query) bytes of the data is the query info
    pvd::Query* query = (pvd::Query*)(data + sizeof(int32_t));
    // the remaining bytes of the data is the query content
    // if is a init query, the content is the plan json string
    // if is a execution query, the content is the binding json string
    void* content = (void*)(data + sizeof(int32_t) + sizeof(pvd::Query));

    switch (query->msg)
    {
        // initialize a plan
        case pvd::Query::Message::Init: {
            std::cout << "Init" << std::endl;
            // message content is the json string of the plan
            std::string plan_json = (char*)content;
            std::cout << plan_json << std::endl;
            pvd::logging("{\"register_plan\": \"" + plan_json + "\"}");
            // parse the json string to a plan
            auto plan = pvd::parse_json_plan(json::parse(plan_json));
            // initialize the plan, register the plan with the root id
            cur_plan_id = plan->id;
            cur_plan = plan;
            // registered_plans[plan->id] = plan;
            node_to_root.clear();
            node_to_root[plan->id] = plan->id;

            recursive_set_node_to_root(plan, plan->id, node_to_root);

            // Find SCache and initialize
            try {
                std::cout << "Initializing Plan" << std::endl;
                plan->initialize([plan, query_id, hdl]() {
                    std::cout << "Plan Initialized" << std::endl;
                    auto out = ar::io::BufferOutputStream::Create().ValueOrDie();
                    { auto _ = out->Write(reinterpret_cast<const uint8_t *>(&query_id), sizeof(query_id)); }
                    auto plan_str = plan->to_string();
                    { auto _ = out->Write(plan_str.c_str(), plan_str.size() + 1); }
                    std::cout << "Sending Plan" << std::endl;
                    std::cout << plan->to_string() << std::endl;
                    auto buffer = out->Finish().ValueOrDie();
                    server.send(hdl, buffer->data(), buffer->size(), websocketpp::frame::opcode::binary);
                });
            }
            catch (std::exception& e) {
                std::string error = "ERROR: " + std::string(e.what());
                server.send(hdl, error.c_str(), error.size() + 1, websocketpp::frame::opcode::text);
            }
            break;
        }
        case pvd::Query::Message::Execute: {
            int node = query->node_id;
            std::cout << "Execute " << node << std::endl;
            if (!node_to_root.contains(node)) {
                std::string error = "ERROR: Plan id not found: " + std::to_string(node);
                server.send(hdl, error.c_str(), error.size() + 1, websocketpp::frame::opcode::text);
            }
            auto plan = cur_plan; // registered_plans.at(node_to_root.at(node));
            std::cout <<"Found plan " << plan->id << std::endl;
            // query content is the binding json string
            std::string binding_json = (char*)content;
            std::cout << binding_json << std::endl;
            // parse the json string to a binding
            auto binding = pvd::parse_json_binding(json::parse(binding_json));
            std::cout <<"Parsed binding " << std::endl;
            plan->execute_subplan(binding, node, [hdl, query_id](std::shared_ptr<pvd::SerialData> data) {
                std::cout <<"Plan Executed " << std::endl;
                // send the result to the client
                auto out = ar::io::BufferOutputStream::Create().ValueOrDie();
                { auto _ = out->Write(reinterpret_cast<const uint8_t *>(&query_id), sizeof(query_id)); }
                data->serialize(out);
                auto buffer = out->Finish().ValueOrDie();
                server.send(hdl, buffer->data(), buffer->size(), websocketpp::frame::opcode::binary);
            });
            break;
        }
        case pvd::Query::Message::Log: {
            std::string log = (char*)content;
            pvd::logging(log);
            break;
        }
    }
}

int main()
{
    server.set_message_handler(&on_message);
    server.clear_access_channels(websocketpp::log::alevel::frame_header | websocketpp::log::alevel::frame_payload);

    server.init_asio();

    srand(static_cast<unsigned int>(time(nullptr)));

    int port = rand() % 50000 + 8000;
    server.listen(port);
    server.start_accept();

    std::cout << "Server started at port " << port << std::endl;

    server.run();
    return 0;
}
