#pragma once

#include <emscripten/emscripten.h>
#include <emscripten/websocket.h>

#include "plan.h"
#include "network.h"

namespace pvd
{
    void register_new_plan(const std::string& plan_json, std::function<void(std::string plan_str)> cb);
    void execute_plan(int node_id, const std::string& binding, execute_callback_t cb);

    class WasmSender : public pvd::QuerySender
    {
    public:
        int max_id = 0;
        query_callback_t query_callback[1000];

        EMSCRIPTEN_WEBSOCKET_T ws;

        void send_query(int32_t id, const pvd::Query& query, void* data) {
            // can we optimize this copy?
            unsigned buf_size = sizeof(int32_t) + sizeof(query) + query.data_size;
            char* buf = new char[buf_size];
            *(int32_t*)buf = id;
            memcpy(buf + sizeof(int32_t), &query, sizeof(query));
            puts((char*)data);
            memcpy(buf + sizeof(int32_t) + sizeof(query), data, query.data_size);
            emscripten_websocket_send_binary(ws, (void*)(buf), buf_size);
            delete[] buf;
        }

        void send(const Query& query, void* data, query_callback_t cb) {
            int id = max_id++;
            query_callback[id] = cb;
            send_query(id, query, data);
            printf("sent query id %d msg %d\n", id, query.msg);
        };

        void receive(void* reply, int64_t size) {
            int32_t id = *(int32_t*)reply;
            Reply r = Reply{static_cast<int64_t>(size - sizeof(int32_t)), (void*)((char*)reply + sizeof(int32_t))};
            query_callback_t cb = query_callback[id];
            cb(r);
        }

        WasmSender(EMSCRIPTEN_WEBSOCKET_T ws) : ws(ws) {}
    };

}
