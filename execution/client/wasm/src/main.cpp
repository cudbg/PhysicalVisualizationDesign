#include <emscripten/emscripten.h>
#include <emscripten/websocket.h>
#include <emscripten/bind.h>
#include <emscripten/val.h>
#include <iostream>
#include <stdio.h>
#include "pvd_client.h"
#include "cloud_api.h"
#include "arrow_utils.h"

// The websocket connection to the server
EMSCRIPTEN_WEBSOCKET_T ws = 0;
// The global sender instance used to send queries to the server
pvd::QuerySender* pvd::SENDER = nullptr;
// The callback function for the websocket special message (inited, error, closed)
// assume to be void msg(std::string result)
emscripten::val msg_cb;

pvd::CloudApi* pvd::cloud;

std::ofstream pvd::log_file;

EM_BOOL onopen(int eventType, const EmscriptenWebSocketOpenEvent *websocketEvent, void *userData) {
    // create the sender instance when the websocket is connected
    pvd::SENDER = new pvd::WasmSender(websocketEvent->socket);
    msg_cb(std::string("websocket connected"));
    return EM_TRUE;
}

EM_BOOL onerror(int eventType, const EmscriptenWebSocketErrorEvent *websocketEvent, void *userData) {
    msg_cb(std::string("websocket error"));
    return EM_TRUE;
}

EM_BOOL onclose(int eventType, const EmscriptenWebSocketCloseEvent *websocketEvent, void *userData) {
    msg_cb(std::string("websocket closed"));
    return EM_TRUE;
}

EM_BOOL onmessage(int eventType, const EmscriptenWebSocketMessageEvent *websocketEvent, void *userData) {
    if (pvd::SENDER == nullptr) {
        msg_cb(std::string("message comes but sender hasn't been initialized"));
        return EM_TRUE;
    }
    else {
        if (websocketEvent->numBytes > 5 && strncmp((const char*)websocketEvent->data, "ERROR", 5) == 0) {
            msg_cb(std::string((char*)websocketEvent->data));
        }
        else {
            pvd::SENDER->receive(websocketEvent->data, websocketEvent->numBytes);
        }
    }
    return EM_TRUE;
}

// register_done_cb: assume to be void register_done_cb()
void register_plan(std::string plan_json, emscripten::val register_done_cb)
{
    if (ws == 0) {
        msg_cb(std::string("websocket not connected"));
    }
    else {
        try {
            pvd::register_new_plan(plan_json, [register_done_cb](std::string plan_str) {
                register_done_cb(plan_str);
            });
        }
        catch (std::exception& e) {
            msg_cb(std::string(e.what()));
        }
    }
}

// execute_cb: assume to be void execute_cb(long buffer_ptr, long buffer_size)
// convert char* buffer_ptr to long in the argument
void execute_plan(int node_id, std::string binding, emscripten::val execute_cb)
{
    if (ws == 0) {
        msg_cb(std::string("websocket not connected"));
    }
    else {
        try
        {
            //std::cout << "Executing with binding: " << binding << std::endl;
            pvd::execute_plan(node_id, binding, [execute_cb](std::shared_ptr<pvd::SerialData> data) {
                auto table = std::dynamic_pointer_cast<pvd::TableData>(data);
                //std::cout << "Result Table " << table->table->num_rows() << " rows" << std::endl;

                auto buf = table_to_buffer(table->table, nullptr, 0);
                //std::cout << "Buffer size: " << buf->size() << std::endl;
                // have to copy the data to a manually allocated buffer to make things work
                char* tmp_buf = new char[buf->size() + 4];
                memcpy(tmp_buf, buf->data(), buf->size());

                // invoke the callback function
                //std::cout << (long)tmp_buf << std::endl;
                //std::cout << *(unsigned char*)(tmp_buf + buf->size() - 1) << std::endl;
                execute_cb((unsigned long)tmp_buf, (unsigned long)buf->size());
                // delete the buffer after the callback function
                delete[] tmp_buf;
                //std::cout << "Buffer deleted" << std::endl;
            });
        }
        catch (std::exception& e) {
            msg_cb(std::string(e.what()));
        }
    }
}

void init(std::string port, emscripten::val cb) {
    msg_cb = cb;

    if (!emscripten_websocket_is_supported()) {
        msg_cb(std::string("websocket not supported"));
    }

    EmscriptenWebSocketCreateAttributes ws_attrs = {
        (std::string("ws://localhost:") + port).c_str(),
        NULL,
        EM_FALSE
    };
    ws = emscripten_websocket_new(&ws_attrs);

    emscripten_websocket_set_onopen_callback(ws, NULL, onopen);
    emscripten_websocket_set_onerror_callback(ws, NULL, onerror);
    emscripten_websocket_set_onclose_callback(ws, NULL, onclose);
    emscripten_websocket_set_onmessage_callback(ws, NULL, onmessage);
}

EMSCRIPTEN_BINDINGS() {
    emscripten::function("init", &init);
    emscripten::function("register", &register_plan);
    emscripten::function("execute", &execute_plan);
}
