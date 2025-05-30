#pragma once

#include <memory>
#include <mutex>
#include <semaphore>
#include <unordered_map>

namespace pvd
{
    struct Query
    {
        enum Message { Init, Execute, Log };
        Message msg;
        int32_t node_id;
        int64_t data_size;
    };

    struct Reply
    {
        const int64_t size;
        void *const data;
        void free() {
            delete[] ((char*)(data) - sizeof(int32_t));
        }
    };

    typedef std::function<void(Reply reply)> query_callback_t;

    class QuerySender
    {
    public:
        virtual void send(const Query& query, void* data, query_callback_t cb) = 0;
        virtual void receive(void* reply, int64_t size) = 0;
    };

    extern QuerySender* SENDER;
}
