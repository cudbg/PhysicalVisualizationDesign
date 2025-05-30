#pragma once
#include <iostream>
#include <arrow/api.h>
#include <arrow/io/api.h>


namespace pvd {

    class CloudApi {
    public:
        virtual void query(std::string sql, std::function<void(std::shared_ptr<arrow::Table>)> cb) = 0;
    };

    extern CloudApi* cloud;
}