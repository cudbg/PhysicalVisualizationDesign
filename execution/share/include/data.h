#pragma once

#include <iostream>
#include <vector>
#include <memory>
#include <variant>
#include <map>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>
#include "metrics.h"

namespace ar = arrow;
namespace cp = arrow::compute;
namespace ac = arrow::acero;

namespace pvd {

    struct SerialData {
        // Serialize the data to the output stream
        virtual void serialize(std::shared_ptr<ar::io::BufferOutputStream> out) = 0;
        // Deserialize (restore) the data from the input stream
        virtual void deserialize(std::shared_ptr<ar::io::BufferReader> buffer) = 0;

        virtual uint64_t size() = 0;
    };

    // A wrapper of arrow::Table
    struct TableData : public SerialData {
        std::shared_ptr<ar::Table> table;

        TableData() : table(nullptr) {}

        TableData(std::shared_ptr<ar::Table> table) {
            int total_chunks = 0;
            for (int i = 0; i < table->num_columns(); i++) {
                total_chunks += table->column(i)->num_chunks();
            }
            if (total_chunks / table->num_columns() * 1000 > table->num_rows()) {
                table = table->CombineChunks().ValueOrDie();
            }
            this->table = table;
        }

        void serialize(std::shared_ptr<ar::io::BufferOutputStream> out) override;

        void deserialize(std::shared_ptr<ar::io::BufferReader> buffer) override;

        std::shared_ptr<TableData> select_rows(const std::vector<int64_t> &indices);

        uint64_t size() override;
    };
}
