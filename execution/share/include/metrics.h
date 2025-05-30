#pragma once

#include <iostream>
#include <fstream>
#include <string>
#include <chrono>
#include <unistd.h>
#include <network.h>

namespace pvd
{
    extern std::ofstream log_file;

    static void logging(std::string message) {
        if (SENDER) {
            // is client
            Query query(Query::Message::Log, 0, message.size() + 1);
            SENDER->send(query, (void*)message.c_str(), [](Reply _) {});
        }
        else {
            log_file << message << std::endl;
            log_file.flush();
        }
    }

    struct Metrics
    {
        int id;
        std::string node;
        uint64_t input_time;
        uint64_t input_num_rows;
        uint64_t input_num_cols;
        uint64_t output_time;
        uint64_t output_num_rows;
        uint64_t output_num_cols;
        uint64_t build_size;

        Metrics() {}

        static uint64_t get_time() {
            return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        }

        void record_input(std::shared_ptr<arrow::Table> table, uint64_t _num_rows = 0, uint64_t _num_cols = 0) {
            input_time = get_time();
            if (table) {
                input_num_rows = table->num_rows();
                input_num_cols = table->num_columns();
            }
            else {
                input_num_rows = _num_rows;
                input_num_cols = _num_cols;
            }
        }

        void record_output(std::shared_ptr<arrow::Table> table, uint64_t _size = 0, uint64_t _num_rows = 0, uint64_t _num_cols = 0) {
            output_time = get_time();
            build_size = _size;
            if (table) {
                output_num_rows = table->num_rows();
                output_num_cols = table->num_columns();
            }
            else {
                output_num_rows = _num_rows;
                output_num_cols = _num_cols;
            }
            logging(to_json());
        }

        std::string to_json() {
            std::string loc;
            if (SENDER) loc = "client";
            else loc = "server";
            return std::string("{") +\
            "\"id\": \"" + std::to_string(id) + "\"," +\
            "\"node\": \"" + node + "\"," +\
            "\"location\": \"" + loc + "\"," +\
            "\"input_num_rows\": " + std::to_string(input_num_rows) + "," +\
            "\"input_num_cols\": " + std::to_string(input_num_cols) + "," +\
            "\"output_num_rows\": " + std::to_string(output_num_rows) + "," +\
            "\"output_num_cols\": " + std::to_string(output_num_cols) + "," +\
            "\"input_time\": " + std::to_string(input_time) + "," +\
            "\"output_time\": " + std::to_string(output_time) + "," +\
            "\"exec_time\": " + std::to_string(output_time - input_time) + "," +\
            "\"build_size\": " + std::to_string(build_size) + "}";
        }
    };
}
