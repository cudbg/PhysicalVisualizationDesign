#pragma once

#include <iostream>
#include <vector>
#include <type_traits>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/acero/exec_plan.h>
#include <arrow/acero/options.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>

namespace ar = arrow;
namespace cp = arrow::compute;
namespace ac = arrow::acero;

const int MAX_BATCH_SIZE = 1000000000;

// Convert a std::vector to an arrow::Array
// Currently support:
// - int, float, std::string, bool
template <typename T>
static std::shared_ptr<arrow::Array> vec_to_array(const std::vector<T>& vec)
{
    if constexpr (std::is_same_v<T, int>) {
        arrow::Int32Builder builder;
        auto _ = builder.AppendValues(vec);
        return std::move(builder.Finish().ValueOrDie());
    }
    else if constexpr (std::is_same_v<T, float>) {
        arrow::FloatBuilder builder;
        auto _ = builder.AppendValues(vec);
        return std::move(builder.Finish().ValueOrDie());
    }
    else if constexpr (std::is_same_v<T, std::string>) {
        arrow::StringBuilder builder;
        auto _ = builder.AppendValues(vec);
        return std::move(builder.Finish().ValueOrDie());
    }
    else if constexpr (std::is_same_v<T, bool>) {
        arrow::BooleanBuilder builder;
        auto _ = builder.AppendValues(vec);
        return std::move(builder.Finish().ValueOrDie());
    }
    throw std::runtime_error("Unsupported type in vec_to_array");
}

// Build an arrow::Schema from a vector of {name, arrow::DataType} pairs
static std::shared_ptr<arrow::Schema> build_schema(
    const std::vector<std::pair<std::string, std::shared_ptr<arrow::DataType>>>& fields)
{
    std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
    for (const auto& field : fields) {
        arrow_fields.push_back(arrow::field(field.first, field.second));
    }
    return arrow::schema(arrow_fields);
}

static uint64_t hash_str(const std::string& s)
{
    uint64_t h = 37;
    for (char c : s) {
        h = (h * 54059) ^ ((uint64_t)c * 76963);
    }
    return h;
}

static uint64_t hash_scalars(const std::vector<std::shared_ptr<arrow::Scalar>>& array)
{
    uint64_t seed = 0;
    for (int i = 0; i < array.size(); i++) {
        seed ^= (uint64_t)(hash_str(array[i]->ToString())) + 0x9e3779b97f4a7c15 + (seed << 6) + (seed >> 2);
    }
    return seed;
}

struct CmpScalar {
    bool operator()(std::shared_ptr<ar::Scalar> lhs, std::shared_ptr<ar::Scalar> rhs) const {
        if (lhs->type->id() == ar::Type::STRING) {
            return std::dynamic_pointer_cast<ar::StringScalar>(lhs)->value < std::dynamic_pointer_cast<ar::StringScalar>(rhs)->value;
        }
        double a = std::dynamic_pointer_cast<ar::DoubleScalar>(lhs->CastTo(ar::float64()).ValueOrDie())->value;
        double b = std::dynamic_pointer_cast<ar::DoubleScalar>(rhs->CastTo(ar::float64()).ValueOrDie())->value;
        return a < b;
    }
};

static std::shared_ptr<arrow::Table> select_table_rows(const std::shared_ptr<arrow::Table>& table, const std::vector<int64_t>& indices)
{
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < table->num_columns(); ++i) {
        auto array = table->column(i);
        auto builder = arrow::MakeBuilder(array->type()).ValueOrDie();
        for (int j = 0; j < indices.size(); ++j) {
            auto _ = builder->AppendScalar(*array->GetScalar(indices[j]).ValueOrDie());
        }
        arrays.push_back(builder->Finish().ValueOrDie());
    }
    return arrow::Table::Make(table->schema(), arrays);
}

static std::shared_ptr<arrow::Table> slice_table(const std::shared_ptr<arrow::Table>& table, int64_t offset, int64_t length)
{
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (int i = 0; i < table->num_columns(); ++i) {
        auto array = table->column(i);
        auto builder = arrow::MakeBuilder(array->type()).ValueOrDie();
        for (int j = 0; j < length; ++j) {
            auto _ = builder->AppendScalar(*array->GetScalar(offset + j).ValueOrDie());
        }
        arrays.push_back(builder->Finish().ValueOrDie());
    }
    return arrow::Table::Make(table->schema(), arrays);
}

static std::shared_ptr<arrow::Table> empty_table_from_schema(std::shared_ptr<arrow::Schema> schema)
{
    std::vector<std::shared_ptr<arrow::Array>> empty_arrays{};
    for (int j = 0; j < schema->num_fields(); ++j) {
        auto array = arrow::MakeBuilder(schema->field(j)->type()).ValueOrDie();
        empty_arrays.push_back(array->Finish().ValueOrDie());
    }
    return arrow::Table::Make(schema, empty_arrays);
}

static ac::Declaration get_test_plan(std::shared_ptr<arrow::Table> table)
{
    auto table_source_option = ac::TableSourceNodeOptions{table, MAX_BATCH_SIZE};
    auto filter_option = ac::FilterNodeOptions{
        cp::greater(cp::field_ref("a"), cp::literal(3))};
    auto project_option = ac::ProjectNodeOptions{{cp::field_ref("a"), cp::field_ref("b")}};

    auto table_source_node = ac::Declaration("table_source", {}, table_source_option);
    auto filter_node = ac::Declaration("filter", {table_source_node}, filter_option);
    auto project_node = ac::Declaration("project", {filter_node}, project_option);
    
    return std::move(project_node);
}

// write an arrow Table to a buffer stream
static std::shared_ptr<arrow::Buffer> table_to_buffer(const std::shared_ptr<arrow::Table>& table, const void* msg, int64_t size)
{
    auto sink = arrow::io::BufferOutputStream::Create().ValueOrDie();
    if (msg != nullptr) {
        auto _ = sink->Write(msg, size);
    }
    auto writer = arrow::ipc::MakeFileWriter(sink, table->schema()).ValueOrDie();
    auto batch = table->CombineChunksToBatch().ValueOrDie();
    { auto _ = writer->WriteRecordBatch(*batch); }
    { auto _ = writer->Close(); }
    auto buffer = sink->Finish();
    return std::move(buffer.ValueOrDie());
}

// read a arrow record batch from a buffer stream
static std::shared_ptr<arrow::Table> buffer_to_table(const uint8_t* data, size_t size)
{
    auto buffer = std::make_shared<arrow::Buffer>(data, size);
    arrow::io::BufferReader source(buffer);
    auto reader = arrow::ipc::RecordBatchFileReader::Open(&source).ValueOrDie();

    auto schema = reader->schema();

    if (reader->CountRows().ValueOr(-1) == -1)
        return empty_table_from_schema(schema);
    else
        return reader->ToTable().ValueOrDie();
}
