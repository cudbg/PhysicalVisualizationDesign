#include "plan.h"

namespace pvd 
{
    void TableData::serialize(std::shared_ptr<ar::io::BufferOutputStream> out)
    {
        auto writer = ar::ipc::MakeStreamWriter(out, table->schema()).ValueOrDie();
        auto batch = table->CombineChunksToBatch().ValueOrDie();
        std::cout << "Serializing " << batch->num_rows() << " rows" << std::endl;
        std::cout << batch->schema()->ToString() << std::endl;
        { auto _ = writer->WriteRecordBatch(*batch); }
        { auto _ = writer->Close(); }
    }

    void TableData::deserialize(std::shared_ptr<ar::io::BufferReader> buffer)
    {
        std::cout << "Descrializing " << std::endl;
        auto reader = ar::ipc::RecordBatchStreamReader::Open(buffer).ValueOrDie();
        std::cout << "reader created " << std::endl;
        auto schema = reader->schema();
        std::cout << "schema read " << std::endl;

        table = reader->ToTable().ValueOrDie();
        std::cout << "Table read " << table->num_rows() << " rows" << std::endl;
        if (table->num_rows() == 0) {
            table = empty_table_from_schema(schema);
        }
    }

    std::shared_ptr<TableData> TableData::select_rows(const std::vector<int64_t> &indices)
    {
        std::vector<std::shared_ptr<ar::Array>> arrays;
        for (int i = 0; i < table->num_columns(); ++i) {
            auto array = table->column(i);
            auto builder = ar::MakeBuilder(array->type()).ValueOrDie();
            for (int j = 0; j < indices.size(); ++j) {
                auto _ = builder->AppendScalar(*array->GetScalar(indices[j]).ValueOrDie());
            }
            arrays.push_back(builder->Finish().ValueOrDie());
        }
        return std::make_shared<TableData>(ar::Table::Make(table->schema(), arrays));
    }

    uint64_t TableData::size()
    {
        uint64_t total_size = 0;
        for (int i = 0; i < table->num_columns(); ++i) {
            auto col = table->column(i);
            for (int j = 0; j < col->num_chunks(); ++j) {
                auto chunk = col->chunk(j);
                for (auto& buf : chunk->data()->buffers)
                    if (buf)
                        total_size += buf->size();
            }
        }
        return total_size;
    }
}
