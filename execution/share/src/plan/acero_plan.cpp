#include "plan.h"
#include "binding.h"
#include "metrics.h"

namespace pvd
{
    void AceroPlan::compile(const BindingMap& binding, compile_callback_t cb)
    {
        /*
        if (auto acero_input = std::dynamic_pointer_cast<AceroPlan>(input)) {
            acero_input->compile(binding, [this, binding, cb](ac::Declaration input_plan) {
                cb(this->build_plan(binding, std::move(input_plan)));
            });
        }
        else {
         */
            input->execute(binding, [this, binding, cb](std::shared_ptr<SerialData> data) {
                //std::cout << "Acero compile data " << (unsigned long) data.get() << std::endl;
                auto table = std::dynamic_pointer_cast<TableData>(data);
                //std::cout << "Acero compile table " << (unsigned long) table->table->num_rows() << std::endl;
                metrics.record_input(table->table);
                auto table_source_option = ac::TableSourceNodeOptions{table->table, MAX_BATCH_SIZE};
                auto source = ac::Declaration("table_source", {}, table_source_option);
                cb(this->build_plan(binding, source));
            });
        //}
    }

    void AceroPlan::execute(const BindingMap& binding, execute_callback_t cb)
    {
        compile(binding, [this, cb](ac::Declaration plan) {
            auto table = std::make_shared<TableData>(ac::DeclarationToTable(plan).ValueOrDie());
            metrics.record_output(table->table);
            cb(table);
        });
    }
}
