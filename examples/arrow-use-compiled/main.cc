#include <arrow/api.h>

#include <iostream>
#include <unistd.h>



arrow::Status RunMain() {
  auto arrowBuildInfo = arrow::GetBuildInfo();
  
  std::cout << "Arrow version=" << arrowBuildInfo.version_string << std::endl;

  // arrow::memory_pool::ThymesismallocAllocator::AddDevice
  arrow::thymesis_mapped_region region = {
    .id = 0,
    .is_remote = false,
    .start = (void *)4096,
    .length = 4096,
    .is_mapped = false,
    .pcie_bus = 7,
    .pcie_slot = 1,
    .pcie_function = 0,
    .pcie_bar = 2
  };
  ARROW_RETURN_NOT_OK(arrow::thymesis_add_mapping(region));
  arrow::thymesis_print_maps();

  // return arrow::Status::OK();

  // Builders are the main way to create Arrays in Arrow from existing values that are not
  // on-disk. In this case, we'll make a simple array, and feed that in.
  // Data types are important as ever, and there is a Builder for each compatible type;
  // in this case, int8.
  arrow::Int8Builder int8builder;
  // ARROW_RETURN_NOT_OK(int8builder.Reserve(1000));
  std::cout << "int8builder init" << std::endl;


  int8_t days_raw[128];
  for (uint8_t i = 0; i < 128; i++) {
    days_raw[i] = -127/2 + i;
  }
  
  int8_t days_raw2[10] = {4, 112, 124, 13, 102, 13, 16, 110, 16, 71};

  for (int i = 0; i < 4096/128/3; i++) {
    std::cout << "i=" << i << std::endl;
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 128));
  }
  std::cout << "days appended" << std::endl;


  // We only have a Builder though, not an Array -- the following code pushes out the
  // built up data into a proper Array.
  std::shared_ptr<arrow::Array> days;
  ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());


  ARROW_RETURN_NOT_OK((*days).Validate());
  ARROW_RETURN_NOT_OK((*days).ValidateFull());
  // std::cout << (*(*days).data()).child_data[0] << std::endl;
  // std::cout << "days: 0="<< *((*days).data() + 1) << " 1=" << (*days).data()[1] << std::endl;
  std::cout << "days done" << std::endl;

  // Builders clear their state every time they fill an Array, so if the type is the same,
  // we can re-use the builder. We do that here for month values.
  int8_t months_raw[5] = {1, 3, 5, 7, 1};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));
  std::shared_ptr<arrow::Array> months;
  ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());
  std::cout << "months" << std::endl;

  // Now that we change to int16, we use the Builder for that data type instead.
  arrow::Int16Builder int16builder;
  int16_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
  ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
  std::shared_ptr<arrow::Array> years;
  ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());
  std::cout << "years" << std::endl;

  // Now, we want a RecordBatch, which has columns and labels for said columns.
  // This gets us to the 2d data structures we want in Arrow.
  // These are defined by schema, which have fields -- here we get both those object types
  // ready.
  std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  std::shared_ptr<arrow::Schema> schema;

  // Every field needs its name and data type.
  field_day = arrow::field("Day", arrow::int8());
  field_month = arrow::field("Month", arrow::int8());
  field_year = arrow::field("Year", arrow::int16());

  // The schema can be built from a vector of fields, and we do so here.
  schema = arrow::schema({field_day, field_month, field_year});

  // With the schema and Arrays full of data, we can make our RecordBatch! Here,
  // each column is internally contiguous. This is in opposition to Tables, which we'll
  // see next.
  std::shared_ptr<arrow::RecordBatch> rbatch;
  // The RecordBatch needs the schema, length for columns, which all must match,
  // and the actual data itself.
  rbatch = arrow::RecordBatch::Make(schema, days->length(), {days, months, years});

  std::cout << rbatch->ToString();

  // std::shared_ptr<arrow::RecordBatchBuilder> rbuilder;
  // (arrow::RecordBatchBuilder(schema, arrow::default_memory_pool(), 0));

  {// This is ugly and you should not do this in production code.

    // In order to see the memory mappings of the currently
    // running process, we use the pmap (for process-map) tool to
    // query the kernel (/proc/self/maps)
    char cmd[256];
    snprintf(cmd, 256, "pmap %d", getpid());
    printf("---- system(\"%s\"):\n", cmd);
    system(cmd);
  }

  return arrow::Status::OK();

}

int main() {
  arrow::Status st = RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    std::cout << "NOT OK" << std::endl;
    return 1;
  }
  std::cout << "OK" << std::endl;
  return 0;
}

