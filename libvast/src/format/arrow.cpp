#include "arrow/api.h"
#include "arrow/builder.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/type.h"
#include "plasma/common.h"

#include "vast/error.hpp"
#include "vast/logger.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/event.hpp"

#include "vast/format/arrow.hpp"

namespace vast {

namespace {

std::pair<std::shared_ptr<arrow::Field>,
          std::vector<std::shared_ptr<arrow::ArrayBuilder>>>
convert_vast_type_to_arrow_field(const type& value, const data& data) {
    std::cout << to_string(value) << " | " << to_string(data) << std::endl;
  auto field = arrow::field("none", arrow::null());
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> builder_vector;
  // Period
  std::vector<std::shared_ptr<arrow::Field>> schema_vector_period = {
    arrow::field("num", arrow::int64()),
    arrow::field("denum", arrow::int64()),
  };
  // Timespan
  std::vector<std::shared_ptr<arrow::Field>> schema_vector_timespan = {
    arrow::field("rep", arrow::int64()),
    arrow::field("period", arrow::struct_(schema_vector_period)),
  };
  if (is<boolean_type>(value)) {
    std::cout << to_string(data) << std::endl;
    field = arrow::field("bool", arrow::boolean());
    auto current_builder = std::make_shared<arrow::BooleanBuilder>();
    // current_builder->Append(get<boolean>(data));
    current_builder->Append("1" == to_string(data));
    builder_vector.push_back(current_builder);
  } else if (is<integer_type>(value)) {
    field = arrow::field("int", arrow::int64());
    auto current_builder = std::make_shared<arrow::Int64Builder>();
    // current_builder->Append(get<integer>(data));
    try {
      current_builder->Append(std::stol(to_string(data)));
      builder_vector.push_back(current_builder);
    } catch (std::invalid_argument e) {}
      builder_vector.push_back(current_builder);
    } else if (is<count_type>(value)) {
    std::cout << to_string(data) << std::endl;
    field = arrow::field("count", arrow::uint64());
    auto current_builder = std::make_shared<arrow::UInt64Builder>();
    try {
      current_builder->Append(std::stol(to_string(data)));
      builder_vector.push_back(current_builder);
    } catch (std::invalid_argument e) {
    }
  } else if (is<real_type>(value)) {
    field = arrow::field("double", arrow::float64());
    auto current_builder = std::make_shared<arrow::FloatBuilder>();
    try {
      current_builder->Append(std::stof(to_string(data)));
      builder_vector.push_back(current_builder);
    } catch (std::invalid_argument e) {}
    builder_vector.push_back(std::make_shared<arrow::NullBuilder>());
  } else if (is<timespan_type>(value)) {
    auto timespan_struct
      = std::make_shared<arrow::StructType>(schema_vector_timespan);
    field = arrow::field("timespan", timespan_struct);
    builder_vector.push_back(std::make_shared<arrow::NullBuilder>());
  } else if (is<timestamp_type>(value)) {
    auto timespan_struct
      = std::make_shared<arrow::StructType>(schema_vector_timespan);
    // Timepoint
    std::vector<std::shared_ptr<arrow::Field>> schema_vector_timepoint = {
      arrow::field("clock", arrow::int64()),
      arrow::field("timespan", timespan_struct),
    };
    auto timepoint_struct
      = std::make_shared<arrow::StructType>(schema_vector_timepoint);
    field = arrow::field("timepoint", timepoint_struct);
    builder_vector.push_back(std::make_shared<arrow::NullBuilder>());
  } else if (is<string_type>(value)) {
    // String
    auto string_ptr = std::make_shared<arrow::StringType>();
    field = arrow::field("string", string_ptr);
    builder_vector.push_back(std::make_shared<arrow::NullBuilder>());
  } else if (is<pattern_type>(value)) {
    field = arrow::field("pattern", arrow::boolean());
  } else if (is<subnet_type>(value)) {
    auto address_ptr = std::make_shared<arrow::FixedSizeBinaryType>(16);
    std::vector<std::shared_ptr<arrow::Field>> schema_vector_subnet = {
      arrow::field("address", address_ptr),
      arrow::field("mask", arrow::int8()),
    };
    auto subnet_struct
      = std::make_shared<arrow::StructType>(schema_vector_subnet);
    field = arrow::field("subnet", subnet_struct);
    builder_vector.push_back(std::make_shared<arrow::NullBuilder>());
  } else if (is<address_type>(value)) {
    // size must set to 16 ??
    auto address_ptr = std::make_shared<arrow::FixedSizeBinaryType>(16);
    field = arrow::field("address", address_ptr);
    builder_vector.push_back(std::make_shared<arrow::NullBuilder>());
  } else if (is<port_type>(value)) {
    std::vector<std::shared_ptr<arrow::Field>> schema_vector_port = {
      arrow::field("port_type", arrow::int8()),
      arrow::field("mask", arrow::int16()),
    };
    auto port_struct = std::make_shared<arrow::StructType>(schema_vector_port);
    field = arrow::field("port", port_struct);
    builder_vector.push_back(std::make_shared<arrow::NullBuilder>());
  } else if (is<record_type>(value)) {
    auto r = get<record_type>(value);
    auto v = get<vector>(data);
    std::vector<std::shared_ptr<arrow::Field>> schema_record;
    u_int32_t i = 0;
    for (auto& e : record_type::each(r)) {
      if (i == v.size()){
        break;
      }
      auto result = convert_vast_type_to_arrow_field(e.trace.back()->type, v[i]);
      schema_record.push_back(std::move(result.first));
      builder_vector.insert(builder_vector.end(), result.second.begin(), result.second.end());
      i++;
    }
    field = arrow::field("record", arrow::struct_(schema_record));
    builder_vector.push_back(std::make_shared<arrow::NullBuilder>());
  } else {
    auto current_builder = std::make_shared<arrow::NullBuilder>();
    current_builder->AppendNull();
    builder_vector.push_back(current_builder);
  }
  return std::make_pair(field, builder_vector);
}

// Transposes a vector of events from a row-wise into the columnar Arrow
// representation in the form of a record batch.
std::shared_ptr<arrow::RecordBatch> transpose(const std::vector<event>& xs) {
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> builder_vector;
  std::vector<type> data;
  for (const auto& e : xs) {
    auto result = convert_vast_type_to_arrow_field(e.type(), e.data());
    schema_vector.push_back(std::move(result.first));
    builder_vector.insert(builder_vector.end(), result.second.begin(),
                          result.second.end());
    if (is<record_type>(e.type())){
      break;
    }
  }
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::unique_ptr<arrow::RecordBatchBuilder> builder;
  arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(),
                                  &builder);
  // builder.Append(builder_vector);
  std::shared_ptr<arrow::RecordBatch> batch;
  //auto batch = arrow::RecordBatch::Make(&schema, 1, &builder_vector);

  /*
  for (u_int32_t i = 0; i < builder->num_fields(); i++){
    std::cout << builder->GetField(i)->Append(schema_vector) << std::endl;
  }
  */

  builder->Flush(&batch);

  std::cout << schema->ToString() << std::endl;
  return batch;
}

// Writes a Record batch into an in-memory buffer.
expected<std::shared_ptr<arrow::Buffer>>
write_to_buffer(const arrow::RecordBatch& batch) {
  auto pool = arrow::default_memory_pool();
  auto buffer = std::make_shared<arrow::PoolBuffer>(pool);
  auto sink = std::make_unique<arrow::io::BufferOutputStream>(buffer);
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  auto status = arrow::ipc::RecordBatchStreamWriter::Open(
    sink.get(), batch.schema(), &writer);
  if (!status.ok())
    return make_error(ec::format_error, "failed to open arrow stream writer",
                      status.ToString());
  status = writer->WriteRecordBatch(batch);
  if (!status.ok())
    return make_error(ec::format_error, "failed to write batch",
                      status.ToString());
  status = writer->Close();
  if (!status.ok())
    return make_error(ec::format_error, "failed to close arrow stream writer",
                      status.ToString());
  status = sink->Close();
  if (!status.ok())
    return make_error(ec::format_error, "failed to close arrow stream writer",
                      status.ToString());
  return buffer;
}

} // namespace

namespace format {
namespace arrow {

writer::writer(const std::string& plasma_socket) {
  VAST_DEBUG(name(), "connects to plasma store at", plasma_socket);
  auto status
    = plasma_client_.Connect(plasma_socket, "", PLASMA_DEFAULT_RELEASE_DELAY);
  connected_ = status.ok();
  if (!connected())
    VAST_ERROR(name(), "failed to connect to plasma store", status.ToString());
}

writer::~writer() {
  auto status = plasma_client_.Disconnect();
  if (!status.ok())
    VAST_ERROR(name(), "failed to disconnect from plasma store");
}

expected<void> writer::write(const std::vector<event>& xs) {
  if (!connected())
    return make_error(ec::format_error, "not connected to plasma store");
  auto record_batch = transpose(xs);
  if (!record_batch)
    return make_error(ec::format_error, "failed to transpose events");
  auto buf = write_to_buffer(*record_batch);
  if (!buf)
    return buf.error();
  VAST_ASSERT(*buf);
  auto oid = make_object((*buf)->data(), static_cast<size_t>((*buf)->size()));
  if (!oid)
    return oid.error();
  return no_error;
}

expected<void> writer::write(const event& x) {
  buffer_.push_back(x); // TODO: avoid copy.
  return no_error;
}

expected<void> writer::flush() {
  if (buffer_.empty())
    return no_error;
  auto r = write(buffer_);
  if (!r)
    return r;
  buffer_.clear();
  return no_error;
};

const char* writer::name() const {
  return "arrow-writer";
  }

bool writer::connected() const {
  return connected_;
}

expected<plasma::ObjectID> writer::make_object(const void* data, size_t size) {
  auto oid = plasma::ObjectID::from_random();
  std::shared_ptr<Buffer> buffer;
  auto status = plasma_client_.Create(oid, static_cast<int64_t>(size), nullptr, 0, &buffer);
  if (!status.ok())
    return make_error(ec::format_error, "failed to create object",
                      status.ToString());
  std::memcpy(buffer->mutable_data(), reinterpret_cast<const char*>(data), size);
  status = plasma_client_.Seal(oid);
  if (!status.ok())
    return make_error(ec::format_error, "failed to create object",
                      status.ToString());
  VAST_DEBUG(name(), "sealed object", oid.hex(), "of size", size);
  return oid;
}

} // namespace arrow
} // namespace format
} // namespace vast
