#include "arrow/api.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/type.h"
#include "arrow/builder.h"
#include "plasma/common.h"

#include "vast/error.hpp"
#include "vast/logger.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/event.hpp"

#include "vast/format/arrow.hpp"

namespace vast {

namespace {

std::shared_ptr<arrow::Field> convert_vast_type_to_arrow_field(type& value) {
  std::shared_ptr<arrow::Field> field;
  // Period
  std::vector<std::shared_ptr<arrow::Field>> schema_period = {
    arrow::field("num", arrow::int64()),
    arrow::field("denum", arrow::int64()),
  };
  // Timespan
  std::vector<std::shared_ptr<arrow::Field>> schema_timespan = {
    arrow::field("rep", arrow::int64()),
    arrow::field("period", arrow::struct_(schema_period)),
  };
  // Timepoint
  std::vector<std::shared_ptr<arrow::Field>> schema_timepoint = {
    arrow::field("clock", arrow::int64()),
    arrow::field("timespan", arrow::struct_(schema_timespan)),
  };
  if ("bool" == value.name()) {
    field
      = std::make_shared<arrow::Field>(arrow::Field("bool", arrow::boolean()));
  } else if ("int" == value.name()) {
    field = std::make_shared<arrow::Field>(arrow::Field("int", arrow::int64()));
  } else if ("count" == value.name()) {
    field
      = std::make_shared<arrow::Field>(arrow::Field("count", arrow::uint64()));
  } else if ("double" == value.name()) {
    field = std::make_shared<arrow::Field>(
      arrow::Field("double", arrow::float64()));
  } else if ("timespan" == value.name()) {
    field = std::make_shared<arrow::Field>(
      arrow::Field("timespan", arrow::struct_(schema_timespan)));
  } else if ("timepoint" == value.name()) {
    field = std::make_shared<arrow::Field>(
      arrow::Field("timepoint", arrow::struct_(schema_timepoint)));
  } else if ("string" == value.name()) {
    field = std::make_shared<arrow::Field>(
      arrow::Field("string", arrow::boolean()));
  } else if ("pattern" == value.name()) {
    field = std::make_shared<arrow::Field>(
      arrow::Field("pattern", arrow::boolean()));
  } else if ("subnet" == value.name()) {
    // size must set to 16
    std::shared_ptr<arrow::FixedSizeBinaryType> address_ptr;
    std::vector<std::shared_ptr<arrow::Field>> schema_subnet = {
      arrow::field("address", address_ptr),
      arrow::field("mask", arrow::int8()),
    };
    field = std::make_shared<arrow::Field>(
      arrow::Field("subnet", arrow::struct_(schema_subnet)));
  } else if ("address" == value.name()) {
    // size must set to 16 ??
    std::shared_ptr<arrow::FixedSizeBinaryType> address_ptr;
    field = std::make_shared<arrow::Field>(
      arrow::Field("address", address_ptr));
  } else if ("port" == value.name()) {
    std::vector<std::shared_ptr<arrow::Field>> schema_port = {
      arrow::field("port_type", arrow::int8()),
      arrow::field("mask", arrow::int16()),
    };
    field = std::make_shared<arrow::Field>(
      arrow::Field("port", arrow::struct_(schema_port)));
  } 
  //else if () {
  // Datasets {vector, set and table}
  //} 
  else {
    // NoneType ?
    field = std::make_shared<arrow::Field>("none", arrow::null());
  }
  return field;
}

// Transposes a vector of events from a row-wise into the columnar Arrow
// representation in the form of a record batch.
std::unique_ptr<arrow::RecordBatch> transpose(const std::vector<event>& xs) {
  auto batch_prt = std::make_unique<arrow::RecordBatch>();
  for (const auto& e : xs) {
    // wrong type is vast::data and not vast::type
    auto col = convert_vast_type_to_arrow_field(e.data());
  }
  return {};
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

} // namespace <anonymous>

namespace format {
namespace arrow {

writer::writer(const std::string& plasma_socket) {
  VAST_DEBUG(name(), "connects to plasma store at", plasma_socket);
  auto status = plasma_client_.Connect(plasma_socket, "",
                                       PLASMA_DEFAULT_RELEASE_DELAY, 0);
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
  std::cout << oid->hex() << std::endl;
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

expected<plasma::ObjectID>
writer::make_object(const void* data, size_t size) {
  auto oid = plasma::ObjectID::from_random();
  uint8_t* buffer;
  auto status = plasma_client_.Create(oid, static_cast<int64_t>(size),
                                      nullptr, 0, &buffer);
  if (!status.ok())
    return make_error(ec::format_error, "failed to create object",
                      status.ToString());
  std::memcpy(buffer, reinterpret_cast<const char*>(data), size);
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
