#include "arrow/api.h"
#include "arrow/builder.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/type.h"
#include "plasma/common.h"

#include "vast/error.hpp"
#include "vast/event.hpp"
#include "vast/format/arrow.hpp"
#include "vast/logger.hpp"
#include "vast/type.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"

namespace vast {

namespace {

std::shared_ptr<arrow::Field> convert_to_arrow_field(const type& value) {
  std::shared_ptr<arrow::Field> field;
  if (is<record_type>(value)) {
    auto r = get<record_type>(value);
    std::vector<std::shared_ptr<arrow::Field>> schema_record;
    u_int32_t i = 0;
    for (auto& e : record_type::each(r)) {
      auto result = convert_to_arrow_field(e.trace.back()->type);
      schema_record.push_back(std::move(result));
      i++;
    }
    field = arrow::field("record", arrow::struct_(schema_record));
  } else {
    format::arrow::convert_visitor f;
    field = visit(f, value);
  }
  return field;
}

// Transposes a vector of events from a row-wise into the columnar Arrow
// representation in the form of a record batch.
std::shared_ptr<arrow::RecordBatch> transpose(const std::vector<event>& xs) {
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  for (const auto& e : xs) {
    auto result = convert_to_arrow_field(e.type());
    schema_vector.push_back(std::move(result));
    if (is<record_type>(e.type())) {
      break;
    }
  }
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::unique_ptr<arrow::RecordBatchBuilder> builder;
  auto status = arrow::RecordBatchBuilder::Make(
    schema, arrow::default_memory_pool(), &builder);
  std::shared_ptr<arrow::RecordBatch> batch;
  format::arrow::insert_visitor iv(*builder);
  if (status.ok()) {
    for (const auto e : xs) {
      visit(iv, e.type(), e.data());
      auto status = builder->Flush(&batch);
      if (!status.ok())
        return batch;
    }
  }
  if (status.ok()) {
    std::cout << schema->ToString() << std::endl;
  }
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
insert_visitor::insert_visitor(::arrow::RecordBatchBuilder& b) : builder(&b) {
  // nop
}

void insert_visitor::operator()(const record_type t,
                                const std::vector<data> d) {
  auto structBuilder = builder->GetFieldAs<::arrow::StructBuilder>(0);
  auto stringBuilder =
    static_cast<::arrow::StringBuilder*>(structBuilder->field_builder(1));
  stringBuilder->Append(get<std::string>(d.at(1)));
  stringBuilder =
    static_cast<::arrow::StringBuilder*>(structBuilder->field_builder(3));
  stringBuilder->Append(get<std::string>(d.at(3)));
  auto nullBuilder =
    static_cast<::arrow::NullBuilder*>(structBuilder->field_builder(4));
  nullBuilder->AppendNull();
  stringBuilder =
    static_cast<::arrow::StringBuilder*>(structBuilder->field_builder(8));
  if (is<std::string>(d.at(8))) {
    //stringBuilder->Append(std::string(get<std::string>(d.at(8))));
  } else {
    stringBuilder->AppendNull();
  }
  for (int  e = 0; e < d.size(); e++) {
    std::cout << to_string(d.at(e)) << " " << structBuilder->field_builder(e)->type()->name() << std::endl;

  }

  std::cout << d.size() << std::endl;
}

void insert_visitor::operator()(const string_type t, const data d) {
  // auto stringBuilder = builder->GetFieldAs<::arrow::StringBuilder>(0);
  // auto status = stringBuilder->Append(d);
  // std::cout << status.message() << "sdsd" << std::endl;
}

writer::writer(const std::string& plasma_socket) {
  VAST_DEBUG(name(), "connects to plasma store at", plasma_socket);
  auto status =
    plasma_client_.Connect(plasma_socket, "", PLASMA_DEFAULT_RELEASE_DELAY);
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
  auto status =
    plasma_client_.Create(oid, static_cast<int64_t>(size), nullptr, 0, &buffer);
  if (!status.ok())
    return make_error(ec::format_error, "failed to create object",
                      status.ToString());
  std::memcpy(buffer->mutable_data(), data, size);
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
