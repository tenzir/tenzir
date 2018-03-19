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
    for (auto& e : record_type::each(r)) {
      auto result = convert_to_arrow_field(e.trace.back()->type);
      schema_record.push_back(std::move(result));
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
expected<std::shared_ptr<arrow::RecordBatch>> transpose(const event& e) {
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  auto result = convert_to_arrow_field(e.type());
  schema_vector.push_back(std::move(result));
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::unique_ptr<arrow::RecordBatchBuilder> builder;
  auto status = arrow::RecordBatchBuilder::Make(
    schema, arrow::default_memory_pool(), &builder);
  std::shared_ptr<arrow::RecordBatch> batch;
  format::arrow::insert_visitor iv(*builder, 0);
  if (status.ok()) {
    auto status = visit(iv, e.type(), e.data());
    if (status.ok())
      status = builder->Flush(&batch);
  }
  if (!status.ok()) {
    return make_error(ec::format_error, "failed to flush batch",
                      status.ToString());
  }
  return batch;
}

// Writes a Record batch into an in-memory buffer.
expected<std::shared_ptr<arrow::Buffer>>
write_to_buffer(const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto pool = arrow::default_memory_pool();
  auto buffer = std::make_shared<arrow::PoolBuffer>(pool);
  auto sink = std::make_unique<arrow::io::BufferOutputStream>(buffer);
  std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  auto status = arrow::ipc::RecordBatchStreamWriter::Open(
    sink.get(), batch->schema(), &writer);
  if (!status.ok())
    return make_error(ec::format_error, "failed to open arrow stream writer",
                      status.ToString());
  status = writer->WriteRecordBatch(*batch, true);
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
using result_type = std::shared_ptr<::arrow::Field>;
result_type convert_visitor::operator()(const boolean_type&) {
  return ::arrow::field("bool", ::arrow::boolean());
}
result_type convert_visitor::operator()(const count_type&) {
  return ::arrow::field("count", ::arrow::uint64());
}
result_type convert_visitor::operator()(const integer_type&) {
  return ::arrow::field("integer", ::arrow::int64());
}
result_type convert_visitor::operator()(const real_type&) {
  return ::arrow::field("real", ::arrow::float64());
}
result_type convert_visitor::operator()(const string_type&) {
  return ::arrow::field("string", std::make_shared<::arrow::StringType>());
}
result_type convert_visitor::operator()(const pattern_type&) {
  return ::arrow::field("pattern", std::make_shared<::arrow::StringType>());
}
result_type convert_visitor::operator()(const address_type&) {
  return ::arrow::field("address",
                        std::make_shared<::arrow::FixedSizeBinaryType>(16));
}
result_type convert_visitor::operator()(const port_type&) {
  std::vector<result_type> schema_vector_port = {
    ::arrow::field("port_type", ::arrow::int8()),
    ::arrow::field("mask", ::arrow::int16()),
  };
  auto port_struct = std::make_shared<::arrow::StructType>(schema_vector_port);
  return ::arrow::field("port", port_struct);
}
result_type convert_visitor::operator()(const subnet_type&) {
  std::vector<result_type> schema_vector_subnet = {
    ::arrow::field("address",
                   std::make_shared<::arrow::FixedSizeBinaryType>(16)),
    ::arrow::field("mask", ::arrow::int8()),
  };
  auto subnet_struct =
    std::make_shared<::arrow::StructType>(schema_vector_subnet);
  return ::arrow::field("subnet", subnet_struct);
}
result_type convert_visitor::operator()(const timespan_type&) {
  return ::arrow::field("timespan", ::arrow::uint64());
}
result_type convert_visitor::operator()(const timestamp_type&) {
  return ::arrow::field("timestamp",
                        ::arrow::timestamp(::arrow::TimeUnit::NANO));
}
insert_visitor::insert_visitor(::arrow::ArrayBuilder& b) : builder(&b) {
  // nop
}
insert_visitor::insert_visitor(::arrow::RecordBatchBuilder& b) : rbuilder(&b) {
  // nop
}
insert_visitor::insert_visitor(::arrow::ArrayBuilder& b, u_int64_t c)
    : builder(&b), counter(c) {
  // nop
}
insert_visitor::insert_visitor(::arrow::RecordBatchBuilder& b, u_int64_t c)
    : rbuilder(&b), counter(c) {
  // nop
}
::arrow::Status insert_visitor::operator()(const type&, const data& d) {
  std::cout << typeid(d).name() << std::endl;
  std::cout << "default" << std::endl;
  return ::arrow::Status::OK();
}
::arrow::Status insert_visitor::operator()(const none_type&, const none&) {
  auto nbuilder = static_cast<::arrow::NullBuilder*>(builder);
  return nbuilder->AppendNull();
}
::arrow::Status insert_visitor::operator()(const count_type&, const count& d) {
  auto cbuilder = static_cast<::arrow::UInt64Builder*>(builder);
  return cbuilder->Append(d);
}
::arrow::Status insert_visitor::operator()(const count_type&, const none&) {
  auto cbuilder = static_cast<::arrow::UInt64Builder*>(builder);
  return cbuilder->AppendNull();
}
::arrow::Status insert_visitor::operator()(const integer_type&,
                                           const integer& d) {
  auto cbuilder = static_cast<::arrow::Int64Builder*>(builder);
  return cbuilder->Append(d);
}
::arrow::Status insert_visitor::operator()(const integer_type&, const none&) {
  auto cbuilder = static_cast<::arrow::Int64Builder*>(builder);
  return cbuilder->AppendNull();
}
::arrow::Status insert_visitor::operator()(const real_type&, const real& d) {
  auto cbuilder = static_cast<::arrow::FloatBuilder*>(builder);
  return cbuilder->Append(d);
}
::arrow::Status insert_visitor::operator()(const real_type&, const none&) {
  auto sbuilder = static_cast<::arrow::FloatBuilder*>(builder);
  return sbuilder->AppendNull();
}
::arrow::Status insert_visitor::operator()(const string_type&,
                                           const std::string& d) {
  auto sbuilder = static_cast<::arrow::StringBuilder*>(builder);
  return sbuilder->Append(d);
}
::arrow::Status insert_visitor::operator()(const string_type&, const none&) {
  auto sbuilder = static_cast<::arrow::StringBuilder*>(builder);
  return sbuilder->AppendNull();
}
::arrow::Status insert_visitor::operator()(const boolean_type&, const bool& d) {
  auto bbuilder = static_cast<::arrow::BooleanBuilder*>(builder);
  return bbuilder->Append(d);
}
::arrow::Status insert_visitor::operator()(const boolean_type&, const none&) {
  auto sbuilder = static_cast<::arrow::BooleanBuilder*>(builder);
  return sbuilder->AppendNull();
}
::arrow::Status insert_visitor::operator()(const timestamp_type&,
                                           const timestamp& d) {
  auto sbuilder = static_cast<::arrow::TimestampBuilder*>(builder);
  return sbuilder->Append(d.time_since_epoch().count());
}
::arrow::Status insert_visitor::operator()(const timespan_type&,
                                           const timespan& d) {
  auto sbuilder = static_cast<::arrow::UInt64Builder*>(builder);
  return sbuilder->Append(d.count());
}
::arrow::Status insert_visitor::operator()(const timespan_type&, const none&) {
  auto sbuilder = static_cast<::arrow::UInt64Builder*>(builder);
  return sbuilder->AppendNull();
}
::arrow::Status insert_visitor::operator()(const subnet_type&,
                                           const subnet& d) {
  auto sbuilder = static_cast<::arrow::StructBuilder*>(builder);
  auto abuilder =
    static_cast<::arrow::FixedSizeBinaryBuilder*>(sbuilder->field_builder(0));
  auto status = abuilder->Append(d.network().bytes_);
  if (!status.ok())
    return status;
  auto mbuilder =
    static_cast<::arrow::UInt8Builder*>(sbuilder->field_builder(1));
  status = mbuilder->Append(d.length());
  if (!status.ok())
    return status;
  return sbuilder->Append(true);
}
::arrow::Status insert_visitor::operator()(const address_type&,
                                           const address& d) {
  auto sbuilder = static_cast<::arrow::FixedSizeBinaryBuilder*>(builder);
  return sbuilder->Append(d.bytes_);
}
::arrow::Status insert_visitor::operator()(const address_type&, const none&) {
  auto sbuilder = static_cast<::arrow::FixedSizeBinaryBuilder*>(builder);
  return sbuilder->AppendNull();
}
::arrow::Status insert_visitor::operator()(const port_type&, const port& d) {

  auto sbuilder = static_cast<::arrow::StructBuilder*>(builder);
  auto tbuilder =
    static_cast<::arrow::UInt8Builder*>(sbuilder->field_builder(0));
  auto status = tbuilder->Append(d.type());
  if (!status.ok())
    return status;
  auto mbuilder =
    static_cast<::arrow::UInt16Builder*>(sbuilder->field_builder(1));
  status = mbuilder->Append(d.number());
  if (!status.ok())
    return status;
  return sbuilder->Append(true);
}
::arrow::Status insert_visitor::operator()(const vector_type& t,
                                           const std::vector<data>& d) {
  auto structBuilder = rbuilder->GetFieldAs<::arrow::StructBuilder>(0);
  u_int64_t offset = 0;
  for (; counter < d.size();) {
    auto b = structBuilder->field_builder(counter + offset);
    format::arrow::insert_visitor a(*b, counter);
    a.rbuilder = this->rbuilder;
    auto status = visit(a, t.value_type, d.at(counter));
    if (!status.ok()) {
      return status;
    }
    if (is<record_type>(t.value_type) && is<std::vector<data>>(d.at(counter))) {
      auto data_v = get<std::vector<data>>(d.at(counter));
      offset += data_v.size() - 1;
    }
    counter++;
  }
  return structBuilder->Append(true);
}
::arrow::Status insert_visitor::operator()(const vector_type&, const none&) {
  auto sbuilder = rbuilder->GetFieldAs<::arrow::StructBuilder>(0);
  return sbuilder->AppendNull();
}
::arrow::Status insert_visitor::operator()(const set_type& t, const set& d) {
  auto structBuilder = rbuilder->GetFieldAs<::arrow::StructBuilder>(0);
  u_int64_t offset = 0;
  // no test data with not emty set available
  for (auto e : d) {
    auto b = structBuilder->field_builder(counter + offset);
    format::arrow::insert_visitor a(*b, counter);
    a.rbuilder = this->rbuilder;
    auto status = visit(a, t.value_type, e);
    if (!status.ok()) {
      return status;
    }
    if (is<record_type>(t.value_type) && is<std::vector<data>>(e)) {
      auto data_v = get<std::vector<data>>(e);
      offset += data_v.size() - 1;
    }
    counter++;
  }
  return structBuilder->Append(true);
}
::arrow::Status insert_visitor::operator()(const set_type&, const none&) {
  auto structBuilder = rbuilder->GetFieldAs<::arrow::StructBuilder>(0);
  return structBuilder->AppendNull();
}
::arrow::Status insert_visitor::operator()(const record_type& t,
                                           const std::vector<data>& d) {
  auto structBuilder = rbuilder->GetFieldAs<::arrow::StructBuilder>(0);
  u_int64_t offset = 0;
  for (; counter < d.size();) {
    auto b = structBuilder->field_builder(counter + offset);
    format::arrow::insert_visitor a(*b, counter);
    a.rbuilder = this->rbuilder;
    auto status = visit(a, t.fields[counter].type, d.at(counter));
    if (!status.ok()) {
      return status;
    }
    if (is<record_type>(t.fields[counter].type)
        && is<std::vector<data>>(d.at(counter))) {
      auto data_v = get<std::vector<data>>(d.at(counter));
      offset += data_v.size() - 1;
    }
    counter++;
  }
  return structBuilder->Append(true);
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
  for (auto e : xs) {
    auto record_batch = transpose(e);
    if (!record_batch)
      return make_error(ec::format_error, "failed to transpose events");
    auto buf = write_to_buffer(*record_batch);
    if (!buf)
      return buf.error();
    VAST_ASSERT(*buf);
    auto oid = make_object((*buf)->data(), static_cast<size_t>((*buf)->size()));
    if (!oid)
      return oid.error();
  }
  return no_error;
} // namespace arrow

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
