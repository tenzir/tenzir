/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

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

// Helper method to call the convert visitor
std::shared_ptr<arrow::Field> convert_to_arrow_field(const type& value) {
  format::arrow::convert_visitor f;
  return visit(f, value);
}

// Transposes a vector of events from a row-wise into the columnar Arrow
// representation in the form of a record batch.
expected<std::shared_ptr<arrow::RecordBatch>> transpose(const event& e) {
  std::vector<std::shared_ptr<arrow::Field>> schema_vector;
  if (is<record_type>(e.type())) {
    auto r = get<record_type>(e.type());
    for (auto& e : record_type::each(r)) {
      schema_vector.push_back(convert_to_arrow_field(e.trace.back()->type));
    }
  } else {
    schema_vector.push_back(convert_to_arrow_field(e.type()));
  }
  auto schema = std::make_shared<arrow::Schema>(schema_vector);
  std::unique_ptr<arrow::RecordBatchBuilder> rbuilder;
  auto status = arrow::RecordBatchBuilder::Make(
    schema, arrow::default_memory_pool(), &rbuilder);
  std::shared_ptr<arrow::RecordBatch> batch;
  std::shared_ptr<::arrow::RecordBatchBuilder> srbuilder(
    std::move(rbuilder.release()));
  format::arrow::insert_visitor iv{srbuilder};
  if (status.ok()) {
    status = visit(iv, e.type(), e.data());
    if (status.ok())
      status = srbuilder->Flush(&batch);
  }
  if (!status.ok())
    return make_error(ec::format_error, "failed to flush batch",
                      status.ToString());
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

::arrow::Status append_port(const port& d, ::arrow::StructBuilder* b) {
  auto tbuilder = static_cast<::arrow::UInt8Builder*>(b->field_builder(0));
  auto status = tbuilder->Append(d.type());
  if (!status.ok())
    return status;
  auto mbuilder = static_cast<::arrow::UInt16Builder*>(b->field_builder(1));
  status = mbuilder->Append(d.number());
  if (!status.ok())
    return status;
  return b->Append(true);
}

::arrow::Status append_subnet(const subnet& d, ::arrow::StructBuilder* b) {
  auto abuilder =
    static_cast<::arrow::FixedSizeBinaryBuilder*>(b->field_builder(0));
  auto status = abuilder->Append(d.network().bytes_);
  if (!status.ok())
    return status;
  auto mbuilder = static_cast<::arrow::UInt8Builder*>(b->field_builder(1));
  status = mbuilder->Append(d.length());
  if (!status.ok())
    return status;
  return b->Append(true);
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

result_type convert_visitor::operator()(const vector_type& t) {
  auto type = convert_to_arrow_field(t.value_type);
  return ::arrow::field("vector", ::arrow::list(type->type()));
}

result_type convert_visitor::operator()(const set_type& t) {
  auto type = convert_to_arrow_field(t.value_type);
  return ::arrow::field("set", ::arrow::list(type->type()));
}

insert_visitor::insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b)
    : rbuilder(b) {
  // nop
}

/*insert_visitor::insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b,
                               u_int64_t c, u_int64_t cbuilder)
    : rbuilder(b), counter(c), cbuilder(cbuilder) {
  // nop
}
*/

insert_visitor::insert_visitor(std::shared_ptr<::arrow::RecordBatchBuilder>& b,
                               u_int64_t c)
    : rbuilder(b), cbuilder(c) {
  // nop
}

::arrow::Status insert_visitor::operator()(const type&, const data&) {
  return ::arrow::Status::OK();
}

::arrow::Status insert_visitor::operator()(const none_type&, none) {
  auto builder = rbuilder->GetFieldAs<::arrow::NullBuilder>(cbuilder);
  return builder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const count_type&, count d) {
  auto builder = rbuilder->GetFieldAs<::arrow::UInt64Builder>(cbuilder);
  return builder->Append(d);
}

::arrow::Status insert_visitor::operator()(const count_type&, none) {
  auto builder = rbuilder->GetFieldAs<::arrow::UInt64Builder>(cbuilder);
  return builder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const integer_type&, integer d) {
  auto builder = rbuilder->GetFieldAs<::arrow::UInt64Builder>(cbuilder);
  return builder->Append(d);
}

::arrow::Status insert_visitor::operator()(const integer_type&, none) {
  auto builder = rbuilder->GetFieldAs<::arrow::UInt64Builder>(cbuilder);
  return builder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const real_type&, real d) {
  auto builder = rbuilder->GetFieldAs<::arrow::FloatBuilder>(cbuilder);
  return builder->Append(d);
}

::arrow::Status insert_visitor::operator()(const real_type&, none) {
  auto builder = rbuilder->GetFieldAs<::arrow::FloatBuilder>(cbuilder);
  return builder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const string_type&, std::string d) {
  auto builder = rbuilder->GetFieldAs<::arrow::StringBuilder>(cbuilder);
  return builder->Append(d);
}

::arrow::Status insert_visitor::operator()(const string_type&, none) {
  auto builder = rbuilder->GetFieldAs<::arrow::StringBuilder>(cbuilder);
  return builder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const boolean_type&, bool d) {
  auto builder = rbuilder->GetFieldAs<::arrow::BooleanBuilder>(cbuilder);
  return builder->Append(d);
}

::arrow::Status insert_visitor::operator()(const boolean_type&, none) {
  auto builder = rbuilder->GetFieldAs<::arrow::BooleanBuilder>(cbuilder);
  return builder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const timestamp_type&,
                                           const timestamp& d) {
  auto builder = rbuilder->GetFieldAs<::arrow::TimestampBuilder>(cbuilder);
  return builder->Append(d.time_since_epoch().count());
}

::arrow::Status insert_visitor::operator()(const timespan_type&,
                                           const timespan& d) {
  auto builder = rbuilder->GetFieldAs<::arrow::UInt64Builder>(cbuilder);
  return builder->Append(d.count());
}

::arrow::Status insert_visitor::operator()(const timespan_type&, none) {
  auto builder = rbuilder->GetFieldAs<::arrow::UInt64Builder>(cbuilder);
  return builder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const subnet_type&,
                                           const subnet& d) {
  auto builder = rbuilder->GetFieldAs<::arrow::StructBuilder>(cbuilder);
  return append_subnet(d, builder);
}

::arrow::Status insert_visitor::operator()(const address_type&,
                                           const address& d) {
  auto builder =
    rbuilder->GetFieldAs<::arrow::FixedSizeBinaryBuilder>(cbuilder);
  return builder->Append(d.bytes_);
}

::arrow::Status insert_visitor::operator()(const address_type&, none) {
  auto builder =
    rbuilder->GetFieldAs<::arrow::FixedSizeBinaryBuilder>(cbuilder);
  return builder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const port_type&, const port& d) {
  auto builder = rbuilder->GetFieldAs<::arrow::StructBuilder>(cbuilder);
  return append_port(d, builder);
}

::arrow::Status insert_visitor::operator()(const port_type&, none) {
  auto sbuilder = rbuilder->GetFieldAs<::arrow::StructBuilder>(cbuilder);
  return sbuilder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const vector_type&, none) {
  auto sbuilder = rbuilder->GetFieldAs<::arrow::ListBuilder>(cbuilder);
  return sbuilder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const set_type&, none) {
  auto structBuilder = rbuilder->GetFieldAs<::arrow::ListBuilder>(cbuilder);
  return structBuilder->AppendNull();
}

::arrow::Status insert_visitor::operator()(const record_type& t,
                                           const std::vector<data>& d) {
  for (u_int64_t counter = 0; counter < d.size();) {
    format::arrow::insert_visitor a{this->rbuilder, counter + cbuilder};
    if (is<record_type>(t.fields[counter].type)
        && is<std::vector<data>>(d.at(counter))) {
      auto data_v = get<std::vector<data>>(d.at(counter));
      cbuilder += data_v.size() - 1;
    }
    auto status = visit(a, t.fields[counter].type, d.at(counter));
    if (!status.ok()) {
      return status;
    }
    counter++;
  }
  return ::arrow::Status::OK();
}

insert_visitor_helper::insert_visitor_helper(::arrow::ArrayBuilder* b)
    : builder(b) {
  // nop
}

::arrow::Status insert_visitor_helper::operator()(const boolean_type&,
                                                  boolean d) {
  auto cbuilder = static_cast<::arrow::BooleanBuilder*>(builder);
  return cbuilder->Append(d);
}

::arrow::Status insert_visitor_helper::operator()(const count_type&, count d) {
  auto cbuilder = static_cast<::arrow::UInt64Builder*>(builder);
  return cbuilder->Append(d);
}

::arrow::Status insert_visitor_helper::operator()(const integer_type&, int d) {
  auto cbuilder = static_cast<::arrow::Int64Builder*>(builder);
  return cbuilder->Append(d);
}

::arrow::Status insert_visitor_helper::operator()(const real_type&, real d) {
  auto cbuilder = static_cast<::arrow::FloatBuilder*>(builder);
  return cbuilder->Append(d);
}

::arrow::Status insert_visitor_helper::operator()(const string_type&,
                                                  std::string d) {
  auto cbuilder = static_cast<::arrow::StringBuilder*>(builder);
  return cbuilder->Append(d);
}
::arrow::Status insert_visitor_helper::operator()(const pattern_type&,
                                                  const pattern& d) {
  auto cbuilder = static_cast<::arrow::StringBuilder*>(builder);
  return cbuilder->Append(d.string());
}

::arrow::Status insert_visitor_helper::operator()(const address_type&,
                                                  const address& d) {
  auto cbuilder = static_cast<::arrow::FixedSizeBinaryBuilder*>(builder);
  return cbuilder->Append(d.bytes_);
}

::arrow::Status insert_visitor_helper::operator()(const port_type&,
                                                  const port& d) {
  auto b = static_cast<::arrow::StructBuilder*>(builder);
  return append_port(d, b);
}

::arrow::Status insert_visitor_helper::operator()(const subnet_type&,
                                                  const subnet& d) {
  auto b = static_cast<::arrow::StructBuilder*>(builder);
  return append_subnet(d, b);
}

::arrow::Status insert_visitor_helper::operator()(const timespan_type&,
                                                  const timespan& d) {
  auto cbuilder = static_cast<::arrow::UInt64Builder*>(builder);
  return cbuilder->Append(d.count());
}

::arrow::Status insert_visitor_helper::operator()(const timestamp_type&,
                                                  const timestamp& d) {
  auto cbuilder = static_cast<::arrow::TimestampBuilder*>(builder);
  return cbuilder->Append(d.time_since_epoch().count());
}

::arrow::Status insert_visitor_helper::operator()(const none_type&, none) {
  auto cbuilder = static_cast<::arrow::NullBuilder*>(builder);
  return cbuilder->AppendNull();
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
  // TODO: write OIDs into a local buffer or something.
  std::vector<plasma::ObjectID> oids;
  return write(xs, oids);
}

expected<void> writer::write(const std::vector<event>& xs,
                             std::vector<plasma::ObjectID>& oids) {
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
    oids.push_back(*oid);
  }
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
