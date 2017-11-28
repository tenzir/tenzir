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

#include "vast/format/arrow.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <plasma/common.h>


#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/span.hpp"
#include "vast/view.hpp"

namespace vast::format::arrow {

namespace {

// TODO: implement this function.
std::shared_ptr<::arrow::DataType> make_data_type(const type& t) {
  auto factory = detail::overload(
    [&](const boolean_type&) {
      return ::arrow::boolean();
    },
    [&](const auto&) -> std::shared_ptr<::arrow::DataType> {
      return nullptr;
    }
  );
  return caf::visit(factory, t);
}

std::shared_ptr<::arrow::Field> make_field(const record_field& field) {
  return ::arrow::field(field.name, make_data_type(field.type));
}

std::shared_ptr<::arrow::Schema> make_schema(const record_type& layout) {
  std::vector<std::shared_ptr<::arrow::Field>> fields;
  fields.reserve(layout.fields.size());
  for (auto& field : layout.fields)
    fields.push_back(make_field(field));
  return ::arrow::schema(std::move(fields));
}

caf::error append(const type& t, data_view v, ::arrow::ArrayBuilder* builder) {
  auto f = detail::overload(
    [&](const boolean_type&) -> caf::error {
      auto ptr = static_cast<::arrow::BooleanBuilder*>(builder);
      auto status = caf::visit(detail::overload(
        [&](caf::none_t) { return ptr->AppendNull(); },
        [&](boolean x) { return ptr->Append(x); },
        [&](auto&&) { return ::arrow::Status::UnknownError("invalid data"); }
      ), v);
      if (!status.ok())
        return make_error(ec::format_error, status.ToString());
      return caf::none;
    },
    // TODO: implement overloads for all types. Ideally this should happen not
    // by duplication of the above lambda, but instead via a single lambda that
    // takes a meta function to find the corresponding builder type for a given
    // VAST type.
    [](const auto&) {
      return make_error(ec::format_error, "not yet implemented");
    }
  );
  return caf::visit(f, t);
}

plasma::ObjectID make_random_object_id() {
  // TODO: properly generate a sequence of random 20 bytes
  auto random_uuid = to_string(uuid::random());
  random_uuid.resize(plasma::kUniqueIDSize);
  return plasma::ObjectID::from_binary(random_uuid);
}

caf::error make_object(plasma::PlasmaClient& client, const plasma::ObjectID oid,
                       span<const byte> bytes) {
  auto size = static_cast<int64_t>(bytes.size());
  std::shared_ptr<::arrow::Buffer> buffer;
  auto status = client.Create(oid, size, nullptr, 0, &buffer);
  if (!status.ok())
    return make_error(ec::format_error, "failed to create plasma object",
                      status.ToString());
  VAST_ASSERT(buffer != nullptr);
  VAST_ASSERT(buffer->size() == bytes.size());
  std::memcpy(buffer->mutable_data(), bytes.data(), bytes.size());
  status = client.Seal(oid);
  if (!status.ok())
    return make_error(ec::format_error, "failed to seal plasma object",
                      status.ToString());
  return caf::none;
}

} // namespace <anonymous>

writer::writer(std::string plasma_socket)
  : plasma_socket_{std::move(plasma_socket)} {
  VAST_DEBUG(name(), "connects to plasma store at", plasma_socket_);
  constexpr int num_retries = 100;
  auto status = plasma_client_.Connect(plasma_socket_, "", 0, num_retries);
  connected_ = status.ok();
  if (!connected())
    VAST_ERROR(name(), "failed to connect to plasma store", status.ToString());
}

writer::~writer() {
  if (!connected())
    return;
  auto status = plasma_client_.Disconnect();
  if (!status.ok())
    VAST_ERROR(name(), "failed to disconnect from plasma store");
}

caf::expected<void> writer::write(const event& x) {
  auto& layout = caf::get<record_type>(x.type());
  auto i = builders_.find(layout);
  if (i == builders_.end()) {
    std::vector<std::unique_ptr<::arrow::ArrayBuilder>> builders;
    // TODO: construct builders
    i = builders_.emplace(layout, std::move(builders)).first;
  }
  auto& builders = i->second;
  VAST_ASSERT(layout.fields.size() == builders.size());
  for (size_t i = 0; i < builders.size(); ++i) {
    auto& field_type = layout.fields[i].type;
    auto builder = builders[i].get();
    if (auto error = append(field_type, make_view(x.data()), builder))
      return error;
  }
  return caf::no_error;
}

std::shared_ptr<::arrow::Buffer> make_buffer(const ::arrow::RecordBatch& xs) {
  CAF_IGNORE_UNUSED(xs);
  std::shared_ptr<::arrow::Buffer> result;
  //auto result = std::make_shared<arrow::PoolBuffer>(pool);
  //auto sink = std::make_unique<arrow::io::BufferOutputStream>(result);
  //std::shared_ptr<arrow::ipc::RecordBatchWriter> writer;
  //auto status = arrow::ipc::RecordBatchStreamWriter::Open(
  //  sink.get(), batch.schema(), &writer);
  //if (!status.ok())
  //  return make_error(ec::format_error, "failed to open arrow stream writer",
  //                    status.ToString());
  //status = writer->WriteRecordBatch(batch);
  //if (!status.ok())
  //  return make_error(ec::format_error, "failed to write batch",
  //                    status.ToString());
  //status = writer->Close();
  //if (!status.ok())
  //  return make_error(ec::format_error, "failed to close arrow stream writer",
  //                    status.ToString());
  //status = sink->Close();
  //if (!status.ok())
  //  return make_error(ec::format_error, "failed to close arrow stream writer",
  //                    status.ToString());
  return result;
};

caf::expected<void> writer::flush() {
  if (!connected())
    return make_error(ec::format_error, "not connected to plasma store");
  // Go through all builders and create record batches.
  for (auto& kvp : builders_) {
    auto& [layout, builders] = kvp;
    // FIXME: Construct a record batch from the current state of builders.
    auto schema = make_schema(layout);
    std::vector<std::shared_ptr<::arrow::Array>> columns;
    auto num_rows = 0; // FIXME
    auto record_batch = ::arrow::RecordBatch::Make(schema, num_rows,
                                                   std::move(columns));
    VAST_ASSERT(record_batch != nullptr);
    // Create buffer from record batch.
    auto buffer = make_buffer(*record_batch);
    auto bytes = make_const_byte_span(buffer->data(), buffer->size());
    // Create Plasma Object ID.
    auto oid = make_random_object_id();
    if (auto err = make_object(plasma_client_, oid, bytes))
      return err;
    // TODO: print object ID as JSON
    std::cout << oid.hex() << std::endl;
  }
  return caf::no_error;
}

const char* writer::name() const {
  return "arrow-writer";
}

bool writer::connected() const {
  return connected_;
}

} // namespace vast::format::arrow
