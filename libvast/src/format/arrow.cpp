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

#include <arrow/buffer.h>
#include <plasma/common.h>

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/event.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/detail/assert.hpp"
#include "vast/error.hpp"
#include "vast/logger.hpp"
#include "vast/span.hpp"

namespace vast::format::arrow {

namespace {

plasma::ObjectID make_random_object_id() {
  // FIXME: properly generate a random number; eventually one for a record
  // batch.
  auto random_uuid = to_string(uuid::random());
  random_uuid.resize(plasma::kUniqueIDSize);
  return plasma::ObjectID::from_binary(random_uuid);
}

caf::error create_object(plasma::PlasmaClient& client, const plasma::ObjectID oid,
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

caf::expected<void> writer::write(event const& x) {
  if (!connected())
    return make_error(ec::format_error, "not connected to plasma store");
  // FIXME: For testing purposes, we store the string representation of each
  // event as separate object.
  auto str = to_string(x);
  auto oid = make_random_object_id();
  if (auto err = create_object(plasma_client_, oid, make_const_byte_span(str)))
    return err;
  std::cout << oid.hex() << std::endl; // FIXME: print as JSON
  return caf::no_error;
}

caf::expected<void> writer::flush() {
  // TODO: flush builders into record batch and ship to plasma
  return caf::no_error;
}

const char* writer::name() const {
  return "arrow-writer";
}

bool writer::connected() const {
  return connected_;
}

} // namespace vast::format::arrow
