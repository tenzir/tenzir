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

#include "vast/error.hpp"
#include "vast/logger.hpp"

namespace vast::format::arrow {

writer::writer(std::string plasma_socket)
  : plasma_socket_{std::move(plasma_socket)} {
  VAST_DEBUG(name(), "connects to plasma store at", plasma_socket);
  constexpr int num_retries = 100;
  auto status = plasma_client_.Connect(plasma_socket, "", 0, num_retries);
  connected_ = status.ok();
  if (!connected_)
    VAST_ERROR(name(), "failed to connect to plasma store");
}

writer::~writer() {
  if (!connected())
    return;
  auto status = plasma_client_.Disconnect();
  if (!status.ok())
    VAST_ERROR(name(), "failed to disconnect from plasma store");
}

caf::expected<void> writer::write(event const& /* e */) {
  if (!connected())
    return make_error(ec::format_error, "not connected to plasma store");
  // TODO: write event into record batch builders.
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
