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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/array/builder_base.h>
#include <plasma/client.h>

#include "vast/format/writer.hpp"
#include "vast/type.hpp"

namespace vast::format::arrow {

/// Converts events into Arrow Record batches and writes them into a Plasma
/// store.
class writer : format::writer {
public:
  writer() = default;

  /// Constructs an Arrow writer that connects to a (local) plasma store.
  /// @param plasma_socket The path to the local Plasma listening socket.
  explicit writer(std::string plasma_socket);

  ~writer();

  caf::expected<void> write(const event& e) override;

  caf::expected<void> flush() override;

  const char* name() const override;

  /// Checks whether the writer is connected to the Plasma store.
  /// @returns `true` if the connection to the Plasma store is alive.
  bool connected() const;

private:
  using array_builder_ptr = std::unique_ptr<::arrow::ArrayBuilder>;

  plasma::PlasmaClient plasma_client_;
  std::string plasma_socket_;
  bool connected_ = false;
  std::unordered_map<record_type, std::vector<array_builder_ptr>> builders_;
};

} // namespace vast::format::arrow
