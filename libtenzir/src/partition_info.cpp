//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/partition_info.hpp"

#include "tenzir/partition_synopsis.hpp"

namespace tenzir {

partition_info::partition_info(class uuid uuid, size_t events,
                               time max_import_time, type schema,
                               uint64_t version) noexcept
  : uuid{uuid},
    events{events},
    max_import_time{max_import_time},
    schema{std::move(schema)},
    version{version} {
  // nop
}

partition_info::partition_info(class uuid uuid,
                               const partition_synopsis& synopsis) noexcept
  : partition_info{uuid, synopsis.events, synopsis.max_import_time,
                   synopsis.schema, synopsis.version} {
  // nop
}

} // namespace tenzir
