//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/fbs/partition_synopsis.hpp>
#include <vast/fbs/synopsis.hpp>

#include "lsvast.hpp"
#include "util.hpp"

namespace lsvast {

static void print_partition_synopsis_legacy(
  const vast::fbs::partition_synopsis::LegacyPartitionSynopsis*
    partition_synopsis,
  indentation& indent, const options& options) {
  if (!partition_synopsis)
    return;
  auto const* id_range = partition_synopsis->id_range();
  if (id_range)
    fmt::print("{}id range: {} - {}\n", indent, id_range->begin(),
               id_range->end());
  auto const* import_time_range = partition_synopsis->import_time_range();
  if (import_time_range)
    fmt::print("{}import time range: {} - {}\n", indent,
               import_time_range->begin(), import_time_range->end());
  fmt::print("{}synopses:\n", indent);
  indented_scope scope{indent};
  for (auto const* synopsis : *partition_synopsis->synopses()) {
    print_synopsis(synopsis, indent, options);
  }
}

void print_partition_synopsis(const std::filesystem::path& path,
                              indentation& indent, const options& formatting) {
  auto synopsis = read_flatbuffer_file<vast::fbs::PartitionSynopsis>(path);
  if (!synopsis) {
    fmt::print("(error reading synopsis file)\n", path.string());
    return;
  }
  switch (synopsis->partition_synopsis_type()) {
    case vast::fbs::partition_synopsis::PartitionSynopsis::legacy:
      print_partition_synopsis_legacy(synopsis->partition_synopsis_as_legacy(),
                                      indent, formatting);
      break;
    default:
      fmt::print("(unknown partition version)\n");
  }
}

} // namespace lsvast
