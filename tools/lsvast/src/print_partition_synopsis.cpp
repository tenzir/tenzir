//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/chunk.hpp>
#include <vast/fbs/partition_synopsis.hpp>
#include <vast/fbs/synopsis.hpp>
#include <vast/type.hpp>

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

static void print_partition_synopsis_v1(
  const vast::fbs::partition_synopsis::PartitionSynopsisV1* partition_synopsis,
  indentation& indent, const options& opts) {
  if (!partition_synopsis)
    return;
  auto version = partition_synopsis->version();
  fmt::print("{}version: {}\n", indent, version);
  auto const* schema = partition_synopsis->schema();
  if (schema) {
    auto type = vast::type{vast::chunk::copy(*schema)};
    fmt::print("{}schema: {}\n", indent, type);
    if (opts.format.verbosity >= output_verbosity::verbose) {
      // We visit the type to strip away the names and get to the
      // underlying record type, which prints all fields.
      indented_scope scope{indent};
      auto f = [&indent](auto&& type) {
        fmt::print("{}{}\n", indent, type);
      };
      caf::visit(f, type);
    }
  }
  auto const* id_range = partition_synopsis->id_range();
  if (id_range)
    fmt::print("{}id range: {} - {}\n", indent, id_range->begin(),
               id_range->end());
  auto const* import_time_range = partition_synopsis->import_time_range();
  if (import_time_range)
    fmt::print("{}import time range: {} - {}\n", indent,
               import_time_range->begin(), import_time_range->end());
  fmt::print("{}{} field sketches\n", indent,
             partition_synopsis->field_sketches()->size());
  if (opts.format.verbosity >= output_verbosity::verbose) {
    for (auto const* field_sketch : *partition_synopsis->field_sketches()) {
      indented_scope scope{indent};
      fmt::print("{}{}\n", indent,
                 field_sketch->field()->name()->string_view());
    }
  }
  fmt::print("{}{} type sketches\n", indent,
             partition_synopsis->type_sketches()->size());
  if (opts.format.verbosity >= output_verbosity::verbose) {
    for (auto const* type_sketch : *partition_synopsis->type_sketches()) {
      indented_scope scope{indent};
      fmt::print("{}{} ", indent,
                 vast::type{vast::chunk::copy(*type_sketch->type())});
      // TODO: Create a `print_sketch()` function
      auto const* sketch = type_sketch->sketch_nested_root();
      switch (sketch->sketch_type()) {
        case vast::fbs::sketch::Sketch::min_max_u64: {
          auto const* sketch_u64 = sketch->sketch_as_min_max_u64();
          fmt::print("{} - {}\n", indent, sketch_u64->min(), sketch_u64->max());
          break;
        }
        default:
          fmt::print("(printing not yet implemented)\n", indent);
      }
    }
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
    case vast::fbs::partition_synopsis::PartitionSynopsis::v1:
      print_partition_synopsis_v1(synopsis->partition_synopsis_as_v1(), indent,
                                  formatting);
      break;
    default:
      fmt::print("(unknown partition version)\n");
  }
}

} // namespace lsvast
