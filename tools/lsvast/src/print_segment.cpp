//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/chunk.hpp>
#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/uuid.hpp>
#include <vast/fbs/segment.hpp>
#include <vast/format/json.hpp>
#include <vast/table_slice.hpp>
#include <vast/uuid.hpp>

#include <iostream>

#include "lsvast.hpp"
#include "util.hpp"

namespace lsvast {

template <typename SegmentFlatbuffer>
static void print_segment_contents(const SegmentFlatbuffer* segment,
                                   indentation&, const options&) {
  if (!segment || !segment->slices()) {
    fmt::print("(null segment)\n");
    return;
  }
  auto settings = caf::settings{};
  auto writer = vast::format::json::writer{
    std::make_unique<std::stringstream>(), settings};
  for (const auto* slice_wrapper : *segment->slices()) {
    if (!slice_wrapper || !slice_wrapper->data()) {
      fmt::print("(null contents)\n");
      return;
    }
    const auto* slice_data = slice_wrapper->data();
    // Use an empty deleter to create a non-owning chunk.
    auto chunk = vast::chunk::make(slice_data->data(), slice_data->size(),
                                   []() noexcept { /* nop */ });
    auto slice
      = vast::table_slice{std::move(chunk), vast::table_slice::verify::no};
    writer.write(slice);
  }
  writer.flush();
  auto& out = static_cast<std::stringstream&>(writer.out());
  fmt::print("{}\n", out.str());
}

void print_segment_v0(const vast::fbs::segment::v0* segment,
                      indentation& indent, const options& options) {
  indented_scope _(indent);
  if (options.segment.print_contents) {
    print_segment_contents(segment, indent, options);
    return;
  }
  vast::uuid id = {};
  if (segment->uuid())
    if (auto error = unpack(*segment->uuid(), id))
      fmt::print(stderr, "{}{}", indent, to_string(error));
  fmt::print("{}Segment\n", indent);
  fmt::print("{}uuid: {}\n", indent, to_string(id));
  fmt::print("{}events: {}\n", indent, segment->events());
  if (options.format.verbosity >= output_verbosity::verbose) {
    fmt::print("{}table_slices:\n", indent);
    indented_scope _(indent);
    size_t total_size = 0;
    for (auto flat_slice : *segment->slices()) {
      // We're intentionally creating a chunk without a deleter here, i.e., a
      // chunk that does not actually take ownership of its data. This is
      // necessary because we're accessing `vast::fbs::Segment` directly instead
      // of going through `vast::segment`, which has the necessary framing to
      // give out table slices that share the segment's lifetime.
      auto chunk = vast::chunk::make(flat_slice->data()->data(),
                                     flat_slice->data()->size(), {});
      auto slice
        = vast::table_slice(std::move(chunk), vast::table_slice::verify::no);
      const auto& layout = slice.layout();
      fmt::print("{}{}: {} rows", indent, layout.name(), slice.rows());
      if (options.format.print_bytesizes) {
        auto size = flat_slice->data()->size();
        fmt::print(" ({})", print_bytesize(size, options.format));
        total_size += size;
      }
      fmt::print("\n");
    }
    if (options.format.print_bytesizes)
      fmt::print("{}total: {}\n", indent,
                 print_bytesize(total_size, options.format));
  }
}

void print_segment(const std::filesystem::path& path, indentation& indent,
                   const options& formatting) {
  auto segment = read_flatbuffer_file<vast::fbs::Segment>(path);
  if (!segment) {
    fmt::print(stderr, "(error reading segment file {})\n", path.string());
    return;
  }
  switch (segment->segment_type()) {
    case vast::fbs::segment::Segment::v0:
      print_segment_v0(segment->segment_as_v0(), indent, formatting);
      break;
    default:
      fmt::print("(unknown partition version)\n");
  }
}

} // namespace lsvast
