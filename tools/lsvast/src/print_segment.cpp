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
#include <vast/table_slice.hpp>
#include <vast/uuid.hpp>

#include <iostream>

#include "lsvast.hpp"
#include "util.hpp"

namespace lsvast {

void print_segment_v0(const vast::fbs::segment::v0* segment,
                      indentation& indent, const options& options) {
  indented_scope _(indent);
  vast::uuid id;
  if (segment->uuid())
    if (auto error = unpack(*segment->uuid(), id))
      std::cerr << indent << to_string(error);
  std::cout << indent << "Segment\n";
  std::cout << indent << "uuid: " << to_string(id) << "\n";
  std::cout << indent << "events: " << segment->events() << "\n";
  if (options.format.verbosity >= output_verbosity::verbose) {
    std::cout << indent << "table_slices:\n";
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
      std::cout << indent << slice.layout().name << ": " << slice.rows()
                << " rows";
      if (options.format.print_bytesizes) {
        auto size = flat_slice->data()->size();
        std::cout << " (" << print_bytesize(size, options.format) << ")";
        total_size += size;
      }
      std::cout << '\n';
    }
    if (options.format.print_bytesizes)
      std::cout << indent
                << "total: " << print_bytesize(total_size, options.format)
                << "\n";
  }
}

void print_segment(const std::filesystem::path& path, indentation& indent,
                   const options& formatting) {
  auto segment = read_flatbuffer_file<vast::fbs::Segment>(path);
  if (!segment) {
    std::cout << "(error reading segment file " << path.string() << ")\n";
  }
  switch (segment->segment_type()) {
    case vast::fbs::segment::Segment::v0:
      print_segment_v0(segment->segment_as_v0(), indent, formatting);
      break;
    default:
      std::cout << "(unknown partition version)\n";
  }
}

} // namespace lsvast
