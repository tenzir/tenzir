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
#include <vast/fbs/flatbuffer_container.hpp>
#include <vast/fbs/segment.hpp>
#include <vast/fbs/segmented_file.hpp>
#include <vast/format/json.hpp>
#include <vast/segment.hpp>
#include <vast/table_slice.hpp>
#include <vast/uuid.hpp>

#include <iostream>

#include "lsvast.hpp"
#include "util.hpp"

namespace lsvast {

static void
print_segment_contents(indentation&, const options&, vast::chunk_ptr chunk) {
  auto segment = vast::segment::make(std::move(chunk));
  if (!segment) {
    fmt::print("(invalid segment)\n");
    return;
  }
  auto settings = caf::settings{};
  auto writer = vast::format::json::writer{
    std::make_unique<std::stringstream>(), settings};
  for (const auto& slice : *segment)
    if (auto err = writer.write(slice))
      fmt::print(stderr, "Err during segment slice write {}", err);
  writer.flush();
  auto& out = static_cast<std::stringstream&>(writer.out());
  fmt::print("{}\n", out.str());
}

static void
print_segment_v0(const vast::fbs::segment::v0* segment, indentation& indent,
                 const options& options, vast::chunk_ptr chunk) {
  indented_scope _(indent);
  if (options.segment.print_contents) {
    print_segment_contents(indent, options, chunk);
    return;
  }
  vast::uuid id = {};
  if (segment->uuid())
    if (auto error = unpack(*segment->uuid(), id))
      fmt::print(stderr, "{}{}", indent, to_string(error));
  fmt::print("{}Segment\n", indent);
  fmt::print("{}uuid: {}\n", indent, to_string(id));
  fmt::print("{}events: {}\n", indent, segment->events());
  fmt::print("{}overflow_slices: {}\n", indent, segment->overflow_slices());
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
  // TODO: Make use of the `vast::segment` class to print segments.
  const vast::fbs::Segment* segment = nullptr;
  auto maybe_chunk = vast::chunk::mmap(path);
  if (!maybe_chunk) {
    fmt::print("(failed to open file: {})", maybe_chunk.error());
    return;
  }
  auto& chunk = *maybe_chunk;
  if (flatbuffers::BufferHasIdentifier(chunk->data(),
                                       vast::fbs::SegmentIdentifier())) {
    segment = vast::fbs::GetSegment(chunk->data());
  } else if (flatbuffers::BufferHasIdentifier(
               chunk->data(), vast::fbs::SegmentedFileHeaderIdentifier())) {
    if (formatting.segment.print_header) {
      auto const* header = vast::fbs::GetSegmentedFileHeader(chunk->data());
      print_segmented_file_header(header, indent, formatting);
    }
    auto container = vast::fbs::flatbuffer_container{chunk};
    segment = container.as_flatbuffer<vast::fbs::Segment>(0);
  } else {
    fmt::print("(unknown identifier)\n");
    return;
  }
  switch (segment->segment_type()) {
    case vast::fbs::segment::Segment::v0:
      print_segment_v0(segment->segment_as_v0(), indent, formatting, chunk);
      break;
    default:
      fmt::print("(unknown segment version)\n");
  }
}

} // namespace lsvast
