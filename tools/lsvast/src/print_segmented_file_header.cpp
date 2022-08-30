//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/fbs/segmented_file.hpp>

#include "lsvast.hpp"
#include "util.hpp"

namespace lsvast {

void print_segmented_file_header(const vast::fbs::SegmentedFileHeader* header,
                                 indentation& indent, const options&) {
  if (header->header_type()
      != vast::fbs::segmented_file::SegmentedFileHeader::v0) {
    fmt::print("{}unknown type: {}\n", indent,
               static_cast<uint8_t>(header->header_type()));
    return;
  }
  auto const* header_v0 = header->header_as_v0();
  auto identifier
    = std::string_view{reinterpret_cast<const char*>(
                         header_v0->inner_identifier()->data()->data()),
                       4};
  fmt::print("{}identifier: {}\n", indent, identifier);
  fmt::print("{}segments:\n", indent, identifier);
  indented_scope scope{indent};
  for (auto const* x : *header_v0->file_segments()) {
    fmt::print("{} {} to {} (size {})\n", indent, x->offset(),
               x->offset() + x->size(), x->size());
  }
}

} // namespace lsvast
