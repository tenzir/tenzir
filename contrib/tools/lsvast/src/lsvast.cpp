//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "lsvast.hpp"

#include <vast/command.hpp>
#include <vast/configuration.hpp>
#include <vast/error.hpp>
#include <vast/fbs/index.hpp>
#include <vast/fbs/partition.hpp>
#include <vast/fbs/segment.hpp>
#include <vast/fbs/segmented_file.hpp>
#include <vast/io/read.hpp>
#include <vast/logger.hpp>

#include <caf/expected.hpp>

#include <iostream>
#include <map>

#include "util.hpp"

namespace lsvast {

static const std::map<Kind, printer> printers = {
  {Kind::Unknown, print_unknown},
  {Kind::DatabaseDir, print_vast_db},
  {Kind::Index, print_index},
  {Kind::Partition, print_partition},
  {Kind::PartitionSynopsis, print_partition_synopsis},
  {Kind::Segment, print_segment},
};

caf::expected<Kind> classify(const std::filesystem::path& path) {
  std::error_code err{};
  const auto is_directory = std::filesystem::is_directory(path, err);
  if (err)
    return caf::make_error(vast::ec::filesystem_error,
                           "Invalid path: " + path.string(), err.message());
  if (is_directory)
    return Kind::DatabaseDir;
  const auto is_regular_file = std::filesystem::is_regular_file(path, err);
  if (err)
    return caf::make_error(vast::ec::filesystem_error,
                           "Not a file: ", err.message());

  if (!is_regular_file)
    return Kind::Unknown;
  auto bytes = vast::io::read(path);
  if (!bytes)
    return Kind::Unknown;
  auto const* buf = bytes->data();
  auto identifier = std::string(flatbuffers::GetBufferIdentifier(buf), 4);
  if (identifier == vast::fbs::SegmentedFileHeaderIdentifier()) {
    auto header
      = flatbuffers::GetRoot<vast::fbs::SegmentedFileHeader>(bytes->data());
    identifier = std::string(
      reinterpret_cast<const char*>(
        header->header_as_v0()->inner_identifier()->data()->data()),
      4);
  }
  if (identifier == vast::fbs::IndexIdentifier())
    return Kind::Index;
  if (identifier == vast::fbs::PartitionIdentifier())
    return Kind::Partition;
  if (identifier == vast::fbs::SegmentIdentifier())
    return Kind::Segment;
  if (identifier == vast::fbs::PartitionSynopsisIdentifier())
    return Kind::PartitionSynopsis;
  return Kind::Unknown;
}

void print_unknown(const std::filesystem::path& path, indentation& indent,
                   const options&) {
  fmt::print("{}(unknown {})\n", indent, path.string());
}

void print_vast_db(const std::filesystem::path& vast_db, indentation& indent,
                   const options& options) {
  // TODO: We should have some versioning for the layout
  // of the vast.db directory itself, so we can still read
  // older versions.
  auto index_dir = vast_db / "index";
  fmt::print("{}{}/\n", indent, index_dir.string());
  {
    indented_scope _(indent);
    fmt::print("{}index.bin - ", indent);
    print_index(index_dir / "index.bin", indent, options);
    std::error_code err{};
    auto dir = std::filesystem::directory_iterator{index_dir, err};
    if (err) {
      fmt::print(stderr, "Failed to find vast db index directory: {}\n",
                 err.message());
    } else {
      for (const auto& entry : dir) {
        const auto stem = entry.path().stem();
        if (stem == "index")
          continue;
        const auto extension = entry.path().extension();
        // TODO: Print partition synopses.
        if (extension == ".mdx")
          continue;
        fmt::print("{}{} - ", indent, stem);
        print_partition(entry.path(), indent, options);
      }
    }
  }
  const auto segments_dir = vast_db / "archive";
  fmt::print("{}{}/\n", indent, segments_dir.string());
  auto segment_options = options;
  // Only print a segment overview, not the whole contents
  segment_options.segment.print_contents = false;
  {
    indented_scope _(indent);
    std::error_code err{};
    auto dir = std::filesystem::directory_iterator{segments_dir, err};
    if (err) {
      fmt::print(stderr, "Failed to find vast db segments directory: {}\n",
                 err.message());
    } else {
      for (const auto& entry : dir) {
        const auto stem = entry.path().stem();
        fmt::print("{}{} - ", indent, stem);
        print_segment(entry.path(), indent, segment_options);
      }
    }
  }
}

} // namespace lsvast

using namespace lsvast;

int main(int argc, char** argv) {
  // Initialize factories.
  [[maybe_unused]] auto config = vast::configuration{};
  std::string raw_path;
  struct options options;
  auto& format = options.format;
  format.print_bytesizes = true;
  format.verbosity = output_verbosity::normal;
  for (int i = 1; i < argc; ++i) {
    auto arg = std::string_view{argv[i]};
    if (arg == "-h" || arg == "--human-readable") {
      format.print_bytesizes = true;
      format.human_readable_numbers = true;
    } else if (arg == "-s" || arg == "--print-bytesizes") {
      format.print_bytesizes = true;
    } else if (arg == "-v" || arg == "--verbose") {
      format.verbosity = output_verbosity::verbose;
    } else if (arg == "--expand-index") {
      if (i + 1 >= argc) {
        fmt::print(stderr, "Missing argument for --expand-index\n");
        return 1;
      }
      options.partition.expand_indexes.emplace_back(argv[++i]);
    } else if (arg == "--raw-bloom-filters") {
      options.synopsis.bloom_raw = true;
    } else { // positional arg
      raw_path = arg;
    }
  }
  if (raw_path.empty()) {
    fmt::print("Usage: ./lsvast <path/to/vast.db> [options]\n"
               "Options:\n"
               "  --verbose\n"
               "  --print-bytesizes\n"
               "  --human-readable\n"
               "  --raw-bloom-filters\n");
    return 1;
  }
  if (raw_path.back() == '/')
    raw_path.resize(raw_path.size() - 1);
  const auto path = std::filesystem::path{raw_path};
  const auto kind = classify(path);
  if (!kind) {
    fmt::print(stderr, "Filesystem error with error code: {}\n", kind.error());
    return 1;
  }
  if (kind == Kind::Unknown) {
    fmt::print(stderr, "Could not determine type of {}\n", argv[1]);
    return 1;
  }
  auto log_context
    = vast::create_log_context(vast::invocation{}, caf::settings{});
  struct indentation indent;
  auto printer = printers.at(*kind);
  printer(path, indent, options);
  return 0;
}
