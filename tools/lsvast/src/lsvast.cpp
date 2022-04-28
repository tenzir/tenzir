//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "lsvast.hpp"

#include <vast/command.hpp>
#include <vast/error.hpp>
#include <vast/fbs/index.hpp>
#include <vast/fbs/partition.hpp>
#include <vast/fbs/segment.hpp>
#include <vast/io/read.hpp>
#include <vast/logger.hpp>
#include <vast/system/configuration.hpp>

#include <caf/expected.hpp>

#include <iostream>
#include <map>

#include "util.hpp"

namespace lsvast {

static const std::map<Kind, printer> printers = {
  {Kind::Unknown, print_unknown}, {Kind::DatabaseDir, print_vast_db},
  {Kind::Index, print_index},     {Kind::Partition, print_partition},
  {Kind::Segment, print_segment},
};

caf::expected<Kind> classify(const std::filesystem::path& path) {
  std::error_code err{};
  const auto is_directory = std::filesystem::is_directory(path, err);
  if (err)
    return caf::make_error(vast::ec::filesystem_error,
                           "Invalid path: " + path.string(), err.message());
  if (is_directory) {
    auto version_file = path / "VERSION";
    auto is_regular_file = std::filesystem::is_regular_file(version_file, err);
    if (err)
      return caf::make_error(vast::ec::filesystem_error,
                             "Invalid path: " + version_file.string(),
                             err.message());
    if (is_regular_file)
      return Kind::DatabaseDir;
  }
  const auto is_regular_file = std::filesystem::is_regular_file(path, err);
  if (err)
    return caf::make_error(vast::ec::filesystem_error,
                           "Not a file: ", err.message());

  if (!is_regular_file)
    return Kind::Unknown;
  auto bytes = vast::io::read(path);
  if (!bytes)
    return Kind::Unknown;
  auto buf = bytes->data();
  if (vast::fbs::IndexBufferHasIdentifier(buf))
    return Kind::Index;
  if (vast::fbs::PartitionBufferHasIdentifier(buf))
    return Kind::Partition;
  if (vast::fbs::SegmentBufferHasIdentifier(buf))
    return Kind::Segment;
  return Kind::Unknown;
}

void print_unknown(const std::filesystem::path& path, indentation& indent,
                   const options&) {
  std::cout << indent << "(unknown " << path.string() << ")\n";
}

void print_vast_db(const std::filesystem::path& vast_db, indentation& indent,
                   const options& options) {
  // TODO: We should have some versioning for the layout
  // of the vast.db directory itself, so we can still read
  // older versions.
  auto index_dir = vast_db / "index";
  std::cout << indent << index_dir.string() << "/\n";
  {
    indented_scope _(indent);
    std::cout << indent << "index.bin - ";
    print_index(index_dir / "index.bin", indent, options);
    std::error_code err{};
    auto dir = std::filesystem::directory_iterator{index_dir, err};
    if (err) {
      std::cerr << "Failed to find vast db index directory: " + err.message()
                << std::endl;
    } else {
      for (const auto& entry : dir) {
        const auto stem = entry.path().stem();
        if (stem == "index")
          continue;
        const auto extension = entry.path().extension();
        // TODO: Print partition synopses.
        if (extension == ".mdx")
          continue;
        std::cout << indent << stem << " - ";
        print_partition(entry.path(), indent, options);
      }
    }
  }
  const auto segments_dir = vast_db / "archive" / "segments";
  std::cout << indent << segments_dir.string() << "/\n";
  auto segment_options = options;
  // Only print a segment overview, not the whole contents
  segment_options.segment.print_contents = false;
  {
    indented_scope _(indent);
    std::error_code err{};
    auto dir = std::filesystem::directory_iterator{segments_dir, err};
    if (err) {
      std::cerr << "Failed to find vast db segments directory: " + err.message()
                << std::endl;
    } else {
      for (const auto& entry : dir) {
        const auto stem = entry.path().stem();
        std::cout << indent << stem << " - ";
        print_segment(entry.path(), indent, segment_options);
      }
    }
  }
}

} // namespace lsvast

using namespace lsvast;

int main(int argc, char** argv) {
  // Initialize factories.
  [[maybe_unused]] auto config = vast::system::configuration{};
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
        std::cerr << "Missing argument for --expand-index\n";
        return 1;
      }
      options.partition.expand_indexes.emplace_back(argv[++i]);
    } else { // positional arg
      raw_path = arg;
    }
  }
  if (raw_path.empty()) {
    std::cerr << "Usage: ./lsvast <path/to/vast.db> [options]\n"
              << "Options:\n"
              << "  --verbose\n"
              << "  --print-bytesizes\n"
              << "  --human-readable\n";
    return 1;
  }
  if (raw_path.back() == '/')
    raw_path.resize(raw_path.size() - 1);
  const auto path = std::filesystem::path{raw_path};
  const auto kind = classify(path);
  if (!kind) {
    std::cerr << "Filesystem error with error code: " << kind.error().code()
              << std::endl;
    return 1;
  }
  if (kind == Kind::Unknown) {
    std::cerr << "Could not determine type of " << argv[1] << std::endl;
    return 1;
  }
  auto log_context
    = vast::create_log_context(vast::invocation{}, caf::settings{});
  struct indentation indent;
  auto printer = printers.at(*kind);
  printer(path, indent, options);
  return 0;
}
