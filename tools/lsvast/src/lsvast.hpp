//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace lsvast {

enum class Kind {
  Unknown,
  DatabaseDir,
  Partition,
  Index,
  Segment,
};

enum class output_verbosity : uint8_t {
  normal,
  verbose,
};

struct formatting_options {
  output_verbosity verbosity = output_verbosity::normal;
  bool print_bytesizes = false;
  // Print e.g. "2 TiB" instead of '2199023255552'.
  bool human_readable_numbers = false;
};

// Options specific to printing partitions.
struct partition_options {
  std::vector<std::string> expand_indexes = {};
};

// Options specific to printing segments.
struct segment_options {
  bool print_contents = true;
};

// Global options.
struct options {
  formatting_options format = {};
  partition_options partition = {};
  segment_options segment = {};
};

struct indentation;

using printer
  = void (*)(const std::filesystem::path&, indentation&, const options&);

void print_unknown(const std::filesystem::path&, indentation&, const options&);
void print_vast_db(const std::filesystem::path&, indentation&, const options&);
void print_partition(const std::filesystem::path&, indentation&,
                     const options&);
void print_index(const std::filesystem::path&, indentation&, const options&);
void print_segment(const std::filesystem::path&, indentation&, const options&);

} // namespace lsvast
