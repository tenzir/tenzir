//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2020 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/fbs/index.hpp>

#include <iostream>

#include "lsvast.hpp"
#include "util.hpp"

namespace lsvast {

void print_index_v0(const vast::fbs::index::v0* index, indentation& indent,
                    const options&) {
  if (!index) {
    std::cout << "(null)\n";
    return;
  }
  std::cout << indent << "Index\n";
  indented_scope _(indent);
  // print schemas
  std::cout << indent << "schemas:\n";
  if (auto stats = index->stats()) {
    indented_scope _(indent);
    for (auto stat : *stats)
      std::cout << indent << stat->name()->c_str() << ": " << stat->count()
                << std::endl;
  }
  // print partitions
  std::cout << indent << "partitions: ";
  if (auto partitions = index->partitions()) {
    std::cout << '[';
    for (size_t i = 0; i < partitions->size(); ++i) {
      auto uuid = partitions->Get(i);
      std::cout << uuid;
      if (i < partitions->size() - 1)
        std::cout << ", ";
    }
    std::cout << ']';
  }
  std::cout << "\n";
}

void print_index(const std::filesystem::path& path, indentation& indent,
                 const options& formatting) {
  auto index = read_flatbuffer_file<vast::fbs::Index>(path);
  if (!index) {
    std::cout << indent << "(error reading index file " << path.string()
              << ")\n";
  }
  switch (index->index_type()) {
    case vast::fbs::index::Index::v0:
      print_index_v0(index->index_as_v0(), indent, formatting);
      break;
    default:
      std::cout << "(unknown partition version)\n";
  }
}

} // namespace lsvast
