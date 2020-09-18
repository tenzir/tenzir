/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/chunk.hpp"
#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/uuid.hpp"
#include "vast/directory.hpp"
#include "vast/fbs/index.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/segment.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/io/read.hpp"
#include "vast/path.hpp"
#include "vast/uuid.hpp"

#include <caf/binary_deserializer.hpp>

#include <flatbuffers/flatbuffers.h>

#include <cstddef>
#include <iostream>
#include <string>

using std::cerr;
using std::cout;
using std::endl;

using namespace std::string_literals;

enum class Kind {
  Unknown,
  DatabaseDir,
  Partition_v0,
  Index_v0,
  Segment_v0,
};

typedef void (*printer)(vast::path);

void print_unknown(vast::path);
void print_vast_db(vast::path);
void print_partition_v0(vast::path);
void print_index_v0(vast::path);
void print_segment_v0(vast::path);

static const std::map<Kind, printer> printers = {
  {Kind::Unknown, print_unknown},
  {Kind::DatabaseDir, print_vast_db},
  {Kind::Index_v0, print_index_v0},
  {Kind::Partition_v0, print_partition_v0},
  {Kind::Segment_v0, print_segment_v0},
};

Kind classify(vast::path path) {
  if (path.is_directory() && path.basename() == "vast.db")
    return Kind::DatabaseDir;
  if (!path.is_regular_file())
    return Kind::Unknown;
  auto bytes = vast::io::read(path);
  if (!bytes)
    return Kind::Unknown;
  auto view = vast::span<const vast::byte>(bytes->data(), bytes->size());
  auto [type, version] = vast::fbs::resolve_filemagic(view);
  if (type == "vast.fbs.Index" && version == vast::fbs::Version::v0)
    return Kind::Index_v0;
  if (type == "vast.fbs.Partition" && version == vast::fbs::Version::v0)
    return Kind::Partition_v0;
  if (type == "vast.fbs.Segment" && version == vast::fbs::Version::v0)
    return Kind::Segment_v0;
  return Kind::Unknown;
}

void print_unknown(vast::path path) {
  std::cout << "(unknown " << path.str() << ")\n";
}

void print_vast_db(vast::path vast_db) {
  // TODO: We should have some versioning for the layout
  // of the vast.db directory itself, so we can still read
  // older versions.
  auto index_dir = vast_db / "index";
  print_index_v0(index_dir / "index.bin");
  for (auto file : vast::directory{index_dir}) {
    auto stem = file.basename(true).str();
    if (stem == "index")
      continue;
    print_partition_v0(file);
  }
  auto segments_dir = vast_db / "archive" / "segments";
  for (auto file : vast::directory{segments_dir}) {
    print_segment_v0(file);
  }
}

void print_partition_v0(vast::path path) {
  using vast::fbs::Partition;
  using vast::fbs::Version;
  auto bytes = vast::io::read(path);
  if (!bytes) {
    std::cout << "(error: " << caf::to_string(bytes.error()) << ")\n";
    return;
  }
  auto view = vast::span<const vast::byte>(bytes->data(), bytes->size());
  auto maybe_partition
    = vast::fbs::as_versioned_flatbuffer<Partition>(view, Version::v0);
  auto& partition = *maybe_partition;
  vast::uuid id;
  if (partition->uuid())
    unpack(*partition->uuid(), id);
  std::cout << "  uuid: " << to_string(id) << "\n";
  std::cout << "  offset: " << partition->offset() << "\n";
  std::cout << "  events: " << partition->events() << "\n";
  for (auto type_ids : *partition->type_ids()) {
    auto name = type_ids->name()->c_str();
    auto ids_bytes = type_ids->ids();
    std::cout << "  " << name << ": ";
    vast::ids restored_ids;
    caf::binary_deserializer bds(
      nullptr, reinterpret_cast<const char*>(ids_bytes->data()),
      ids_bytes->size());
    if (auto error = bds(restored_ids))
      std::cout << "(error: " << caf::to_string(error) << ")";
    else
      std::cout << rank(restored_ids);
    std::cout << "\n";
  }
  // TODO: print combined_layout and indexes
}

void print_index_v0(vast::path path) {
  using vast::fbs::Index;
  using vast::fbs::Version;
  std::cout << "Index v0" << std::endl;
  auto bytes = vast::io::read(path);
  if (!bytes)
    std::cout << "(error: " << caf::to_string(bytes.error()) << ")\n";
  auto view = vast::span<const vast::byte>(bytes->data(), bytes->size());
  auto maybe_index
    = vast::fbs::as_versioned_flatbuffer<Index>(view, Version::v0);
  if (!maybe_index)
    std::cout << "(error: " << caf::to_string(maybe_index.error()) << ")\n";
  auto& index = *maybe_index;
  std::cout << "layouts:\n";
  if (!index->stats()) {
    std::cout << "  (null)\n";
  } else {
    for (auto stat : *index->stats())
      std::cout << "  " << stat->name()->c_str() << ": " << stat->count()
                << std::endl;
  }
  // TODO: Print partition ids and meta index contents.
}

void print_segment_v0(vast::path path) {
  using vast::fbs::Segment;
  using vast::fbs::Version;
  std::cout << "Segment v0" << std::endl;
  auto bytes = vast::io::read(path);
  if (!bytes) {
    std::cout << "(error: " << caf::to_string(bytes.error()) << ")\n";
    return;
  }
  auto view = vast::span<const vast::byte>(bytes->data(), bytes->size());
  auto maybe_segment
    = vast::fbs::as_versioned_flatbuffer<Segment>(view, Version::v0);
  if (!maybe_segment) {
    std::cout << "(error: " << caf::to_string(maybe_segment.error()) << ")\n";
    return;
  }
  auto& segment = *maybe_segment;
  vast::uuid id;
  if (segment->uuid())
    unpack(*segment->uuid(), id);
  std::cout << "  uuid: " << to_string(id) << "\n";
  std::cout << "  events: " << segment->events() << "\n";
}

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "Usage: ./lsvast <path/to/vast.db>\n";
    return 1;
  }
  std::string raw_path{argv[1]};
  if (raw_path.back() == '/')
    raw_path.resize(raw_path.size() - 1);
  auto path = vast::path{raw_path};
  auto kind = classify(path);
  // TODO: Add command line options to force a specific kind.
  if (kind == Kind::Unknown) {
    std::cerr << "Could not determine type of " << argv[1] << std::endl;
    return 1;
  }
  auto printer = printers.at(kind);
  printer(path);
  return 0;
}
