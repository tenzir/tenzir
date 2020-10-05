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
#include "vast/fbs/index_v0.hpp"
#include "vast/fbs/partition.hpp"
#include "vast/fbs/partition_v0.hpp"
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
  Partition_v1,
  Index_v0,
  Index_v1,
  Segment_v0,
};

typedef void (*printer)(vast::path);

void print_unknown(vast::path);
void print_vast_db(vast::path);
void print_partition(vast::path);
void print_partition_v0(vast::path);
void print_partition_v1(vast::path);
void print_index(vast::path);
void print_index_v0(vast::path);
void print_index_v1(vast::path);
void print_segment_v0(vast::path);

static const std::map<Kind, printer> printers = {
  {Kind::Unknown, print_unknown},
  {Kind::DatabaseDir, print_vast_db},
  {Kind::Index_v0, print_index_v0},
  {Kind::Index_v1, print_index_v1},
  {Kind::Partition_v0, print_partition_v0},
  {Kind::Partition_v1, print_partition_v1},
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
  if (type == "vast.fbs.v0.Index" && version == vast::fbs::Version::v0)
    return Kind::Index_v0;
  if (type == "vast.fbs.Index" && version == vast::fbs::Version::v1)
    return Kind::Index_v1;
  if (type == "vast.fbs.v0.Partition" && version == vast::fbs::Version::v0)
    return Kind::Partition_v0;
  if (type == "vast.fbs.Partition" && version == vast::fbs::Version::v1)
    return Kind::Partition_v1;
  if (type == "vast.fbs.Segment" && version == vast::fbs::Version::v0)
    return Kind::Segment_v0;
  return Kind::Unknown;
}

template <typename T>
struct flatbuffer_deleter {
  // Plumbing for a move-only type.
  flatbuffer_deleter() = default;
  flatbuffer_deleter(const flatbuffer_deleter&) = delete;
  flatbuffer_deleter(flatbuffer_deleter&&) = default;

  flatbuffer_deleter(std::vector<vast::byte>&& c) : chunk_(std::move(c)) {
  }

  void operator()(const T*) {
    // nop (the destructor of `chunk_` already releases the memory)
  }

  std::vector<vast::byte> chunk_;
};

// Get contents of the specified file as versioned flatbuffer, or nullptr in
// case of a read error/version mismatch.
// The unique_pointer is used to have a pointer with the correct flatbuffer
// type, that will still delete the underlying vector from `io::read`
// automatically upon destruction.
template <typename T>
std::unique_ptr<const T, flatbuffer_deleter<T>>
read_flatbuffer_file(vast::path path, vast::fbs::Version version) {
  using result_t = std::unique_ptr<const T, flatbuffer_deleter<T>>;
  auto result
    = result_t(static_cast<const T*>(nullptr), flatbuffer_deleter<T>{});
  auto maybe_bytes = vast::io::read(path);
  if (!maybe_bytes)
    return result;
  auto bytes = std::move(*maybe_bytes);
  auto view = vast::span<const vast::byte>(bytes.data(), bytes.size());
  auto maybe_flatbuffer = vast::fbs::as_versioned_flatbuffer<T>(view, version);
  if (!maybe_flatbuffer)
    return result;
  return result_t(*maybe_flatbuffer, flatbuffer_deleter<T>(std::move(bytes)));
}

std::ostream& operator<<(std::ostream& out, const vast::fbs::UUID* uuid) {
  if (!uuid || !uuid->data())
    return out << "(null)";
  for (size_t i = 0; i < uuid->data()->size(); ++i)
    out << std::hex << +uuid->data()->Get(i);
  return out;
}

void print_unknown(vast::path path) {
  std::cout << "(unknown " << path.str() << ")\n";
}

void print_vast_db(vast::path vast_db) {
  // TODO: We should have some versioning for the layout
  // of the vast.db directory itself, so we can still read
  // older versions.
  auto index_dir = vast_db / "index";
  print_index(index_dir / "index.bin");
  for (auto file : vast::directory{index_dir}) {
    auto stem = file.basename(true).str();
    if (stem == "index")
      continue;
    print_partition(file);
  }
  auto segments_dir = vast_db / "archive" / "segments";
  for (auto file : vast::directory{segments_dir}) {
    print_segment_v0(file);
  }
}

/// Prints the common fields of v0 and v1 partitions.
template <typename T>
void print_partition_common(const T* partition) {
  if (!partition) {
    std::cout << "(error reading partition)\n";
    return;
  }
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

void print_partition_v0(vast::path path) {
  auto partition = read_flatbuffer_file<vast::fbs::v0::Partition>(
    path, vast::fbs::Version::v0);
  print_partition_common(partition.get());
}

void print_partition_v1(vast::path path) {
  auto partition
    = read_flatbuffer_file<vast::fbs::Partition>(path, vast::fbs::Version::v1);
  print_partition_common(partition.get());
  // TODO: print partition synopsis
}

void print_partition(vast::path path) {
  auto bytes = vast::io::read(path);
  if (!bytes) {
    std::cout << "(error: " << vast::render(bytes.error()) << ")\n";
    return;
  }
  auto view = vast::span<const vast::byte>(bytes->data(), bytes->size());
  auto version = vast::fbs::resolve_filemagic(view).second;
  if (version == vast::fbs::Version::v0)
    print_partition_v0(path);
  else if (version == vast::fbs::Version::v1)
    print_partition_v1(path);
  else
    std::cout << "(unknown version)\n";
}

template <typename T>
static void print_index_common(const T* index) {
  if (!index) {
    std::cout << "(error reading index)\n";
    return;
  }
  std::cout << "layouts:\n";
  if (!index->stats()) {
    std::cout << "  (null)\n";
  } else {
    for (auto stat : *index->stats())
      std::cout << "  " << stat->name()->c_str() << ": " << stat->count()
                << std::endl;
  }
  std::cout << "partitions: ";
  if (!index->partitions()) {
    std::cout << "(null)\n";
  } else {
    std::cout << '[';
    for (auto uuid : *index->partitions())
      std::cout << uuid << ", ";
    std::cout << "]\n";
  }
}

void print_index_v0(vast::path path) {
  auto idx
    = read_flatbuffer_file<vast::fbs::v0::Index>(path, vast::fbs::Version::v0);
  print_index_common(idx.get());
  // TODO: Print meta index contents.
}

void print_index_v1(vast::path path) {
  auto idx
    = read_flatbuffer_file<vast::fbs::Index>(path, vast::fbs::Version::v1);
  print_index_common(idx.get());
}

void print_index(vast::path path) {
  auto bytes = vast::io::read(path);
  if (!bytes) {
    std::cout << "(error: " << vast::render(bytes.error()) << ")\n";
    return;
  }
  auto view = vast::span<const vast::byte>(bytes->data(), bytes->size());
  auto version = vast::fbs::resolve_filemagic(view).second;
  if (version == vast::fbs::Version::v0)
    print_index_v0(path);
  else if (version == vast::fbs::Version::v1)
    print_index_v1(path);
  else
    std::cout << "(unknown version)\n";
}

void print_segment_v0(vast::path path) {
  using vast::fbs::Segment;
  using vast::fbs::Version;
  std::cout << "Segment v0" << std::endl;
  auto bytes = vast::io::read(path);
  if (!bytes) {
    std::cout << "(error: " << vast::render(bytes.error()) << ")\n";
    return;
  }
  auto view = vast::span<const vast::byte>(bytes->data(), bytes->size());
  auto maybe_segment
    = vast::fbs::as_versioned_flatbuffer<Segment>(view, Version::v0);
  if (!maybe_segment) {
    std::cout << "(error: " << vast::render(maybe_segment.error()) << ")\n";
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
