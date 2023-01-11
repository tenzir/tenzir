//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/parseable/vast/uuid.hpp"

#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/uuid.hpp>
#include <vast/detail/filter_dir.hpp>
#include <vast/detail/logger_formatters.hpp>
#include <vast/fbs/utils.hpp>
#include <vast/io/read.hpp>
#include <vast/io/write.hpp>
#include <vast/partition_synopsis.hpp>
#include <vast/synopsis_factory.hpp>
#include <vast/system/configuration.hpp>
#include <vast/system/index.hpp>
#include <vast/uuid.hpp>
#include <vast/value_index_factory.hpp>

#include <fmt/core.h>

#include <filesystem>

int regenerate_mdx(const std::filesystem::path& dbdir) {
  std::error_code err{};
  auto index_dir = dbdir / "index";
  if (!std::filesystem::exists(index_dir, err)) {
    fmt::print(stderr, "No such file or directory: {}\n", index_dir);
    return 1;
  }
  auto index_file = index_dir / "index.bin";
  if (!std::filesystem::exists(index_file, err)) {
    fmt::print(stderr, "No such file or directory: {}\n", index_file);
    return 1;
  }
  fmt::print(stderr, "loading list of partitions from {}\n", index_file);
  auto buffer = vast::io::read(index_file);
  if (!buffer) {
    fmt::print(stderr, "failed to read index from {}: {}\n", index_file,
               vast::render(buffer.error()));
    return 1;
  }
  const auto* index = vast::fbs::GetIndex(buffer->data());
  if (!index) {
    fmt::print(stderr, "failed to interpret contents of {} as fbs::Index\n",
               index_file);
    return 1;
  }
  if (index->index_type() != vast::fbs::index::Index::v0) {
    fmt::print(stderr, "unknown index version");
    return 1;
  }
  const auto* index_v0 = index->index_as_v0();
  VAST_ASSERT(index_v0);
  const auto* partition_uuids = index_v0->partitions();
  VAST_ASSERT(partition_uuids);
  for (const auto* uuid_fb : *partition_uuids) {
    VAST_ASSERT(uuid_fb);
    vast::uuid partition_uuid{};
    if (auto error = unpack(*uuid_fb, partition_uuid)) {
      fmt::print(stderr, "skipping an uuid ({})\n", error);
      continue;
    }
    auto part_path = index_dir / to_string(partition_uuid);
    auto synopsis_path = index_dir / (to_string(partition_uuid) + ".mdx");
    if (!std::filesystem::exists(part_path, err)) {
      fmt::print(stderr, "skipping {}: file not found\n", partition_uuid);
      continue;
    }
    // The actual work happens here.
    if (auto error
        = vast::system::extract_partition_synopsis(part_path, synopsis_path)) {
      fmt::print(stderr, "error creating {}: {}\n", synopsis_path,
                 vast::render(error));
      continue;
    }
    fmt::print(stderr, "successfully wrote {}\n", synopsis_path);
  }
  return 0;
}

caf::error write_index_bin(const std::vector<vast::uuid>& uuids,
                           const std::filesystem::path& index_file) {
  flatbuffers::FlatBufferBuilder builder;
  std::vector<flatbuffers::Offset<vast::fbs::LegacyUUID>> partition_offsets;
  for (const auto& uuid : uuids) {
    if (auto uuid_fb = pack(builder, uuid))
      partition_offsets.push_back(*uuid_fb);
    else
      return uuid_fb.error();
  }
  fmt::print("writing {} partition\n", partition_offsets.size());
  auto partitions = builder.CreateVector(partition_offsets);
  vast::fbs::index::v0Builder v0_builder(builder);
  v0_builder.add_partitions(partitions);
  auto index_v0 = v0_builder.Finish();
  vast::fbs::IndexBuilder index_builder(builder);
  index_builder.add_index_type(vast::fbs::index::Index::v0);
  index_builder.add_index(index_v0.Union());
  auto index = index_builder.Finish();
  vast::fbs::FinishIndexBuffer(builder, index);
  auto chunk = vast::fbs::release(builder);
  // TODO: Compare `previous_index` and `index` and output the diff.
  // Write updated index to disk.
  return vast::io::write(index_file, as_bytes(chunk));
}

int regenerate_index_nocontent(const std::filesystem::path& dbdir) {
  std::error_code err{};
  auto index_dir = dbdir / "index";
  if (!std::filesystem::exists(index_dir, err)) {
    fmt::print(stderr, "No such file or directory: {}\n", index_dir);
    return 1;
  }
  // Get the old index for comparison if it exists.
  auto index_file = index_dir / "index.bin";
  [[maybe_unused]] auto const* previous_index
    = static_cast<vast::fbs::Index*>(nullptr);
  if (std::filesystem::exists(index_file, err)) {
    auto buffer = vast::io::read(index_file);
    if (buffer)
      previous_index = vast::fbs::GetIndex(buffer->data());
  }
  // Collect data of the new `index.bin`.
  auto files = vast::detail::filter_dir(
    index_dir, [](const std::filesystem::path& file) -> bool {
      return file.extension() == ".mdx";
    });
  if (!files) {
    fmt::print(stderr, "Error traversing directory: {}", files.error());
    return 1;
  }
  auto uuids = std::vector<vast::uuid>{};
  for (auto& file : *files) {
    auto name = file.filename().replace_extension("").string();
    auto uuid = vast::uuid{};
    if (!vast::parsers::uuid(name, uuid)) {
      fmt::print("could not parse {} as uuid", name);
      return 1;
    }
    uuids.push_back(uuid);
  }
  // Build the new `index.bin`.
  if (auto error = write_index_bin(uuids, index_file)) {
    fmt::print(stderr, "error writing index to {}: {}", index_file, error);
    return 1;
  }
  return 0;
}

int regenerate_index(const std::filesystem::path& dbdir) {
  std::error_code err{};
  auto index_dir = dbdir / "index";
  if (!std::filesystem::exists(index_dir, err)) {
    fmt::print(stderr, "No such file or directory: {}\n", index_dir);
    return 1;
  }
  // Get the old index for comparison if it exists.
  auto index_file = index_dir / "index.bin";
  [[maybe_unused]] auto const* previous_index
    = static_cast<vast::fbs::Index*>(nullptr);
  if (std::filesystem::exists(index_file, err)) {
    auto buffer = vast::io::read(index_file);
    if (buffer)
      previous_index = vast::fbs::GetIndex(buffer->data());
  }
  // Collect data of the new `index.bin`.
  auto files = vast::detail::filter_dir(
    index_dir, [](const std::filesystem::path& file) -> bool {
      return file.extension() == ".mdx";
    });
  if (!files) {
    fmt::print(stderr, "Error traversing directory: {}", files.error());
    return 1;
  }
  auto uuids = std::vector<vast::uuid>{};
  for (auto& file : *files) {
    auto partition_file = file.filename().replace_extension("");
    auto chunk = vast::chunk::mmap(partition_file);
    if (!chunk) {
      fmt::print(stderr, "Error mapping file {}: {}", file, chunk.error());
      return 1;
    }
    auto const* partition = vast::fbs::GetPartition(chunk->get()->data());
    if (partition->partition_type()
        != vast::fbs::partition::Partition::legacy) {
      fmt::print(stderr, "found unsupported version for partition {}\n", file);
      return 1;
    }
    auto const* partition_legacy = partition->partition_as_legacy();
    VAST_ASSERT(partition_legacy);
    auto const* uuid_fb = partition_legacy->uuid();
    vast::uuid uuid = {};
    if (auto error = unpack(*uuid_fb, uuid)) {
      fmt::print(stderr, "Could not unpack uuid in {}: {}", file, error);
      return 1;
    }
    uuids.push_back(uuid);
    for (const auto* partition_stats : *partition_legacy->type_ids()) {
      vast::ids ids;
      if (auto error
          = vast::fbs::deserialize_bytes(partition_stats->ids(), ids)) {
        fmt::print(stderr, "could not deserialize ids for partition {}: {}\n",
                   uuid, error);
        return 1;
      }
    }
  }
  // Build the new `index.bin`.
  if (auto error = write_index_bin(uuids, index_file)) {
    fmt::print(stderr, "error writing index to {}: {}", index_file, error);
    return 1;
  }
  return 0;
}

int main(int argc, char* argv[]) {
  const auto* usage = R"_(
Usage: vast-regenerate --mdx /path/to/vast.db
       vast-regenerate --index /path/to/vast.db
       vast-regenerate --index-hollow /path/to/vast.db

Note that 'vast-regenerate' is intended for advanced users and developers.

In '--mdx' mode, the 'index/*.mdx' files are regenerated from existing
partitions.

In '--index' mode, the 'index.bin' file is regenerated from the partitions
found on disk.

In '--index-hollow' mode the 'index.bin' will be be regenerated from
the partition synopses, just looking at the filenames and not loading
the content of any files. Note that this will produce an index file
with an incorrect all-zero event count.
)_";
  // Initialize factories.
  [[maybe_unused]] auto config = vast::system::configuration{};
  std::error_code err{};
  if (argc != 3) {
    fmt::print(stderr, "{}", usage);
    return 1;
  }
  auto path = std::filesystem::path{};
  bool mdx = false;
  bool index = false;
  bool index_hollow = false;
  for (int i = 1; i < argc; ++i) {
    auto arg = std::string_view{argv[i]};
    if (arg == "-h" || arg == "--help") {
      fmt::print("{}", usage);
      return 0;
    } else if (arg == "--mdx") {
      mdx = true;
    } else if (arg == "--index") {
      index = true;
    } else if (arg == "--index-hollow") {
      index_hollow = true;
    } else {
      path = arg;
    }
  }
  if (path.empty()) {
    fmt::print(stderr, "error: missing required path argument.\n\n{}", usage);
    return 1;
  }
  if (!mdx && !index && !index_hollow) {
    fmt::print(stderr, "error: at least one mode option must be given.\n\n{}",
               usage);
    return 1;
  }
  if (mdx + index + index_hollow > 1) {
    fmt::print(stderr, "error: only one mode option may be given.\n");
    return 1;
  }
  if (mdx)
    return regenerate_mdx(path);
  if (index)
    return regenerate_index(path);
  if (index_hollow)
    return regenerate_index_nocontent(path);
}
