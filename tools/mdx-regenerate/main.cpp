//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/concept/printable/to_string.hpp>
#include <vast/concept/printable/vast/uuid.hpp>
#include <vast/detail/logger_formatters.hpp>
#include <vast/io/read.hpp>
#include <vast/synopsis_factory.hpp>
#include <vast/system/index.hpp>
#include <vast/uuid.hpp>
#include <vast/value_index_factory.hpp>

#include <fmt/core.h>

#include <filesystem>

int main(int argc, char* argv[]) {
  // vast::factory_traits<vast::value_index>::initialize();
  vast::factory<vast::synopsis>::initialize();

  std::error_code err{};
  if (argc < 2) {
    fmt::print(stderr, "Usage: {} /path/to/vast.db\n", argv[0]);
    return 1;
  }
  auto dbdir = std::filesystem::path{argv[1]};
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
  fmt::print("loading list of partitions from {}\n", index_file);
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
    fmt::print("successfully wrote {}\n", synopsis_path);
  }
}
