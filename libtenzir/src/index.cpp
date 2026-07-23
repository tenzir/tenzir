//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/index.hpp"

#include "tenzir/fwd.hpp"

#include "tenzir/active_partition.hpp"
#include "tenzir/catalog.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/tenzir/bitmap.hpp"
#include "tenzir/concept/printable/tenzir/error.hpp"
#include "tenzir/concept/printable/tenzir/expression.hpp"
#include "tenzir/concept/printable/tenzir/table_slice.hpp"
#include "tenzir/concept/printable/tenzir/uuid.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/defaults.hpp"
#include "tenzir/detail/actor_metrics.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/fanout_counter.hpp"
#include "tenzir/detail/fill_status_map.hpp"
#include "tenzir/detail/narrow.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/error.hpp"
#include "tenzir/fbs/index.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/fbs/partition_transform.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/fbs/uuid.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/partition_transformer.hpp"
#include "tenzir/passive_partition.hpp"
#include "tenzir/plugin/register.hpp"
#include "tenzir/plugin/store.hpp"
#include "tenzir/shutdown.hpp"
#include "tenzir/status.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/uuid.hpp"

#include <caf/actor_registry.hpp>
#include <caf/error.hpp>
#include <caf/make_copy_on_write.hpp>
#include <caf/response_promise.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <flatbuffers/flatbuffers.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <ctime>
#include <deque>
#include <filesystem>
#include <memory>
#include <numeric>
#include <span>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

// clang-format off
//
// # Import
//
// The index splits the "stream" of incoming table slices by schema and forwards
// them to active partitions. It rotates the active partition for each schema
// when the active partition timeout is hit or the partition reached its maximum
// size.
//
//              table slice              table slice                      table slice column
//   importer ----------------> index ---------------> active partition ------------------------> indexer
//                                                                      ------------------------> indexer
//                                                                                ...
//
// # Lookup
//
// At the same time, the index is also involved in the lookup path, where it
// receives an expression and loads the partitions that might contain relevant
// results into memory.
//
//    expression                                lookup()
//   ------------>  index                  --------------------> catalog
//                                                                 |
//     query_id,                                                   |
//     scheduled,                                                  |
//     remaining                            [uuid, query_context]  |
//   <-----------  (creates query state)  <------------------------/
//                            |
//                            |  query_id, n_taste
//                            |
//    query_id, n             v                   expression, client
//   ------------> (spawn n partitions) --------------------------------> partition
//                                                                            |
//                                                      ids                   |
//   <------------------------------------------------------------------------/
//                                                      ids                   |
//   <------------------------------------------------------------------------/
//                                                                            |
//
//                                                                          [...]
//
//                                                      atom::done            |
//   <------------------------------------------------------------------------/
//
//
// # Partition Transforms
//
//
//
//   atom::apply, transform              spawn()
// ---------------------------> index  -----------> partition_transformer
//                                                                    |
//                                                                    \--------------> write index/markers/188427dd-1577-4b2a-b99c-09e91d1c167f
//                                                                    \--------------> write index/markers/188427dd-1577-4b2a-b99c-09e91d1c167f.mdx
//                                                                    |
//                                                                  [...] (2 files per output partition)
//                                      vector<partition_synopsis>    |
//                             index  <-------------------------------/
//                            |     | -----|
//                                         | write index/markers/{transform_id}.marker
//                                         | (contains list of input and output partitions)
//                            |     | <----/
//                            |     | ~~~~~|
//                                         | atom::rename (move output partitions from index/markers/ to index/ )
//                                         | update index statistics
//                                         | atom::erase (for every input partition)
//   atom::done               |     |<~~~~~/
// <--------------------------|     |
//                                  |------|
//                                         |
//                                         | erase index/markers/{transform_id}.marker
//                                    <----/
//
// On index startup in `index::load_from_disk()` we first go through the `index/markers/` directory
// and finish up the work recorded in any existing marker files.
//
// # Erase
//
// We currently have two distinct erasure code paths: One externally driven by
// the disk monitor, who looks at the file system and identifies those partitions
// that shall be removed. This is done by the `atom::erase` handler.
//
// clang-format on

namespace {

/// Test if the bytes 4-8 of the are equal to `identifier`.
bool test_file_identifier(std::filesystem::path file, const char* identifier) {
  std::byte buffer[8];
  if (auto err
      = tenzir::io::read(file, std::span<std::byte>{buffer, sizeof(buffer)});
      err.valid()) {
    return false;
  }
  return std::memcmp(buffer + 4, identifier, 4) == 0;
}

} // namespace

namespace tenzir {

std::optional<std::filesystem::path>
store_path_for_partition(const std::filesystem::path& base_path,
                         const uuid& id) {
  std::error_code err{};
  for (const char* ext : {"store", "feather", "parquet"}) {
    auto store_filename = fmt::format("{}.{}", id, ext);
    auto candidate = base_path / "archive" / store_filename;
    if (std::filesystem::exists(candidate, err)) {
      return candidate;
    }
  }
  return std::nullopt;
}

caf::error
extract_partition_synopsis(const std::filesystem::path& partition_path,
                           const std::filesystem::path& partition_synopsis_path,
                           bool verify) {
  // Use blocking operations here since this is part of the startup.
  auto chunk = chunk::mmap(partition_path);
  if (not chunk) {
    return std::move(chunk.error());
  }
  auto maybe_partition = partition_chunk::get_flatbuffer(*chunk);
  if (not maybe_partition) {
    return caf::make_error(
      ec::format_error, fmt::format("malformed partition at {}: {}",
                                    partition_path, maybe_partition.error()));
  }
  const auto* partition = *maybe_partition;
  if (partition->partition_type() != fbs::partition::Partition::legacy) {
    return caf::make_error(
      ec::format_error,
      fmt::format("unknown version {} for "
                  "partition at {}",
                  static_cast<uint8_t>(partition->partition_type()),
                  partition_path));
  }
  const auto* partition_legacy = partition->partition_as_legacy();
  TENZIR_ASSERT(partition_legacy);
  partition_synopsis ps;
  if (auto error = unpack(*partition_legacy, ps); error.valid()) {
    return error;
  }
  flatbuffers::FlatBufferBuilder builder;
  auto ps_offset = pack(builder, ps);
  if (not ps_offset) {
    return ps_offset.error();
  }
  fbs::PartitionSynopsisBuilder ps_builder(builder);
  ps_builder.add_partition_synopsis_type(
    fbs::partition_synopsis::PartitionSynopsis::legacy);
  ps_builder.add_partition_synopsis(ps_offset->Union());
  auto flatbuffer = ps_builder.Finish();
  fbs::FinishPartitionSynopsisBuffer(builder, flatbuffer);
  auto chunk_out = fbs::release(builder);
  // Verify the freshly built buffer when callers intend to read it back
  // without verification, so a corrupt synopsis is caught at the source.
  if (verify) {
    if (auto checked = tenzir::flatbuffer<fbs::PartitionSynopsis>::make(
          chunk_ptr{chunk_out});
        not checked) {
      return caf::make_error(
        ec::format_error,
        fmt::format("refusing to write malformed partition "
                    "synopsis to {}: {}",
                    partition_synopsis_path, checked.error()));
    }
  }
  return io::save(partition_synopsis_path,
                  std::span{chunk_out->data(), chunk_out->size()});
}

caf::expected<flatbuffers::Offset<fbs::Index>>
pack(flatbuffers::FlatBufferBuilder& builder, const index_state& state) {
  TENZIR_DEBUG("index persists {} uuids of definitely persisted and {}"
               "uuids of maybe persisted partitions",
               state.persisted_partitions.size(), state.unpersisted.size());
  std::vector<flatbuffers::Offset<fbs::LegacyUUID>> partition_offsets;
  for (const auto& uuid : state.persisted_partitions) {
    if (auto uuid_fb = pack(builder, uuid)) {
      partition_offsets.push_back(*uuid_fb);
    } else {
      return uuid_fb.error();
    }
  }
  // We don't know if these will make it to disk before the index and the rest
  // of the system is shut down (in case of a hard/dirty shutdown), so we just
  // store everything and throw out the missing partitions when loading the
  // index.
  for (const auto& kv : state.unpersisted) {
    if (auto uuid_fb = pack(builder, kv.first)) {
      partition_offsets.push_back(*uuid_fb);
    } else {
      return uuid_fb.error();
    }
  }
  auto partitions = builder.CreateVector(partition_offsets);
  fbs::index::v0Builder v0_builder(builder);
  v0_builder.add_partitions(partitions);
  auto index_v0 = v0_builder.Finish();
  fbs::IndexBuilder index_builder(builder);
  index_builder.add_index_type(tenzir::fbs::index::Index::v0);
  index_builder.add_index(index_v0.Union());
  auto index = index_builder.Finish();
  fbs::FinishIndexBuffer(builder, index);
  return index;
}

tenzir::chunk_ptr create_marker(const std::vector<tenzir::uuid>& in,
                                const std::vector<tenzir::uuid>& out,
                                keep_original_partition keep) {
  flatbuffers::FlatBufferBuilder builder;
  auto in_offsets
    = flatbuffers::Offset<flatbuffers::Vector<const fbs::UUID*>>{};
  if (keep == keep_original_partition::no) {
    in_offsets = builder.CreateVectorOfStructs<fbs::UUID>(
      in.size(), [&in](size_t i, fbs::UUID* vec) {
        ::memcpy(vec->mutable_data()->Data(), in[i].begin(),
                 tenzir::uuid::num_bytes);
      });
  }
  auto out_offsets = builder.CreateVectorOfStructs<fbs::UUID>(
    out.size(), [&out](size_t i, fbs::UUID* vec) {
      ::memcpy(vec->mutable_data()->Data(), out[i].begin(),
               tenzir::uuid::num_bytes);
    });
  auto v0_offset = tenzir::fbs::partition_transform::Createv0(
    builder, in_offsets, out_offsets);
  auto transform_offset = tenzir::fbs::CreatePartitionTransform(
    builder, tenzir::fbs::partition_transform::PartitionTransform::v0,
    v0_offset.Union());
  fbs::FinishPartitionTransformBuffer(builder, transform_offset);
  return tenzir::chunk::make(builder.Release());
}

// -- partition_factory --------------------------------------------------------

partition_factory::partition_factory(index_state& state) : state_{state} {
  // nop
}

filesystem_actor& partition_factory::filesystem() {
  return filesystem_;
}

partition_actor partition_factory::operator()(const uuid& id) const {
  // Load partition from disk.
  if (state_.persisted_partitions.find(id)
      == state_.persisted_partitions.end()) {
    TENZIR_WARN("{} did not find partition {} in it's internal state, but "
                "tries "
                "to load it regardless",
                *state_.self, id);
  }
  const auto path = state_.partition_path(id);
  TENZIR_TRACE("{} loads partition {} for path {}", *state_.self, id, path);
  materializations_++;
  return state_.self->spawn(passive_partition, id, filesystem_, path,
                            caf::message_priority::normal);
}

size_t partition_factory::materializations() const {
  return materializations_;
}

// -- index_state --------------------------------------------------------------

index_state::index_state(index_actor::pointer self)
  : self{self}, inmem_partitions{0, partition_factory{*this}} {
}

// -- persistence --------------------------------------------------------------

std::filesystem::path
index_state::index_filename(const std::filesystem::path& basename) const {
  return basename / dir / "index.bin";
}

std::filesystem::path index_state::marker_path(const uuid& id) const {
  return markersdir / fmt::format("{:l}.marker", id);
}

std::filesystem::path index_state::partition_path(const uuid& id) const {
  return dir / fmt::format("{:l}", id);
}

std::filesystem::path index_state::archive_dir() const {
  return dir / ".." / "archive";
}

std::string index_state::partition_path_template() const {
  return (dir / "{:l}").string();
}

std::filesystem::path
index_state::transformer_partition_path(const uuid& id) const {
  return markersdir / fmt::format("{:l}", id);
}

std::string index_state::transformer_partition_path_template() const {
  return (markersdir / "{:l}").string();
}

std::filesystem::path
index_state::partition_synopsis_path(const uuid& id) const {
  return synopsisdir / fmt::format("{:l}.mdx", id);
}

std::filesystem::path
index_state::transformer_partition_synopsis_path(const uuid& id) const {
  return markersdir / fmt::format("{:l}.mdx", id);
}

std::string index_state::transformer_partition_synopsis_path_template() const {
  return (dir / "markers" / "{:l}.mdx").string();
}

caf::error index_state::load_from_disk() {
  // We dont use the filesystem actor here because this function is only
  // called once during startup, when no other actors exist yet.
  std::error_code err{};
  auto const file_exists = std::filesystem::exists(dir, err);
  if (not file_exists) {
    TENZIR_VERBOSE("{} found no prior state, starting with a clean slate",
                   *self);
    self->mail(atom::start_v, std::vector<partition_synopsis_pair>{})
      .request(catalog, caf::infinite)
      .then([](atom::ok) {},
            [this](caf::error err) {
              self->quit(std::move(err));
            });
    return caf::none;
  }
  // Start by finishing up any in-progress transforms.
  if (std::filesystem::is_directory(markersdir, err)) {
    auto error = [&]() -> caf::error {
      auto transforms_dir_iter
        = std::filesystem::directory_iterator(markersdir, err);
      if (err) {
        return caf::make_error(ec::filesystem_error,
                               fmt::format("{} failed to list directory "
                                           "contents "
                                           "of {}: {}",
                                           *self, dir, err.message()));
      }
      for (auto const& entry : transforms_dir_iter) {
        if (entry.path().extension() != ".marker") {
          continue;
        }
        auto chunk = tenzir::chunk::mmap(entry.path());
        if (not chunk) {
          TENZIR_WARN("{} failed to mmap chunk at {}: {}", *self, entry.path(),
                      chunk.error());
          continue;
        }
        auto maybe_flatbuffer
          = tenzir::flatbuffer<tenzir::fbs::PartitionTransform>::make(
            std::move(*chunk));
        if (not maybe_flatbuffer) {
          TENZIR_WARN("{} failed to open transform {}: {}", *self, entry.path(),
                      err.message());
          continue;
        }
        auto& transform_flatbuffer = *maybe_flatbuffer;
        if (transform_flatbuffer->transform_type()
            != tenzir::fbs::partition_transform::PartitionTransform::v0) {
          TENZIR_WARN("{} detected unknown transform version at {}", *self,
                      entry.path());
          continue;
        }
        auto const* transform_v0 = transform_flatbuffer->transform_as_v0();
        for (auto const* id : *transform_v0->input_partitions()) {
          auto uuid = tenzir::uuid::from_flatbuffer(*id);
          auto path = partition_path(uuid);
          if (std::filesystem::exists(path, err)) {
            // TODO: In combination with inhomogeneous partitions, this may
            // result in incorrect index statistics. This depends on whether the
            // statistics where already updated on-disk before Tenzir crashed or
            // not, which is hard to figure out here.
            auto partition = self->spawn(passive_partition, uuid, filesystem,
                                         path, caf::message_priority::normal);
            self->mail(atom::erase_v)
              .request(partition, caf::infinite)
              .then(
                [this, uuid](atom::done) {
                  TENZIR_DEBUG("{} erased partition {} during startup", *self,
                               uuid);
                },
                [this, uuid](const caf::error& e) {
                  TENZIR_WARN("{} failed to erase partition {} during startup: "
                              "{}",
                              *self, uuid, e);
                });
          }
        }
        for (auto const* id : *transform_v0->output_partitions()) {
          const auto uuid = tenzir::uuid::from_flatbuffer(*id);
          const auto from_partition = fmt::format(
            TENZIR_FMT_RUNTIME(transformer_partition_path_template()), uuid);
          const auto to_partition = partition_path(uuid);
          const auto from_partition_synopsis = fmt::format(
            TENZIR_FMT_RUNTIME(transformer_partition_synopsis_path_template()),
            uuid);
          const auto to_partition_synopsis = partition_synopsis_path(uuid);
          auto ec = std::error_code{};
          std::filesystem::rename(from_partition, to_partition, ec);
          if (ec) {
            TENZIR_WARN("failed to rename '{}' to '{}': {}", from_partition,
                        to_partition, ec.message());
          }
          ec.clear();
          std::filesystem::rename(from_partition_synopsis,
                                  to_partition_synopsis, ec);
          if (ec) {
            TENZIR_WARN("failed to rename '{}' to '{}': {}",
                        from_partition_synopsis, to_partition_synopsis,
                        ec.message());
          }
        }
      }
      // TODO: This does not handle store files, which may already have been
      // written. Since a store file may also be written before the partition
      // itself, there does not currently seem to be a bulletproof way of
      // handling this.
      std::filesystem::remove_all(markersdir);
      return caf::none;
    }();
    if (error.valid()) {
      TENZIR_WARN("{} failed to finish leftover transforms: {}", *self, error);
    }
  }
  auto dir_iter = std::filesystem::directory_iterator(dir, err);
  if (err) {
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to list directory contents of "
                                       "{}: {}",
                                       dir, err.message()));
  }
  auto partition_ids = std::vector<uuid>{};
  auto oversized_partition_ids = std::vector<uuid>{};
  auto synopsis_files = std::vector<uuid>{};
  auto synopses = std::vector<partition_synopsis_pair>{};
  // Partition index file sizes captured during the scan so that the load below
  // does not need an additional stat per partition.
  auto partition_index_sizes = std::unordered_map<uuid, uint64_t>{};
  for (const auto& entry : dir_iter) {
    const auto stem = entry.path().stem();
    tenzir::uuid partition_uuid{};
    // Ignore files that don't use UUID for the filename.
    if (not parsers::uuid(stem.string(), partition_uuid)) {
      continue;
    }
    auto ext = entry.path().extension();
    if (ext.empty()) {
      auto size_err = std::error_code{};
      const auto file_size = entry.file_size(size_err);
      // Newer partitions are not limited to FLATBUFFERS_MAX_BUFFER_SIZE,
      // this is only a problem for older ones that still have `fbs::Partition`
      // as root type.
      if (not size_err and file_size >= FLATBUFFERS_MAX_BUFFER_SIZE
          and test_file_identifier(entry, fbs::PartitionIdentifier())) {
        auto store_path
          = dir / ".." / "archive" / fmt::format("{:u}.store", partition_uuid);
        if (std::filesystem::exists(store_path, err)) {
          oversized_partition_ids.push_back(partition_uuid);
        } else {
          TENZIR_WARN("{} did not find a store file for the oversized "
                      "partition {} and won't attempt to recover the data",
                      *self, partition_uuid);
        }
      } else {
        partition_ids.push_back(partition_uuid);
        partition_index_sizes.emplace(partition_uuid,
                                      size_err ? uint64_t{0} : file_size);
      }
    } else if (ext == std::filesystem::path{".mdx"}) {
      synopsis_files.push_back(partition_uuid);
    }
  }
  std::sort(partition_ids.begin(), partition_ids.end());
  std::sort(synopsis_files.begin(), synopsis_files.end());
  auto orphans = std::vector<uuid>{};
  std::set_difference(synopsis_files.begin(), synopsis_files.end(),
                      partition_ids.begin(), partition_ids.end(),
                      std::back_inserter(orphans));
  // Do a bit of housekeeping. MDX files without matching partitions shouldn't
  // be there in the first place.
  TENZIR_DEBUG("{} deletes {} orphaned mdx files", *self, orphans.size());
  for (auto& orphan : orphans) {
    std::filesystem::remove(dir / fmt::format("{}.mdx", orphan), err);
  }
  // We build an in-memory representation of the archive folder for quicker
  // lookup when we add file paths and sizes to the in-memory synopsis. Sizes
  // are captured here so the load below needs no additional stat per store.
  struct store_info {
    std::filesystem::path path = {};
    uint64_t size = 0;
  };
  const auto store_map = [&] {
    auto result = std::map<uuid, store_info>{};
    auto store_path = dir / ".." / "archive";
    if (not std::filesystem::is_directory(store_path, err)) {
      return result;
    }
    for (auto const& store_file :
         std::filesystem::directory_iterator{store_path}) {
      tenzir::uuid store_uuid{};
      if (not parsers::uuid(store_file.path().stem().string(), store_uuid)) {
        continue;
      }
      auto size_err = std::error_code{};
      const auto size = store_file.file_size(size_err);
      result.emplace(store_uuid, store_info{store_file.path(),
                                            size_err ? uint64_t{0} : size});
    }
    return result;
  }();
  // Resolve the base directories once instead of paying a `canonical()` (a
  // symlink-resolving `realpath`, i.e. several stats) per partition; the
  // per-partition file names are appended to the resolved base. This matters
  // on networked storage where each such call is a round-trip.
  auto resolve_dir
    = [](const std::filesystem::path& p) -> std::filesystem::path {
    std::error_code ec{};
    if (auto result = std::filesystem::canonical(p, ec); not ec) {
      return result;
    }
    ec.clear();
    if (auto result = std::filesystem::absolute(p, ec); not ec) {
      return result.lexically_normal();
    }
    return p.lexically_normal();
  };
  const auto index_dir = resolve_dir(dir);
  const auto synopsis_dir = resolve_dir(synopsisdir);
  const auto archive_dir = resolve_dir(dir / ".." / "archive");
  const auto lazy_sketches = synopsis_opts.lazy_sketches;
  const auto skip_verification = synopsis_opts.skip_synopsis_verification;
  // `synopsis_files` was scanned from `dir`, but synopses live under
  // `synopsisdir`. These are the same in the default configuration; when they
  // differ we must check the actual synopsis path instead of the scan result,
  // otherwise we would regenerate every synopsis on each startup.
  const auto synopsis_in_index_dir = synopsisdir == dir;
  // Loads a single partition synopsis from disk. This is invoked concurrently
  // from multiple worker threads below, so it must not touch shared mutable
  // state: it only reads data prepared above (all immutable during the load)
  // and the (post-initialization, immutable) synopsis factory, and produces a
  // fresh synopsis. Results are merged into the shared collections after all
  // workers have finished.
  auto load_one =
    [&](const uuid& partition_uuid) -> caf::expected<partition_synopsis_pair> {
    auto part_path = partition_path(partition_uuid);
    auto synopsis_path = partition_synopsis_path(partition_uuid);
    // Generate the external partition synopsis file if it doesn't exist. In the
    // common case the synopsis lives in the scanned index directory, so we can
    // use the scan result and avoid a stat; otherwise we check the actual path.
    // When verification is skipped on read, verify the freshly written synopsis.
    const auto synopsis_exists
      = synopsis_in_index_dir
          ? std::binary_search(synopsis_files.begin(), synopsis_files.end(),
                               partition_uuid)
          : std::filesystem::exists(synopsis_path);
    if (not synopsis_exists) {
      if (auto error = extract_partition_synopsis(part_path, synopsis_path,
                                                  skip_verification);
          error.valid()) {
        return error;
      }
    }
    TRY(auto chunk, chunk::mmap(synopsis_path));
    // Skipping verification avoids faulting in the entire buffer (including
    // sketch payloads that are never decoded); it is only safe because such
    // synopses are verified when written.
    auto maybe_flatbuffer
      = skip_verification
          ? flatbuffer<fbs::PartitionSynopsis>::make_unsafe(std::move(chunk))
          : flatbuffer<fbs::PartitionSynopsis>::make(std::move(chunk));
    if (not maybe_flatbuffer) {
      return std::move(maybe_flatbuffer.error());
    }
    const auto ps_flatbuffer = std::move(*maybe_flatbuffer);
    partition_synopsis_ptr ps = caf::make_copy_on_write<partition_synopsis>();
    if (ps_flatbuffer->partition_synopsis_type()
        != fbs::partition_synopsis::PartitionSynopsis::legacy) {
      return caf::make_error(ec::format_error, "invalid partition synopsis "
                                               "version");
    }
    TENZIR_ASSERT(ps_flatbuffer->partition_synopsis_as_legacy());
    const auto& synopsis_legacy
      = *ps_flatbuffer->partition_synopsis_as_legacy();
    if (auto error = unpack(synopsis_legacy, ps.unshared(), lazy_sketches);
        error.valid()) {
      return error;
    }
    // Attach file locations and sizes. Sizes were captured during the
    // directory scans above and URLs are built from the pre-resolved base
    // directories, so this needs no further filesystem access.
    if (const auto it = partition_index_sizes.find(partition_uuid);
        it != partition_index_sizes.end()) {
      ps.unshared().indexes_file = {
        .url
        = fmt::format("file://{}", (index_dir / part_path.filename()).string()),
        .size = it->second,
      };
    }
    ps.unshared().sketches_file = {
      .url = fmt::format("file://{}",
                         (synopsis_dir / synopsis_path.filename()).string()),
      .size = ps_flatbuffer.chunk()->size(),
    };
    auto f = store_map.find(partition_uuid);
    if (f == store_map.end()) {
      // For completeness sake we could open the partition and look if the
      // data is somewhere else entirely, but no known implementation ever
      // deviated from the default path scheme, so we assume filesystem
      // corruption here.
      return diagnostic::error(ec::no_such_file)
        .note("discarding partition {} due to a missing store file",
              partition_uuid)
        .to_error();
    }
    ps.unshared().store_file = {
      .url = fmt::format("file://{}",
                         (archive_dir / f->second.path.filename()).string()),
      .size = f->second.size,
    };
    return partition_synopsis_pair{partition_uuid, std::move(ps)};
  };
  // Load the partitions concurrently. Each partition is independent, so we
  // distribute them across a pool of worker threads using a shared atomic
  // cursor. This is particularly effective on networked storage (e.g. NFS),
  // where loading is dominated by I/O latency that overlaps across requests.
  // The index actor is detached and blocked here during startup, and no other
  // actor interacts with this state yet, so using plain threads is safe.
  const auto num_partitions = partition_ids.size();
  auto concurrency = synopsis_opts.load_concurrency;
  if (concurrency == 0) {
    concurrency = std::max<size_t>(1, std::thread::hardware_concurrency());
  }
  concurrency
    = std::min<size_t>(concurrency, std::max<size_t>(1, num_partitions));
  // Report progress for large loads so operators can see startup advancing;
  // stay quiet for small ones to avoid log noise.
  const auto report_progress = num_partitions >= size_t{1000};
  const auto progress_step = std::max<size_t>(1, num_partitions / 20);
  const auto lazy_suffix = lazy_sketches
                             ? std::string{" deferring Bloom-filter sketches;"}
                             : std::string{};
  const auto verify_suffix = skip_verification
                               ? std::string{" skipping verification;"}
                               : std::string{};
  if (report_progress) {
    TENZIR_INFO("{} loads {} partition synopses using {} thread(s);{}{}", *self,
                num_partitions, concurrency, lazy_suffix, verify_suffix);
  } else {
    TENZIR_VERBOSE("{} loads {} partition synopses using {} thread(s);{}{}",
                   *self, num_partitions, concurrency, lazy_suffix,
                   verify_suffix);
  }
  const auto load_start = std::chrono::steady_clock::now();
  auto worker_synopses
    = std::vector<std::vector<partition_synopsis_pair>>(concurrency);
  std::atomic<size_t> next_index{0};
  std::atomic<size_t> loaded{0};
  auto work = [&](size_t worker) {
    for (auto idx = next_index.fetch_add(1, std::memory_order_relaxed);
         idx < num_partitions;
         idx = next_index.fetch_add(1, std::memory_order_relaxed)) {
      const auto& partition_uuid = partition_ids[idx];
      try {
        auto result = load_one(partition_uuid);
        if (not result) {
          TENZIR_VERBOSE("{} failed to load partition {}: {}", *self,
                         partition_uuid, result.error());
          continue;
        }
        worker_synopses[worker].push_back(std::move(*result));
      } catch (const std::exception& ex) {
        TENZIR_VERBOSE("{} failed to load partition {}: {}", *self,
                       partition_uuid, ex.what());
        continue;
      }
      if (const auto n = loaded.fetch_add(1, std::memory_order_relaxed) + 1;
          report_progress and n % progress_step == 0) {
        TENZIR_INFO("{} loaded {}/{} partition synopses ({}%)", *self, n,
                    num_partitions, n * 100 / num_partitions);
      }
    }
  };
  if (concurrency <= 1) {
    work(0);
  } else {
    auto threads = std::vector<std::thread>{};
    threads.reserve(concurrency);
    for (size_t worker = 0; worker < concurrency; ++worker) {
      threads.emplace_back(work, worker);
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }
  for (auto& batch : worker_synopses) {
    for (auto& pair : batch) {
      persisted_partitions.emplace(pair.uuid);
      synopses.push_back(std::move(pair));
    }
  }
  const auto load_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - load_start)
                         .count();
  if (report_progress) {
    TENZIR_INFO("{} loaded {} partition synopses in {} ms", *self,
                synopses.size(), load_ms);
  } else {
    TENZIR_VERBOSE("{} loaded {} partition synopses in {} ms", *self,
                   synopses.size(), load_ms);
  }
  // Recommend the user to run 'tenzir-ctl rebuild' if any partition syopses
  // are outdated. We need to nudge them a bit so we can drop support for older
  // partition versions more freely.
  const auto num_outdated = std::ranges::count_if(synopses, [](const auto& x) {
    return x.synopsis->version < version::current_partition_version;
  });
  if (num_outdated > 0) {
    TENZIR_WARN("{} detected {}/{} outdated partitions; consider running "
                "'tenzir-ctl "
                "rebuild' to upgrade existing partitions in the background",
                *self, num_outdated, synopses.size());
  }
  // We collect all synopses to send them in bulk, since the `await` interface
  // doesn't lend itself to a huge number of awaited messages: Only the tip of
  // the current awaited list is considered, leading to an O(n**2) worst-case
  // behavior if the responses arrive in the same order to how they were sent.
  TENZIR_DEBUG("{} requesting bulk merge of {} partitions", *self,
               synopses.size());
  self->mail(atom::start_v, std::move(synopses))
    .request(catalog, caf::infinite)
    .then(
      [this](atom::ok) {
        TENZIR_VERBOSE(
          "{} finished initializing and is ready to accept queries", *self);
      },
      [this](caf::error& err) {
        TENZIR_ERROR("{} failed to load catalog state from disk: {}", *self,
                     err);
        self->send_exit(self, std::move(err));
      });
  return caf::none;
}

/// Persists the state to disk.
void index_state::flush_to_disk() {
  auto builder = flatbuffers::FlatBufferBuilder{};
  auto index = pack(builder, *this);
  if (not index) {
    TENZIR_WARN("{} failed to pack index: {}", *self, index.error());
    return;
  }
  auto chunk = fbs::release(builder);
  self->mail(atom::write_v, index_filename(), chunk)
    .request(filesystem, caf::infinite)
    .then(
      [this](atom::ok) {
        TENZIR_DEBUG("{} successfully persisted index state", *self);
      },
      [this](const caf::error& err) {
        TENZIR_WARN("{} failed to persist index state: {}", *self, render(err));
      });
}

// -- inbound path -----------------------------------------------------------

void index_state::handle_slice(table_slice x) {
  const auto& schema = x.schema();
  auto active_partition = active_partitions.find(schema);
  if (active_partition == active_partitions.end()) {
    auto part = create_active_partition(schema);
    if (not part) {
      self->quit(caf::make_error(ec::logic_error,
                                 fmt::format("{} failed to create active "
                                             "partition: {}",
                                             *self, part.error())));
      return;
    }
    active_partition = *part;
  } else if (active_partition->second.events + x.rows() > partition_capacity) {
    TENZIR_TRACE("{} flushes active partition {} with {}/{} events due to {} "
                 "incoming events",
                 *self, schema, active_partition->second.events,
                 partition_capacity, x.rows());
    decommission_active_partition(schema, {});
    flush_to_disk();
    auto part = create_active_partition(schema);
    if (not part) {
      self->quit(caf::make_error(ec::logic_error,
                                 fmt::format("{} failed to create active "
                                             "partition: {}",
                                             *self, part.error())));
      return;
    }
    active_partition = *part;
  }
  TENZIR_ASSERT(active_partition->second.actor);
  buffered_events += x.rows();
  active_partition->second.events += x.rows();
  self->mail(x).send(active_partition->second.actor);
  // Flush the partition that was written to if it exceeds the capacity. We
  // already check above whether the write would exceed the capacity and then
  // flush ahead of time, but this can still happen if the single table slice we
  // just got already exceeds it.
  if (active_partition->second.events >= partition_capacity) {
    TENZIR_TRACE("{} flushes active partition {} with {}/{} events due to {} "
                 "incoming events directly exceeding capacity",
                 *self, schema, active_partition->second.events,
                 partition_capacity, x.rows());
    decommission_active_partition(schema, {});
    flush_to_disk();
  }
  // When the total number of events in active partitions exceeds the configured
  // limit, we flush the largest partition repeatedly until we are below it.
  // Note that this might be suboptimal in some cases, for example if the limit
  // is 1000, we have 9 partitions with 100 events, and the final partition is
  // the only one being written to. We then always flush it, limiting it to 100
  // events at a time. Another strategy to try here would be LRU, but that could
  // potentially require flushing many smaller partitions before dropping below
  // the limit.
  while (buffered_events > max_buffered_events) {
    TENZIR_ASSERT(not active_partitions.empty());
    auto max = std::ranges::max_element(active_partitions, std::less<>{},
                                        [](auto& entry) {
                                          return entry.second.events;
                                        });
    TENZIR_VERBOSE("{} flushes active partition {} with {}/{} events due to "
                   "{}/{} buffered events",
                   *self, max->first, max->second.events, partition_capacity,
                   buffered_events, max_buffered_events);
    decommission_active_partition(max->first, {});
    flush_to_disk();
  }
}

// -- partition handling -----------------------------------------------------

caf::expected<std::unordered_map<type, active_partition_info>::iterator>
index_state::create_active_partition(const type& schema) {
  TENZIR_ASSERT(taxonomies);
  TENZIR_ASSERT(schema);
  auto id = uuid::random();
  const auto [active_partition, inserted]
    = active_partitions.emplace(schema, active_partition_info{});
  TENZIR_ASSERT(inserted);
  TENZIR_ASSERT(active_partition != active_partitions.end());
  active_partition->second.actor
    = self->spawn(::tenzir::active_partition, schema, id, filesystem,
                  index_opts, synopsis_opts, store_actor_plugin, taxonomies);
  active_partition->second.id = id;
  detail::weak_run_delayed(self, active_partition_timeout, [schema, id, this] {
    const auto it = active_partitions.find(schema);
    if (it == active_partitions.end() or it->second.id != id) {
      // If the partition was already rotated then there's nothing to do for us.
      return;
    }
    TENZIR_TRACE("{} flushes active partition {} with {}/{} {} events "
                 "after {} timeout",
                 *self, it->second.id, it->second.events, partition_capacity,
                 schema, data{active_partition_timeout});
    decommission_active_partition(schema, [this, schema,
                                           id](const caf::error& err) mutable {
      if (err.valid()) {
        TENZIR_WARN("{} failed to flush active partition {} ({}) after {} "
                    "timeout: {}",
                    *self, id, schema, data{active_partition_timeout}, err);
      }
    });
    flush_to_disk();
  });
  TENZIR_TRACE("{} created new partition {}", *self, id);
  return active_partition;
}

void index_state::decommission_active_partition(
  type schema, std::function<void(const caf::error&)> completion) {
  // We need to take `schema` by value here because it could be derived from the
  // key in the map which we are now going to erase.
  const auto active_partition = active_partitions.find(schema);
  TENZIR_ASSERT(active_partition != active_partitions.end());
  TENZIR_ASSERT(buffered_events >= active_partition->second.events);
  buffered_events -= active_partition->second.events;
  const auto id = active_partition->second.id;
  const auto actor = std::exchange(active_partition->second.actor, {});
  const auto type = active_partition->first;
  // Move the active partition to the list of unpersisted partitions.
  TENZIR_ASSERT_EXPENSIVE(not unpersisted.contains(id));
  unpersisted.emplace(id, unpersisted_partition_info{
                            .schema = type,
                            .actor = actor,
                            .ref_count = 1,
                            .visible_for_recent = true,
                            .exit_sent = false,
                            .exit_reason = caf::none,
                          });
  active_partitions.erase(active_partition);
  // Persist active partition asynchronously.
  const auto part_path = partition_path(id);
  const auto synopsis_path = partition_synopsis_path(id);
  TENZIR_TRACE("{} persists active partition {} to {}", *self, schema,
               part_path);
  self->mail(atom::persist_v, part_path, synopsis_path)
    .request(actor, caf::infinite)
    .then(
      [=, this](partition_synopsis_ptr& ps) {
        TENZIR_TRACE("{} successfully persisted partition {} {}", *self, schema,
                     id);
        // The catalog expects to own the partition synopsis it receives,
        // so we make a copy for the listeners.
        // TODO: We should skip this continuation if we're currently shutting
        // down.
        auto apsv = std::vector<partition_synopsis_pair>{{id, ps}};
        self->mail(atom::merge_v, std::move(apsv))
          .request(catalog, caf::infinite)
          .then(
            [=, this](atom::ok) {
              TENZIR_TRACE("{} inserted partition {} {} to the catalog", *self,
                           schema, id);
              for (auto& listener : partition_creation_listeners) {
                self->mail(atom::update_v, partition_synopsis_pair{id, ps})
                  .send(listener);
              }
              persisted_partitions.emplace(id);
              retire_partition(id, caf::exit_reason::normal);
              if (completion) {
                completion(caf::none);
              }
            },
            [=, this](const caf::error& err) {
              TENZIR_ERROR("{} failed to commit partition {} {} to the "
                           "catalog, "
                           "the contained data will not be available for "
                           "queries: {}",
                           *self, schema, id, err);
              retire_partition(id, err);
              if (completion) {
                completion(err);
              }
            });
      },
      [=, this](caf::error& err) {
        TENZIR_ERROR("{} failed to persist partition {} {} and evicts data "
                     "from "
                     "memory to preserve process integrity: {}",
                     *self, schema, id, err);
        retire_partition(id, err);
        if (completion) {
          completion(err);
        }
      });
}

auto index_state::flush() -> caf::typed_response_promise<void> {
  // If we've got nothing to flush we can just exit immediately.
  auto rp = self->make_response_promise<void>();
  if (active_partitions.empty()) {
    rp.deliver();
    return rp;
  }
  auto counter = detail::make_fanout_counter(
    active_partitions.size(),
    [rp]() mutable {
      rp.deliver();
    },
    [rp](caf::error error) mutable {
      rp.deliver(std::move(error));
    });
  // We gather the schemas first before we call decomission active partition
  // on every active partition to avoid iterator invalidation.
  auto schemas = std::vector<type>{};
  schemas.reserve(active_partitions.size());
  for (const auto& [schema, _] : active_partitions) {
    schemas.push_back(schema);
  }
  for (const auto& schema : schemas) {
    decommission_active_partition(schema,
                                  [counter](const caf::error& err) mutable {
                                    if (err.valid()) {
                                      counter->receive_error(err);
                                    } else {
                                      counter->receive_success();
                                    }
                                  });
  }
  return rp;
}

void index_state::pin_recent_partition(const uuid& id) {
  const auto it = unpersisted.find(id);
  TENZIR_ASSERT(it != unpersisted.end());
  TENZIR_ASSERT(it->second.visible_for_recent);
  ++it->second.ref_count;
}

void index_state::unpin_recent_partition(const uuid& id) {
  const auto it = unpersisted.find(id);
  TENZIR_ASSERT(it != unpersisted.end());
  TENZIR_ASSERT(it->second.ref_count > 0);
  --it->second.ref_count;
  if (it->second.ref_count != 0) {
    return;
  }
  if (not it->second.exit_sent) {
    if (it->second.exit_reason.valid()) {
      self->send_exit(it->second.actor, it->second.exit_reason);
    } else {
      self->send_exit(it->second.actor, caf::exit_reason::normal);
    }
  }
  unpersisted.erase(it);
}

void index_state::retire_partition(const uuid& id, caf::error reason) {
  const auto it = unpersisted.find(id);
  TENZIR_ASSERT(it != unpersisted.end());
  it->second.visible_for_recent = false;
  it->second.exit_reason = std::move(reason);
  TENZIR_ASSERT(it->second.ref_count > 0);
  --it->second.ref_count;
  if (it->second.ref_count != 0) {
    return;
  }
  if (not it->second.exit_sent) {
    if (it->second.exit_reason.valid()) {
      self->send_exit(it->second.actor, it->second.exit_reason);
    } else {
      self->send_exit(it->second.actor, caf::exit_reason::normal);
    }
  }
  unpersisted.erase(it);
}

void index_state::drain_retired_partitions(caf::error reason) {
  for (auto& [_, partition] : unpersisted) {
    partition.visible_for_recent = false;
    auto shutdown_reason = reason.valid() ? reason : partition.exit_reason;
    if (shutdown_reason.valid()) {
      self->send_exit(partition.actor, shutdown_reason);
      partition.exit_reason = shutdown_reason;
    } else {
      self->send_exit(partition.actor, caf::exit_reason::normal);
      partition.exit_reason = caf::exit_reason::normal;
    }
    partition.exit_sent = true;
  }
}

void index_state::add_partition_creation_listener(
  partition_creation_listener_actor listener) {
  partition_creation_listeners.push_back(listener);
}

// -- query handling ---------------------------------------------------------

auto index_state::schedule_lookups() -> size_t {
  if (not pending_queries.has_work()) {
    return 0u;
  }
  const size_t previous_partition_lookups = running_partition_lookups;
  while (running_partition_lookups < max_concurrent_partition_lookups) {
    // 1. Get the partition with the highest accumulated priority.
    auto next = pending_queries.next();
    if (not next) {
      TENZIR_TRACE("{} did not find a partition to query", *self);
      break;
    }
    auto immediate_completion = [&](const query_queue::entry& x) {
      for (auto qid : x.queries) {
        if (auto client = pending_queries.handle_completion(qid)) {
          TENZIR_TRACE("{} completes query {} immediately", *self, qid);
          self->mail(atom::done_v).send(*client);
        }
      }
    };
    if (next->erased) {
      TENZIR_VERBOSE("{} skips erased partition {}", *self, next->partition);
      immediate_completion(*next);
      continue;
    }
    if (next->queries.empty()) {
      TENZIR_VERBOSE("{} skips partition {} because it has no scheduled "
                     "queries",
                     *self, next->partition);
      continue;
    }
    TENZIR_TRACE("{} schedules partition {} for {}", *self, next->partition,
                 next->queries);
    // 2. Acquire the actor for the selected partition, potentially materializing
    //    it from its persisted state.
    auto acquire = [&](const uuid& partition_id) -> partition_actor {
      // We need to first check whether the ID is the active partition or one
      // of our unpersisted ones. Only then can we dispatch to our LRU cache.
      partition_actor part;
      tenzir::type partition_type{};
      for (const auto& [type, active_partition] : active_partitions) {
        if (active_partition.actor != nullptr
            and active_partition.id == partition_id) {
          part = active_partition.actor;
          break;
        }
      }
      if (not part) {
        if (auto it = unpersisted.find(partition_id); it != unpersisted.end()) {
          part = it->second.actor;
        } else if (auto it = persisted_partitions.find(partition_id);
                   it != persisted_partitions.end()) {
          part = inmem_partitions.get_or_load(partition_id);
        }
      }
      if (not part) {
        TENZIR_WARN("{} failed to load partition {} that was part of a query",
                    *self, partition_id);
      }
      return part;
    };
    auto partition_actor = acquire(next->partition);
    if (not partition_actor) {
      // We need to mark failed partitions as completed to avoid clients going
      // out of sync.
      immediate_completion(*next);
      continue;
    }
    // 3. request all relevant queries in a loop
    auto ts = std::chrono::system_clock::now();
    auto active_lookup_id = active_lookup_counter++;
    active_lookups.emplace_back(active_lookup_id, ts, *next);
    auto active_lookup = active_lookups.end() - 1;
    for (auto qid : next->queries) {
      auto it = pending_queries.queries().find(qid);
      if (it == pending_queries.queries().end()) {
        TENZIR_WARN("{} tried to access non-existent query {}", *self, qid);
        auto& qs = std::get<2>(*active_lookup).queries;
        qs.erase(std::remove(qs.begin(), qs.end(), qid), qs.end());
        if (qs.empty()) {
          --running_partition_lookups;
          active_lookups.erase(active_lookup);
        }
        continue;
      }
      auto handle_completion = [active_lookup_id, qid, this] {
        if (auto client = pending_queries.handle_completion(qid)) {
          self->mail(atom::done_v).send(*client);
        }
        // 4. recursively call schedule_lookups in the done handler. ...or
        //    when all done? (5)
        // 5. decrement running_partition_lookups when all queries that
        //    were started are done. Keep track in the closure.
        auto active_lookup = std::find_if(
          active_lookups.begin(), active_lookups.end(), [&](const auto& entry) {
            return std::get<0>(entry) == active_lookup_id;
          });
        TENZIR_ASSERT(active_lookup != active_lookups.end());
        auto& qs = std::get<2>(*active_lookup).queries;
        qs.erase(std::remove(qs.begin(), qs.end(), qid), qs.end());
        if (qs.empty()) {
          --running_partition_lookups;
          active_lookups.erase(active_lookup);
          const auto num_scheduled = schedule_lookups();
          TENZIR_TRACE("{} scheduled {} partitions after completion of a "
                       "previously scheduled lookup",
                       *self, num_scheduled);
        }
      };
      const auto& context_it
        = it->second.query_contexts_per_type.find(next->schema);
      if (context_it == it->second.query_contexts_per_type.end()) {
        TENZIR_WARN("{} failed to evaluate query {} for partition {}: query "
                    "context for schema is already unvailable",
                    *self, qid, next->partition);
        inmem_partitions.drop(next->partition);
        handle_completion();
        continue;
      }
      self->mail(atom::query_v, context_it->second)
        .request(partition_actor, defaults::scheduler_timeout)
        .then(
          [this, handle_completion, qid, pid = next->partition](uint64_t n) {
            TENZIR_TRACE("{} received {} results for query {} from partition "
                         "{}",
                         *self, n, qid, pid);
            handle_completion();
          },
          [this, handle_completion, qid,
           pid = next->partition](const caf::error& err) {
            TENZIR_WARN("{} failed to evaluate query {} for partition {}: {}",
                        *self, qid, pid, err);
            // We don't know if this was a transient error or if the
            // partition/store is corrupted. However, the partition actor has
            // possibly already exited so at least we have to clear it from
            // the cache so that subsequent queries get a chance to respawn it
            // cleanly instead of trying to talk to the dead.
            inmem_partitions.drop(pid);
            handle_completion();
          });
    }
    running_partition_lookups++;
  }
  TENZIR_ASSERT(running_partition_lookups >= previous_partition_lookups);
  return running_partition_lookups - previous_partition_lookups;
}

// -- introspection ----------------------------------------------------------

std::size_t index_state::memusage() const {
  auto calculate_usage = []<class T>(const T& collection) -> std::size_t {
    return collection.size() * sizeof(typename T::value_type);
  };
  auto usage = std::size_t{sizeof(*this)};
  for (const auto& [type, partition_info] : active_partitions) {
    usage += as_bytes(type).size() + sizeof(partition_info);
  }
  for (const auto& [id, partition] : unpersisted) {
    usage += sizeof(id) + as_bytes(partition.schema).size() + sizeof(partition);
  }
  usage += persisted_partitions.size()
           * sizeof(decltype(persisted_partitions)::value_type);
  usage += pending_queries.memusage();
  usage += calculate_usage(flush_listeners);
  usage += calculate_usage(partition_creation_listeners);
  usage += calculate_usage(partitions_in_transformation);
  return usage;
}

index_actor::behavior_type
index(index_actor::stateful_pointer<index_state> self,
      filesystem_actor filesystem, catalog_actor catalog,
      const std::filesystem::path& dir, std::string store_backend,
      size_t max_buffered_events, size_t partition_capacity,
      duration active_partition_timeout, size_t max_inmem_partitions,
      size_t max_concurrent_partition_lookups,
      const std::filesystem::path& catalog_dir, index_config index_config) {
  TENZIR_TRACE("index {} {} {} {} {} {} {} {} {}", TENZIR_ARG(self->id()),
               TENZIR_ARG(filesystem), TENZIR_ARG(dir),
               TENZIR_ARG(partition_capacity),
               TENZIR_ARG(active_partition_timeout),
               TENZIR_ARG(max_inmem_partitions),
               TENZIR_ARG(max_concurrent_partition_lookups),
               TENZIR_ARG(catalog_dir), TENZIR_ARG(index_config));
  if (self->getf(caf::scheduled_actor::is_detached_flag)) {
    caf::detail::set_thread_name("tnz.index");
  }
  TENZIR_VERBOSE("{} initializes index in {} with a maximum partition "
                 "size of {} events and {} resident partitions",
                 *self, dir, partition_capacity, max_inmem_partitions);
  self->state().index_opts["cardinality"] = partition_capacity;
  self->state().synopsis_opts = std::move(index_config);
  if (dir != catalog_dir) {
    TENZIR_VERBOSE("{} uses {} for catalog data", *self, catalog_dir);
  }
  // Set members.
  self->state().self = self;
  self->state().max_concurrent_partition_lookups
    = max_concurrent_partition_lookups;
  self->state().store_actor_plugin
    = plugins::find<store_actor_plugin>(store_backend);
  if (not self->state().store_actor_plugin) {
    auto error = caf::make_error(ec::invalid_configuration,
                                 fmt::format("could not find "
                                             "store plugin '{}'",
                                             store_backend));
    TENZIR_ERROR("{}", render(error));
    self->quit(error);
    return index_actor::behavior_type::make_empty_behavior();
  }
  self->state().filesystem = std::move(filesystem);
  self->state().catalog = std::move(catalog);
  self->state().taxonomies = std::make_shared<tenzir::taxonomies>();
  self->state().taxonomies->concepts = modules::concepts();
  self->state().dir = dir;
  self->state().synopsisdir = catalog_dir;
  self->state().markersdir = dir / "markers";
  self->state().partition_capacity = partition_capacity;
  self->state().max_buffered_events = max_buffered_events;
  self->state().active_partition_timeout = active_partition_timeout;
  self->state().inmem_partitions.factory().filesystem()
    = self->state().filesystem;
  self->state().inmem_partitions.resize(max_inmem_partitions);
  // Read persistent state.
  if (auto err = self->state().load_from_disk(); err.valid()) {
    TENZIR_ERROR("{} failed to load index state from disk: {}", *self,
                 render(err));
    self->quit(err);
    return index_actor::behavior_type::make_empty_behavior();
  }
  detail::weak_run_delayed_loop(
    self, defaults::metrics_interval,
    [self, actor_metrics_builder
           = detail::make_actor_metrics_builder()]() mutable {
      const auto importer
        = self->system().registry().get<importer_actor>("tenzir.importer");
      // There exists a very unlikely scenario where the importer was not
      // spawned within the metrics interval after the index was spawned. The
      // importer requires a handle to the index on startup, and the index
      // needs a handle to the index for forwarding metrics, so we cannot just
      // reverse the startup order here. Instead, we just delay the first
      // metrics until the importer is ready.
      if (not importer) [[unlikely]] {
        return;
      }
      self->mail(detail::generate_actor_metrics(actor_metrics_builder, self))
        .send(importer);
    });
  return {
    [self](atom::done, uuid partition_id) {
      TENZIR_TRACE("{} queried partition {} successfully", *self, partition_id);
    },
    [self](table_slice& slice) {
      self->state().handle_slice(std::move(slice));
    },
    [self](atom::subscribe, atom::create,
           const partition_creation_listener_actor& listener,
           send_initial_dbstate should_send) {
      TENZIR_DEBUG("{} adds partition creation listener", *self);
      self->state().add_partition_creation_listener(listener);
      if (should_send == send_initial_dbstate::no) {
        return;
      }
      // When we get here, the initial bulk upgrade and any table slices
      // finished since then have already been sent to the catalog, and
      // since CAF guarantees message order within the same inbound queue
      // they will all be part of the response vector.
      self->mail(atom::get_v)
        .request(self->state().catalog, caf::infinite)
        .then(
          [=](std::vector<partition_synopsis_pair>& v) {
            self->mail(atom::update_v, std::move(v)).send(listener);
          },
          [](const caf::error& e) {
            TENZIR_WARN(
              "index failed to get list of partitions from catalog: {}", e);
          });
    },
    [self](atom::get, bool internal) -> caf::result<std::vector<table_slice>> {
      auto rp = self->make_response_promise<std::vector<table_slice>>();
      auto result = std::make_shared<std::vector<table_slice>>();
      auto pending = std::make_shared<size_t>(0);
      auto first_error = std::make_shared<caf::error>();
      auto pinned_partitions = std::make_shared<std::vector<uuid>>();
      auto finish = [result, pending, rp, first_error, pinned_partitions,
                     &state = self->state()]() mutable {
        if (*pending != 0) {
          return;
        }
        for (const auto& id : *pinned_partitions) {
          state.unpin_recent_partition(id);
        }
        pinned_partitions->clear();
        if (first_error->valid()) {
          rp.deliver(*first_error);
          return;
        }
        rp.deliver(std::move(*result));
      };
      auto track_unpersisted_partition
        = [pending, pinned_partitions, &state = self->state()](const uuid& id) {
            state.pin_recent_partition(id);
            pinned_partitions->push_back(id);
            *pending += 1;
          };
      // Collect from active partitions.
      for (const auto& [schema, info] : self->state().active_partitions) {
        if (schema.attribute("internal").has_value() != internal) {
          continue;
        }
        *pending += 1;
        self->mail(atom::get_v)
          .request(info.actor, caf::infinite)
          .then(
            [result, pending,
             finish](std::vector<table_slice>& slices) mutable {
              result->insert(result->end(),
                             std::make_move_iterator(slices.begin()),
                             std::make_move_iterator(slices.end()));
              *pending -= 1;
              finish();
            },
            [pending, first_error, finish](const caf::error& err) mutable {
              if (not first_error->valid()) {
                *first_error = err;
              }
              *pending -= 1;
              finish();
            });
      }
      // Collect from unpersisted partitions (being written to disk).
      for (const auto& [uuid, partition] : self->state().unpersisted) {
        if (not partition.visible_for_recent
            or partition.schema.attribute("internal").has_value() != internal) {
          continue;
        }
        track_unpersisted_partition(uuid);
        self->mail(atom::get_v)
          .request(partition.actor, caf::infinite)
          .then(
            [result, pending,
             finish](std::vector<table_slice>& slices) mutable {
              result->insert(result->end(),
                             std::make_move_iterator(slices.begin()),
                             std::make_move_iterator(slices.end()));
              *pending -= 1;
              finish();
            },
            [pending, first_error, finish](const caf::error& err) mutable {
              if (not first_error->valid()) {
                *first_error = err;
              }
              *pending -= 1;
              finish();
            });
      }
      // If no partitions, return immediately
      finish();
      return rp;
    },
    [self](atom::resolve,
           tenzir::expression& expr) -> caf::result<catalog_lookup_result> {
      auto query_context = query_context::make_extract("index", self, expr);
      query_context.id = tenzir::uuid::random();
      return self->mail(atom::candidates_v, std::move(query_context))
        .delegate(self->state().catalog);
    },
    [self](atom::erase, uuid partition_id) -> caf::result<atom::done> {
      TENZIR_VERBOSE("{} erases partition {}", *self, partition_id);
      auto rp = self->make_response_promise<atom::done>();
      auto path = self->state().partition_path(partition_id);
      auto synopsis_path = self->state().partition_synopsis_path(partition_id);
      if (not self->state().persisted_partitions.contains(partition_id)) {
        std::error_code err{};
        const auto file_exists = std::filesystem::exists(path, err);
        if (not file_exists) {
          rp.deliver(caf::make_error(
            ec::logic_error, fmt::format("unknown partition for path {}: {}",
                                         path, err.message())));
          return rp;
        }
      }
      self->mail(atom::erase_v, partition_id)
        .urgent()
        .request(self->state().catalog, caf::infinite)
        .then(
          [self, partition_id, path, synopsis_path, rp](atom::ok) mutable {
            TENZIR_DEBUG("{} erased partition {} from catalog", *self,
                         partition_id);
            self->state().persisted_partitions.erase(partition_id);
            // We don't remove the partition from the queue directly because the
            // query API requires clients to keep track of the number of
            // candidate partitions. Removing the partition from the queue
            // would require us to update the partition counters in the query
            // states and the client would go out of sync. That would require
            // the index to deal with a few complicated corner cases.
            self->state().pending_queries.mark_partition_erased(partition_id);
            // Remove the synopsis file. We can already safely do so because
            // the catalog acked the erase.
            self->mail(atom::erase_v, synopsis_path)
              .urgent()
              .request(self->state().filesystem, caf::infinite)
              .then(
                [self, partition_id](atom::done) {
                  TENZIR_TRACE("{} erased partition synopsis {} from "
                               "filesystem",
                               *self, partition_id);
                },
                [self, partition_id, synopsis_path](const caf::error& err) {
                  TENZIR_WARN("{} failed to erase partition "
                              "synopsis {} at {}: {}",
                              *self, partition_id, synopsis_path, err);
                });
            // A helper function to erase the dense index file with some
            // logging.
            auto erase_dense_index_file = [=] {
              self->mail(atom::erase_v, path)
                .urgent()
                .request(self->state().filesystem, caf::infinite)
                .then(
                  [self, partition_id](atom::done) {
                    TENZIR_TRACE("{} erased partition {} from filesystem",
                                 *self, partition_id);
                  },
                  [self, partition_id, path](const caf::error& err) {
                    TENZIR_WARN("{} failed to erase partition {} at {}: {}",
                                *self, partition_id, path, err);
                  });
            };
            auto store_path = store_path_for_partition(self->state().dir / "..",
                                                       partition_id);
            if (store_path) {
              erase_dense_index_file();
              rp.delegate(self->state().filesystem, atom::erase_v, *store_path);
              return;
            }
            // Fallback path: In case the store file is not found
            // at the expected path we need to load the partition
            // and retrieve the correct path from the store header.
            TENZIR_DEBUG("{} did not find a store for partition {}, inspecting "
                         "the store header",
                         *self, partition_id);
            self->mail(atom::mmap_v, path)
              .urgent()
              .request(self->state().filesystem, caf::infinite)
              .then(
                [=](const chunk_ptr& chunk) mutable {
                  TENZIR_DEBUG("{} mmapped partition {} to extract store path "
                               "for erasure",
                               *self, partition_id);
                  using flatbuffers::uoffset_t;
                  using flatbuffers::soffset_t;
                  if (not chunk
                      or chunk->size() < FLATBUFFERS_MIN_BUFFER_SIZE) {
                    erase_dense_index_file();
                    rp.deliver(caf::make_error( //
                      ec::filesystem_error,
                      fmt::format("failed to load the state for "
                                  "partition {}",
                                  path)));
                    return;
                  }
                  if (chunk->size() >= FLATBUFFERS_MAX_BUFFER_SIZE
                      and flatbuffers::BufferHasIdentifier(
                        chunk->data(), fbs::PartitionIdentifier())) {
                    TENZIR_WARN("failed to load partition for deletion at {} "
                                "because its size of {} exceeds the maximum "
                                "allowed size of {}. The index statistics will "
                                "be incorrect until the database has been "
                                "rebuilt and restarted",
                                path, chunk->size(),
                                FLATBUFFERS_MAX_BUFFER_SIZE);
                    erase_dense_index_file();
                    rp.deliver(caf::make_error(ec::filesystem_error,
                                               "aborting erasure due to "
                                               "encountering a legacy "
                                               "oversized partition"));
                    return;
                  }
                  // TODO: We could send `all_ids` as the second
                  // argument here, which doesn't really make sense
                  // from an interface perspective but would save the
                  // partition from recomputing the same bitmap.
                  auto partition_actor
                    = self->state().inmem_partitions.eject(partition_id);
                  rp.delegate(partition_actor, atom::erase_v);
                },
                [=](caf::error& err) mutable {
                  TENZIR_WARN("{} failed to load partition {} for erase "
                              "fallback "
                              "path: {}",
                              *self, partition_id, err);
                  erase_dense_index_file();
                  rp.deliver(std::move(err));
                });
          },
          [self, partition_id, rp](caf::error& err) mutable {
            TENZIR_WARN("{} failed to erase partition {} from catalog: {}",
                        *self, partition_id, err);
            rp.deliver(std::move(err));
          });
      return rp;
    },
    [self](atom::erase,
           const std::vector<uuid>& partition_ids) -> caf::result<atom::done> {
      // TODO: It would probably be more efficient to implement the
      // handler for multiple ids directly as opposed to dispatching
      // onto the single-id erase handler.
      auto rp = self->make_response_promise<atom::done>();
      auto fanout_counter = detail::make_fanout_counter(
        partition_ids.size(),
        [rp]() mutable {
          rp.deliver(atom::done_v);
        },
        [rp](caf::error&& e) mutable {
          rp.deliver(std::move(e));
        });
      for (auto const& id : partition_ids) {
        self->mail(atom::erase_v, id)
          .request(static_cast<index_actor>(self), caf::infinite)
          .then(
            [=](atom::done) {
              fanout_counter->receive_success();
            },
            [=](caf::error& e) {
              fanout_counter->receive_error(std::move(e));
            });
      }
      return rp;
    },
    [self](atom::apply, ast::pipeline pipe,
           std::vector<partition_info> selected_partitions,
           keep_original_partition keep,
           std::string origin) -> caf::result<partition_apply_result> {
      if (selected_partitions.empty()) {
        return caf::make_error(ec::invalid_argument, "no partitions given");
      }
      TENZIR_DEBUG("{} applies a pipeline to partitions {}", *self,
                   selected_partitions);
      TENZIR_ASSERT(self->state().store_actor_plugin);
      auto input_partitions = std::vector<partition_info>{};
      input_partitions.reserve(selected_partitions.size());
      std::erase_if(selected_partitions, [&](const auto& entry) {
        if (self->state().persisted_partitions.contains(entry.uuid)) {
          return false;
        }
        TENZIR_WARN("{} skips unknown partition {} for pipeline {:?}", *self,
                    entry.uuid, pipe);
        return true;
      });
      auto corrected_partitions = catalog_lookup_result{};
      for (const auto& partition : selected_partitions) {
        if (self->state()
              .partitions_in_transformation.insert(partition.uuid)
              .second) {
          corrected_partitions.candidate_infos[partition.schema]
            .partition_infos.emplace_back(partition);
          input_partitions.emplace_back(partition);
        } else {
          // Getting overlapping partitions triggers a warning, and we
          // silently ignore the partition at the cost of the transformation
          // being less efficient.
          // TODO: Implement some synchronization mechanism for partition
          // erasure so rebuild, compaction, and aging can properly
          // synchronize.
          TENZIR_WARN("{} refuses to apply transformation '{:?}' to partition "
                      "{} because it is currently being transformed",
                      *self, pipe, partition.uuid);
        }
      }
      if (corrected_partitions.empty()) {
        return partition_apply_result{};
      }
      auto store_id = std::string{self->state().store_actor_plugin->name()};
      auto input_partition_path_template
        = self->state().partition_path_template();
      auto archive_dir = self->state().archive_dir();
      auto partition_path_template
        = self->state().transformer_partition_path_template();
      auto partition_synopsis_path_template
        = self->state().transformer_partition_synopsis_path_template();
      /// Yummy. Partitioned Foam. :)
      partition_transformer_actor partition_transfomer = self->spawn(
        partition_transformer, store_id, self->state().synopsis_opts,
        self->state().index_opts, self->state().catalog,
        self->state().filesystem, std::move(input_partitions), pipe,
        std::move(input_partition_path_template), std::move(archive_dir),
        std::move(partition_path_template),
        std::move(partition_synopsis_path_template), std::move(origin));
      /// Monitor the actor to remove it from the collection of active
      /// transformers.
      auto partition_transformer_addr = partition_transfomer->address();
      auto partition_completion_disposable = self->monitor(
        partition_transfomer,
        [self, partition_transformer_addr](const caf::error&) {
          const auto it = self->state().active_transformers.find(
            partition_transformer_addr);
          TENZIR_ASSERT(it != self->state().active_transformers.end());
          self->state().active_transformers.erase(it);
        });
      const auto [_, inserted] = self->state().active_transformers.try_emplace(
        std::move(partition_transformer_addr),
        std::move(partition_completion_disposable));
      TENZIR_ASSERT(inserted);
      auto marker_path = self->state().marker_path(uuid::random());
      auto rp = self->make_response_promise<partition_apply_result>();
      auto deliver = [self, rp, corrected_partitions, marker_path](
                       caf::expected<partition_apply_result>&& result) mutable {
        // Erase errors don't matter too much here, leftover in-progress
        // transforms will be cleaned up on next startup.
        self->mail(atom::erase_v, marker_path)
          .request(self->state().filesystem, caf::infinite)
          .then(
            [](atom::done) { /* nop */
                             ;
            },
            [self, marker_path](const caf::error& e) {
              TENZIR_DEBUG("{} failed to erase in-progress marker at {}: "
                           "{}",
                           *self, marker_path, e);
            });
        for (const auto& [_, candidate_info] :
             corrected_partitions.candidate_infos) {
          for (const auto& partition : candidate_info.partition_infos) {
            self->state().partitions_in_transformation.erase(partition.uuid);
          }
        }
        if (result) {
          rp.deliver(std::move(*result));
        } else {
          rp.deliver(std::move(result.error()));
        }
        // We clear the in-memory partitions here because they are only used
        // by the partition transformer which will take quite some time to
        // start again.
        self->state().inmem_partitions.clear();
      };
      // TODO: Implement some kind of monadic composition instead of these
      // nested requests.
      // TODO: With CAF 0.19 it will no longer be needed to keep
      // partition_transformer alive in the lambda as the promise kept in the
      // state will keep the actor alive
      self->mail(atom::persist_v)
        .request(partition_transfomer, caf::infinite)
        .then(
          [self, deliver, corrected_partitions, keep, marker_path, rp,
           partition_transfomer](
            partition_transformer_result& transform_result) mutable {
            std::vector<uuid> old_partition_ids;
            old_partition_ids.reserve(transform_result.input_partitions.size());
            for (const auto& partition : transform_result.input_partitions) {
              old_partition_ids.emplace_back(partition.uuid);
            }
            auto apsv = std::move(transform_result.output_partitions);
            // Point each output synopsis at its final `.mdx` path (the marker
            // is renamed there before the merge below). With lazy sketches the
            // catalog drops the Bloom filters on merge and reloads them on
            // demand from this path, so it must be set or pruning would be
            // lost for transformed/rebuilt partitions until the next restart.
            for (auto& aps : apsv) {
              if (aps.synopsis) {
                aps.synopsis.unshared().sketches_file.url = fmt::format(
                  "file://{}",
                  self->state().partition_synopsis_path(aps.uuid).string());
              }
            }
            std::vector<uuid> new_partition_ids;
            new_partition_ids.reserve(apsv.size());
            for (auto const& [uuid, _] : apsv) {
              new_partition_ids.push_back(uuid);
            }
            auto result = std::vector<partition_info>{};
            for (auto const& aps : apsv) {
              // If synopsis was null (ie. all events were deleted),
              // the partition transformer should not have included
              // it in the result.
              TENZIR_ASSERT(aps.synopsis);
              auto info = partition_info{
                aps.uuid,
                *aps.synopsis,
              };
              result.emplace_back(std::move(info));
            }
            auto transformed_input_partitions
              = std::move(transform_result.input_partitions);
            auto input_complete = transform_result.input_complete;
            // Record in-progress marker.
            auto marker_chunk
              = create_marker(old_partition_ids, new_partition_ids, keep);
            self->mail(atom::write_v, marker_path, marker_chunk)
              .request(self->state().filesystem, caf::infinite)
              .then(
                [=, apsv = std::move(apsv)](atom::ok) mutable {
                  // Move the written partitions from the `markers/`
                  // directory into the regular index directory.
                  auto renames = std::vector<
                    std::pair<std::filesystem::path, std::filesystem::path>>{};
                  for (auto const& aps : apsv) {
                    auto old_path
                      = self->state().transformer_partition_path(aps.uuid);
                    auto old_synopsis_path
                      = self->state().transformer_partition_synopsis_path(
                        aps.uuid);
                    auto new_path = self->state().partition_path(aps.uuid);
                    auto new_synopsis_path
                      = self->state().partition_synopsis_path(aps.uuid);
                    renames.emplace_back(std::move(old_path),
                                         std::move(new_path));
                    renames.emplace_back(std::move(old_synopsis_path),
                                         std::move(new_synopsis_path));
                  }
                  self->mail(atom::move_v, std::move(renames))
                    .request(self->state().filesystem, caf::infinite)
                    .then(
                      // Delete input partitions if necessary.
                      [=, apsv = std::move(apsv)](atom::done) mutable {
                        if (keep == keep_original_partition::yes) {
                          if (not apsv.empty()) {
                            self->mail(atom::merge_v, apsv)
                              .request(self->state().catalog, caf::infinite)
                              .then(
                                [self, deliver, transformed_input_partitions,
                                 result, input_complete,
                                 apsv](atom::ok) mutable {
                                  // Update index statistics and list of
                                  // persisted partitions.
                                  for (auto const& aps : apsv) {
                                    self->state().persisted_partitions.emplace(
                                      aps.uuid);
                                  }
                                  self->state().flush_to_disk();
                                  deliver(partition_apply_result{
                                    .input_partitions
                                    = std::move(transformed_input_partitions),
                                    .output_partitions = std::move(result),
                                    .input_complete = input_complete,
                                  });
                                },
                                [deliver](caf::error& e) mutable {
                                  deliver(std::move(e));
                                });
                          } else {
                            deliver(partition_apply_result{
                              .input_partitions
                              = std::move(transformed_input_partitions),
                              .output_partitions = std::move(result),
                              .input_complete = input_complete,
                            });
                          }
                        } else { // keep == keep_original_partition::no
                          self->mail(atom::replace_v, old_partition_ids, apsv)
                            .request(self->state().catalog, caf::infinite)
                            .then(
                              [self, deliver, old_partition_ids,
                               transformed_input_partitions, result, apsv,
                               input_complete](atom::ok) mutable {
                                for (auto const& aps : apsv) {
                                  self->state().persisted_partitions.emplace(
                                    aps.uuid);
                                }
                                self->state().flush_to_disk();
                                self->mail(atom::erase_v, old_partition_ids)
                                  .request(static_cast<index_actor>(self),
                                           caf::infinite)
                                  .then(
                                    [=](atom::done) mutable {
                                      deliver(partition_apply_result{
                                        .input_partitions = std::move(
                                          transformed_input_partitions),
                                        .output_partitions = std::move(result),
                                        .input_complete = input_complete,
                                      });
                                    },
                                    [=](const caf::error& e) mutable {
                                      deliver(e);
                                    });
                              },
                              [deliver](const caf::error& e) mutable {
                                deliver(e);
                              });
                        }
                      },
                      [deliver, self, rp](caf::error& e) mutable {
                        TENZIR_WARN("{} failed to finalize partition "
                                    "transformer output: {}",
                                    *self, e);
                        deliver(std::move(e));
                      });
                },
                [deliver](const caf::error& e) mutable {
                  deliver(e);
                });
          },
          [deliver](const caf::error& e) mutable {
            deliver(e);
          });
      return rp;
    },
    [self](atom::flush) -> caf::result<void> {
      TENZIR_DEBUG("{} got a flush request from {}", *self,
                   self->current_sender());
      if (self->state().active_partitions.empty()) {
        return {};
      }
      return self->state().flush();
    },
    // -- status_client_actor --------------------------------------------------
    [](atom::status, status_verbosity, duration) -> record {
      return {};
    },
    [self](const caf::exit_msg& msg) {
      TENZIR_VERBOSE("{} received EXIT from {} with reason: {}", *self,
                     msg.source, msg.reason);
      auto perform_shutdown = [self](auto reason) {
        self->state().drain_retired_partitions(reason);
        auto dependents = std::vector<caf::actor>{};
        dependents.reserve(self->state().active_transformers.size());
        for (auto& [act, disp] : self->state().active_transformers) {
          disp.dispose();
          dependents.push_back(caf::actor_cast<caf::actor>(act));
        }
        shutdown<policy::parallel>(self, std::move(dependents), reason);
      };
      self->state().shutting_down = true;
      self->mail(atom::flush_v)
        .request(static_cast<index_actor>(self), std::chrono::minutes{10})
        .then(
          [perform_shutdown, reason = msg.reason]() {
            perform_shutdown(reason);
          },
          [perform_shutdown](caf::error& err) {
            auto diag
              = diagnostic::error(std::move(err)).note("while shutting down");
            if (err == caf::sec::request_timeout) {
              diag
                = std::move(diag).note("shutdown timeout: risk of data loss!");
            }
            perform_shutdown(std::move(diag).to_error());
          });
    },
  };
}

} // namespace tenzir
