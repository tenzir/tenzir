//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
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
#include "tenzir/shutdown.hpp"
#include "tenzir/status.hpp"
#include "tenzir/table_slice.hpp"
#include "tenzir/uuid.hpp"

#include <caf/error.hpp>
#include <caf/make_copy_on_write.hpp>
#include <caf/response_promise.hpp>
#include <caf/scoped_actor.hpp>
#include <caf/send.hpp>
#include <flatbuffers/flatbuffers.h>

#include <algorithm>
#include <chrono>
#include <ctime>
#include <deque>
#include <filesystem>
#include <memory>
#include <numeric>
#include <span>
#include <unistd.h>

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
      = tenzir::io::read(file, std::span<std::byte>{buffer, sizeof(buffer)}))
    return false;
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
    if (std::filesystem::exists(candidate, err))
      return candidate;
  }
  return std::nullopt;
}

caf::error extract_partition_synopsis(
  const std::filesystem::path& partition_path,
  const std::filesystem::path& partition_synopsis_path) {
  // Use blocking operations here since this is part of the startup.
  auto chunk = chunk::mmap(partition_path);
  if (!chunk)
    return std::move(chunk.error());
  auto maybe_partition = partition_chunk::get_flatbuffer(*chunk);
  if (!maybe_partition)
    return caf::make_error(
      ec::format_error, fmt::format("malformed partition at {}: {}",
                                    partition_path, maybe_partition.error()));
  const auto* partition = *maybe_partition;
  if (partition->partition_type() != fbs::partition::Partition::legacy)
    return caf::make_error(
      ec::format_error,
      fmt::format("unknown version {} for "
                  "partition at {}",
                  static_cast<uint8_t>(partition->partition_type()),
                  partition_path));
  const auto* partition_legacy = partition->partition_as_legacy();
  TENZIR_ASSERT(partition_legacy);
  partition_synopsis ps;
  if (auto error = unpack(*partition_legacy, ps))
    return error;
  flatbuffers::FlatBufferBuilder builder;
  auto ps_offset = pack(builder, ps);
  if (!ps_offset)
    return ps_offset.error();
  fbs::PartitionSynopsisBuilder ps_builder(builder);
  ps_builder.add_partition_synopsis_type(
    fbs::partition_synopsis::PartitionSynopsis::legacy);
  ps_builder.add_partition_synopsis(ps_offset->Union());
  auto flatbuffer = ps_builder.Finish();
  fbs::FinishPartitionSynopsisBuffer(builder, flatbuffer);
  auto chunk_out = fbs::release(builder);
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
    if (auto uuid_fb = pack(builder, uuid))
      partition_offsets.push_back(*uuid_fb);
    else
      return uuid_fb.error();
  }
  // We don't know if these will make it to disk before the index and the rest
  // of the system is shut down (in case of a hard/dirty shutdown), so we just
  // store everything and throw out the missing partitions when loading the
  // index.
  for (const auto& kv : state.unpersisted) {
    if (auto uuid_fb = pack(builder, kv.first))
      partition_offsets.push_back(*uuid_fb);
    else
      return uuid_fb.error();
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
  if (state_.persisted_partitions.find(id) == state_.persisted_partitions.end())
    TENZIR_WARN("{} did not find partition {} in it's internal state, but "
                "tries "
                "to load it regardless",
                *state_.self, id);
  const auto path = state_.partition_path(id);
  TENZIR_DEBUG("{} loads partition {} for path {}", *state_.self, id, path);
  materializations_++;
  return state_.self->spawn(passive_partition, id, filesystem_, path);
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
  if (!file_exists) {
    TENZIR_VERBOSE("{} found no prior state, starting with a clean slate",
                   *self);
    return caf::none;
  }
  // Start by finishing up any in-progress transforms.
  if (std::filesystem::is_directory(markersdir, err)) {
    auto error = [&]() -> caf::error {
      auto transforms_dir_iter
        = std::filesystem::directory_iterator(markersdir, err);
      if (err)
        return caf::make_error(ec::filesystem_error,
                               fmt::format("{} failed to list directory "
                                           "contents "
                                           "of {}: {}",
                                           *self, dir, err.message()));
      for (auto const& entry : transforms_dir_iter) {
        if (entry.path().extension() != ".marker")
          continue;
        auto chunk = tenzir::chunk::mmap(entry.path());
        if (!chunk) {
          TENZIR_WARN("{} failed to mmap chunk at {}: {}", *self, entry.path(),
                      chunk.error());
          continue;
        }
        auto maybe_flatbuffer
          = tenzir::flatbuffer<tenzir::fbs::PartitionTransform>::make(
            std::move(*chunk));
        if (!maybe_flatbuffer) {
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
            auto partition
              = self->spawn(passive_partition, uuid, filesystem, path);
            self->request(partition, caf::infinite, atom::erase_v)
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
          if (ec)
            TENZIR_WARN("failed to rename '{}' to '{}': {}", from_partition,
                        to_partition, ec.message());
          ec.clear();
          std::filesystem::rename(from_partition_synopsis,
                                  to_partition_synopsis, ec);
          if (ec)
            TENZIR_WARN("failed to rename '{}' to '{}': {}",
                        from_partition_synopsis, to_partition_synopsis,
                        ec.message());
        }
      }
      // TODO: This does not handle store files, which may already have been
      // written. Since a store file may also be written before the partition
      // itself, there does not currently seem to be a bulletproof way of
      // handling this.
      std::filesystem::remove_all(markersdir);
      return caf::none;
    }();
    if (error)
      TENZIR_WARN("{} failed to finish leftover transforms: {}", *self, error);
  }
  auto dir_iter = std::filesystem::directory_iterator(dir, err);
  if (err)
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to list directory contents of "
                                       "{}: {}",
                                       dir, err.message()));
  auto partitions = std::vector<uuid>{};
  auto oversized_partitions = std::vector<uuid>{};
  auto synopsis_files = std::vector<uuid>{};
  auto synopses
    = std::make_shared<std::unordered_map<uuid, partition_synopsis_ptr>>();
  for (const auto& entry : dir_iter) {
    const auto stem = entry.path().stem();
    tenzir::uuid partition_uuid{};
    // Ignore files that don't use UUID for the filename.
    if (!parsers::uuid(stem.string(), partition_uuid))
      continue;
    auto ext = entry.path().extension();
    if (ext.empty()) {
      // Newer partitions are not limited to FLATBUFFERS_MAX_BUFFER_SIZE,
      // this is only a problem for older ones that still have `fbs::Partition`
      // as root type.
      if (entry.file_size() >= FLATBUFFERS_MAX_BUFFER_SIZE
          && test_file_identifier(entry, fbs::PartitionIdentifier())) {
        auto store_path
          = dir / ".." / "archive" / fmt::format("{:u}.store", partition_uuid);
        if (std::filesystem::exists(store_path, err))
          oversized_partitions.push_back(partition_uuid);
        else
          TENZIR_WARN("{} did not find a store file for the oversized "
                      "partition {} and won't attempt to recover the data",
                      *self, partition_uuid);
      } else
        partitions.push_back(partition_uuid);
    } else if (ext == std::filesystem::path{".mdx"})
      synopsis_files.push_back(partition_uuid);
  }
  std::sort(partitions.begin(), partitions.end());
  std::sort(synopsis_files.begin(), synopsis_files.end());
  auto orphans = std::vector<uuid>{};
  std::set_difference(synopsis_files.begin(), synopsis_files.end(),
                      partitions.begin(), partitions.end(),
                      std::back_inserter(orphans));
  // Do a bit of housekeeping. MDX files without matching partitions shouldn't
  // be there in the first place.
  TENZIR_DEBUG("{} deletes {} orphaned mdx files", *self, orphans.size());
  for (auto& orphan : orphans)
    std::filesystem::remove(dir / fmt::format("{}.mdx", orphan), err);
  // We build an in-memory representation of the archive folder for quicker
  // lookup when we add file paths to the in-memory synopsis.
  const auto store_map = [&] {
    auto result = std::map<uuid, std::filesystem::path>{};
    auto store_path = dir / ".." / "archive";
    if (!std::filesystem::is_directory(store_path, err))
      return result;
    for (auto const& store_file :
         std::filesystem::directory_iterator{store_path}) {
      tenzir::uuid store_uuid{};
      if (!parsers::uuid(store_file.path().stem().string(), store_uuid))
        continue;
      result.emplace(store_uuid, store_file.path());
    }
    return result;
  }();
  // Now try to load the partitions - with a progress indicator.
  for (size_t idx = 0; idx < partitions.size(); ++idx) {
    auto partition_uuid = partitions[idx];
    auto error = [&]() -> caf::error {
      auto part_path = partition_path(partition_uuid);
      TENZIR_DEBUG("{} unpacks partition {} ({}/{})", *self, partition_uuid,
                   idx, partitions.size());
      // Generate external partition synopsis file if it doesn't exist.
      auto synopsis_path = partition_synopsis_path(partition_uuid);
      if (!exists(synopsis_path)) {
        if (auto error = extract_partition_synopsis(part_path, synopsis_path))
          return error;
      }
      auto chunk = chunk::mmap(synopsis_path);
      if (!chunk)
        return chunk.error();
      const auto* ps_flatbuffer
        = fbs::GetPartitionSynopsis(chunk->get()->data());
      partition_synopsis_ptr ps = caf::make_copy_on_write<partition_synopsis>();
      if (ps_flatbuffer->partition_synopsis_type()
          != fbs::partition_synopsis::PartitionSynopsis::legacy)
        return caf::make_error(ec::format_error, "invalid partition synopsis "
                                                 "version");
      const auto& synopsis_legacy
        = *ps_flatbuffer->partition_synopsis_as_legacy();
      if (auto error = unpack(synopsis_legacy, ps.unshared()))
        return error;
      // Add partition file sizes.
      {
        uint64_t bitmap_file_size = std::filesystem::file_size(part_path, err);
        if (err) {
          TENZIR_WARN("failed to get the size of the partition index file at "
                      "{}: {}",
                      part_path, err.message());
          bitmap_file_size = 0u;
        }
        if (const auto canonical_part_path = canonical(part_path, err);
            not err) {
          ps.unshared().indexes_file = {
            .url = fmt::format("file://{}", canonical_part_path),
            .size = bitmap_file_size,
          };
        }
        if (const auto canonical_synopsis_path = canonical(synopsis_path, err);
            not err) {
          ps.unshared().sketches_file = {
            .url = fmt::format("file://{}", canonical_synopsis_path),
            .size = chunk->get()->size(),
          };
        }
        auto f = store_map.find(partition_uuid);
        if (f == store_map.end()) {
          // For completeness sake we could open the partition and look if the
          // data is somewhere else entirely, but no known implementation ever
          // deviated from the default path scheme, so we assume filesystem
          // corruption here.
          return add_context(ec::no_such_file,
                             "discarding partition {} due to a missing store "
                             "file",
                             partition_uuid);
        }
        auto store_path = f->second;
        auto store_size = std::filesystem::file_size(store_path, err);
        if (err) {
          TENZIR_WARN("failed to get the size of the partition store file at "
                      "{}: {}",
                      store_path, err.message());
          store_size = 0u;
        }
        if (const auto canonical_store_path = canonical(store_path, err);
            not err) {
          ps.unshared().store_file = {
            .url = fmt::format("file://{}", canonical_store_path),
            .size = store_size,
          };
        }
      }
      persisted_partitions.emplace(partition_uuid);
      synopses->emplace(partition_uuid, std::move(ps));
      return caf::none;
    }();
    if (error) {
      TENZIR_VERBOSE("{} failed to load partition {}: {}", *self,
                     partition_uuid, error);
    }
  }
  //  Recommend the user to run 'tenzir-ctl rebuild' if any partition syopses
  //  are outdated. We need to nudge them a bit so we can drop support for older
  //  partition versions more freely.
  const auto num_outdated = std::count_if(
    synopses->begin(), synopses->end(), [](const auto& id_and_synopsis) {
      return id_and_synopsis.second->version
             < version::current_partition_version;
    });
  if (num_outdated > 0) {
    TENZIR_WARN("{} detected {}/{} outdated partitions; consider running "
                "'tenzir-ctl "
                "rebuild' to upgrade existing partitions in the background",
                *self, num_outdated, synopses->size());
  }
  // We collect all synopses to send them in bulk, since the `await` interface
  // doesn't lend itself to a huge number of awaited messages: Only the tip of
  // the current awaited list is considered, leading to an O(n**2) worst-case
  // behavior if the responses arrive in the same order to how they were sent.
  TENZIR_DEBUG("{} requesting bulk merge of {} partitions", *self,
               synopses->size());
  this->accept_queries = false;
  self
    ->request(catalog, caf::infinite, atom::merge_v,
              std::exchange(synopses, {}))
    .then(
      [this](atom::ok) {
        TENZIR_VERBOSE(
          "{} finished initializing and is ready to accept queries", *self);
        this->accept_queries = true;
        for (auto&& [rp, query_context] : std::exchange(delayed_queries, {}))
          rp.delegate(static_cast<index_actor>(self), atom::evaluate_v,
                      std::move(query_context));
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
  if (!index) {
    TENZIR_WARN("{} failed to pack index: {}", *self, index.error());
    return;
  }
  auto chunk = fbs::release(builder);
  self
    ->request(filesystem, caf::infinite, atom::write_v, index_filename(), chunk)
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
    if (!part) {
      self->quit(caf::make_error(ec::logic_error,
                                 fmt::format("{} failed to create active "
                                             "partition: {}",
                                             *self, part.error())));
      return;
    }
    active_partition = *part;
  } else if (x.rows() > active_partition->second.capacity) {
    TENZIR_DEBUG("{} flushes active partition {} with {} rows and {}/{} events",
                 *self, schema, x.rows(),
                 partition_capacity - active_partition->second.capacity,
                 partition_capacity);
    decommission_active_partition(schema, {});
    flush_to_disk();
    auto part = create_active_partition(schema);
    if (!part) {
      self->quit(caf::make_error(ec::logic_error,
                                 fmt::format("{} failed to create active "
                                             "partition: {}",
                                             *self, part.error())));
      return;
    }
    active_partition = *part;
  }
  TENZIR_ASSERT(active_partition->second.actor);
  self->send(active_partition->second.actor, x);
  if (active_partition->second.capacity == partition_capacity
      && x.rows() > active_partition->second.capacity) {
    TENZIR_WARN("{} got table slice with {} rows that exceeds the "
                "default partition capacity of {} rows",
                *self, x.rows(), partition_capacity);
    active_partition->second.capacity = 0;
  } else {
    TENZIR_ASSERT(active_partition->second.capacity >= x.rows());
    active_partition->second.capacity -= x.rows();
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
  active_partition->second.capacity = partition_capacity;
  active_partition->second.id = id;
  detail::weak_run_delayed(self, active_partition_timeout, [schema, id, this] {
    const auto it = active_partitions.find(schema);
    if (it == active_partitions.end() or it->second.id != id) {
      // If the partition was already rotated then there's nothing to do for us.
      return;
    }
    TENZIR_DEBUG("{} flushes active partition {} with {}/{} {} events "
                 "after {} timeout",
                 *self, it->second.id, partition_capacity - it->second.capacity,
                 partition_capacity, schema, data{active_partition_timeout});
    decommission_active_partition(schema, [this, schema,
                                           id](const caf::error& err) mutable {
      if (err) {
        TENZIR_WARN("{} failed to flush active partition {} ({}) after {} "
                    "timeout: {}",
                    *self, id, schema, data{active_partition_timeout}, err);
      }
    });
    flush_to_disk();
  });
  TENZIR_DEBUG("{} created new partition {}", *self, id);
  return active_partition;
}

void index_state::decommission_active_partition(
  const type& schema, std::function<void(const caf::error&)> completion) {
  const auto active_partition = active_partitions.find(schema);
  TENZIR_ASSERT(active_partition != active_partitions.end());
  const auto id = active_partition->second.id;
  const auto actor = std::exchange(active_partition->second.actor, {});
  const auto type = active_partition->first;
  // Move the active partition to the list of unpersisted partitions.
  TENZIR_ASSERT_EXPENSIVE(!unpersisted.contains(id));
  unpersisted[id] = {type, actor};
  active_partitions.erase(active_partition);
  // Persist active partition asynchronously.
  const auto part_path = partition_path(id);
  const auto synopsis_path = partition_synopsis_path(id);
  TENZIR_TRACE("{} persists active partition {} to {}", *self, schema,
               part_path);
  self->request(actor, caf::infinite, atom::persist_v, part_path, synopsis_path)
    .then(
      [=, this](partition_synopsis_ptr& ps) {
        TENZIR_TRACE("{} successfully persisted partition {} {}", *self, schema,
                     id);
        // The catalog expects to own the partition synopsis it receives,
        // so we make a copy for the listeners.
        // TODO: We should skip this continuation if we're currently shutting
        // down.
        auto apsv = std::vector<partition_synopsis_pair>{{id, ps}};
        self->request(catalog, caf::infinite, atom::merge_v, std::move(apsv))
          .then(
            [=, this](atom::ok) {
              TENZIR_TRACE("{} inserted partition {} {} to the catalog", *self,
                           schema, id);
              for (auto& listener : partition_creation_listeners)
                self->send(listener, atom::update_v,
                           partition_synopsis_pair{id, ps});
              unpersisted.erase(id);
              persisted_partitions.emplace(id);
              self->send_exit(actor, caf::exit_reason::normal);
              if (completion)
                completion(caf::none);
            },
            [=, this](const caf::error& err) {
              TENZIR_ERROR("{} failed to commit partition {} {} to the "
                           "catalog, "
                           "the contained data will not be available for "
                           "queries: {}",
                           *self, schema, id, err);
              unpersisted.erase(id);
              self->send_exit(actor, err);
              if (completion)
                completion(err);
            });
      },
      [=, this](caf::error& err) {
        TENZIR_ERROR("{} failed to persist partition {} {} and evicts data "
                     "from "
                     "memory to preserve process integrity: {}",
                     *self, schema, id, err);
        unpersisted.erase(id);
        self->send_exit(actor, err);
        if (completion)
          completion(err);
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
                                    if (err) {
                                      counter->receive_error(err);
                                    } else {
                                      counter->receive_success();
                                    }
                                  });
  }
  return rp;
}

void index_state::add_partition_creation_listener(
  partition_creation_listener_actor listener) {
  partition_creation_listeners.push_back(listener);
}

// -- query handling ---------------------------------------------------------

auto index_state::schedule_lookups() -> size_t {
  if (!pending_queries.has_work())
    return 0u;
  const size_t previous_partition_lookups = running_partition_lookups;
  while (running_partition_lookups < max_concurrent_partition_lookups) {
    // 1. Get the partition with the highest accumulated priority.
    auto next = pending_queries.next();
    if (!next) {
      TENZIR_DEBUG("{} did not find a partition to query", *self);
      break;
    }
    auto immediate_completion = [&](const query_queue::entry& x) {
      for (auto qid : x.queries) {
        if (auto client = pending_queries.handle_completion(qid)) {
          TENZIR_DEBUG("{} completes query {} immediately", *self, qid);
          self->send(*client, atom::done_v);
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
    TENZIR_DEBUG("{} schedules partition {} for {}", *self, next->partition,
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
            && active_partition.id == partition_id) {
          part = active_partition.actor;
          break;
        }
      }
      if (!part) {
        if (auto it = unpersisted.find(partition_id); it != unpersisted.end()) {
          part = it->second.second;
        } else if (auto it = persisted_partitions.find(partition_id);
                   it != persisted_partitions.end()) {
          part = inmem_partitions.get_or_load(partition_id);
        }
      }
      if (!part)
        TENZIR_WARN("{} failed to load partition {} that was part of a query",
                    *self, partition_id);
      return part;
    };
    auto partition_actor = acquire(next->partition);
    if (!partition_actor) {
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
        if (auto client = pending_queries.handle_completion(qid))
          self->send(*client, atom::done_v);
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
          TENZIR_DEBUG("{} scheduled {} partitions after completion of a "
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
      self
        ->request(partition_actor, defaults::scheduler_timeout, atom::query_v,
                  context_it->second)
        .then(
          [this, handle_completion, qid, pid = next->partition](uint64_t n) {
            TENZIR_DEBUG("{} received {} results for query {} from partition "
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
  usage += persisted_partitions.size()
           * sizeof(decltype(persisted_partitions)::value_type);
  usage += pending_queries.memusage();
  for (const auto& [addr, uuids] : monitored_queries) {
    usage += sizeof(addr) + calculate_usage(uuids);
  }
  usage += calculate_usage(flush_listeners);
  usage += calculate_usage(partition_creation_listeners);
  usage += calculate_usage(partitions_in_transformation);
  return usage;
}

index_actor::behavior_type
index(index_actor::stateful_pointer<index_state> self,
      filesystem_actor filesystem, catalog_actor catalog,
      const std::filesystem::path& dir, std::string store_backend,
      size_t partition_capacity, duration active_partition_timeout,
      size_t max_inmem_partitions, size_t taste_partitions,
      size_t max_concurrent_partition_lookups,
      const std::filesystem::path& catalog_dir, index_config index_config) {
  TENZIR_TRACE_SCOPE(
    "index {} {} {} {} {} {} {} {} {} {}", TENZIR_ARG(self->id()),
    TENZIR_ARG(filesystem), TENZIR_ARG(dir), TENZIR_ARG(partition_capacity),
    TENZIR_ARG(active_partition_timeout), TENZIR_ARG(max_inmem_partitions),
    TENZIR_ARG(taste_partitions), TENZIR_ARG(max_concurrent_partition_lookups),
    TENZIR_ARG(catalog_dir), TENZIR_ARG(index_config));
  if (self->getf(caf::scheduled_actor::is_detached_flag)) {
    caf::detail::set_thread_name("tenzir.index");
  }
  TENZIR_VERBOSE("{} initializes index in {} with a maximum partition "
                 "size of {} events and {} resident partitions",
                 *self, dir, partition_capacity, max_inmem_partitions);
  self->state.index_opts["cardinality"] = partition_capacity;
  self->state.synopsis_opts = std::move(index_config);
  if (dir != catalog_dir)
    TENZIR_VERBOSE("{} uses {} for catalog data", *self, catalog_dir);
  // Set members.
  self->state.self = self;
  self->state.accept_queries = true;
  self->state.max_concurrent_partition_lookups
    = max_concurrent_partition_lookups;
  self->state.store_actor_plugin
    = plugins::find<store_actor_plugin>(store_backend);
  if (!self->state.store_actor_plugin) {
    auto error = caf::make_error(ec::invalid_configuration,
                                 fmt::format("could not find "
                                             "store plugin '{}'",
                                             store_backend));
    TENZIR_ERROR("{}", render(error));
    self->quit(error);
    return index_actor::behavior_type::make_empty_behavior();
  }
  self->state.filesystem = std::move(filesystem);
  self->state.catalog = std::move(catalog);
  self->state.taxonomies = std::make_shared<tenzir::taxonomies>();
  self->state.taxonomies->concepts = modules::concepts();
  self->state.dir = dir;
  self->state.synopsisdir = catalog_dir;
  self->state.markersdir = dir / "markers";
  self->state.partition_capacity = partition_capacity;
  self->state.active_partition_timeout = active_partition_timeout;
  self->state.taste_partitions = taste_partitions;
  self->state.inmem_partitions.factory().filesystem() = self->state.filesystem;
  self->state.inmem_partitions.resize(max_inmem_partitions);
  // Read persistent state.
  if (auto err = self->state.load_from_disk()) {
    TENZIR_ERROR("{} failed to load index state from disk: {}", *self,
                 render(err));
    self->quit(err);
    return index_actor::behavior_type::make_empty_behavior();
  }
  self->set_exit_handler([self](const caf::exit_msg& msg) {
    TENZIR_VERBOSE("{} received EXIT from {} with reason: {}", *self,
                   msg.source, msg.reason);
    for (auto&& [rp, _] : std::exchange(self->state.delayed_queries, {})) {
      rp.deliver(msg.reason);
    }
    self->state.shutting_down = true;
    self
      ->request(static_cast<index_actor>(self), std::chrono::minutes{10},
                atom::flush_v)
      .then(
        [self]() {
          self->quit();
        },
        [self](caf::error& err) {
          auto diag
            = diagnostic::error(std::move(err)).note("while shutting down");
          if (err == caf::sec::request_timeout) {
            diag = std::move(diag).note("shutdown timeout: risk of data loss!");
          }
          self->quit(std::move(diag).to_error());
        });
  });
  // Set up a down handler for monitored exporter actors.
  self->set_down_handler([=](const caf::down_msg& msg) {
    auto it = self->state.monitored_queries.find(msg.source);
    if (it == self->state.monitored_queries.end()) {
      TENZIR_WARN("{} received DOWN from unexpected sender", *self);
      return;
    }
    const auto& [_, ids] = *it;
    if (!ids.empty()) {
      // Workaround to {fmt} 7 / gcc 10 combo, which errors with "passing views
      // as lvalues is disallowed" when not formating the join view separately.
      const auto ids_string = fmt::to_string(fmt::join(ids, ", "));
      TENZIR_DEBUG("{} received DOWN for queries [{}] and drops remaining "
                   "query results",
                   *self, ids_string);
      for (const auto& id : ids) {
        if (auto err = self->state.pending_queries.remove_query(id))
          TENZIR_DEBUG("{} did not remove {} from the query queue. It was "
                       "presumably already removed upon completion ({})",
                       *self, id, err);
      }
    }
    self->state.monitored_queries.erase(it);
  });
  detail::weak_run_delayed_loop(
    self, defaults::metrics_interval,
    [self, actor_metrics_builder
           = detail::make_actor_metrics_builder()]() mutable {
      const auto importer
        = self->system().registry().get<importer_actor>("tenzir.importer");
      self->send(importer,
                 detail::generate_actor_metrics(actor_metrics_builder, self));
    });
  return {
    [self](atom::done, uuid partition_id) {
      TENZIR_DEBUG("{} queried partition {} successfully", *self, partition_id);
    },
    [self](table_slice& slice) {
      self->state.handle_slice(std::move(slice));
    },
    [self](atom::subscribe, atom::create,
           const partition_creation_listener_actor& listener,
           send_initial_dbstate should_send) {
      TENZIR_DEBUG("{} adds partition creation listener", *self);
      self->state.add_partition_creation_listener(listener);
      if (should_send == send_initial_dbstate::no)
        return;
      // When we get here, the initial bulk upgrade and any table slices
      // finished since then have already been sent to the catalog, and
      // since CAF guarantees message order within the same inbound queue
      // they will all be part of the response vector.
      self->request(self->state.catalog, caf::infinite, atom::get_v)
        .then(
          [=](std::vector<partition_synopsis_pair>& v) {
            self->send(listener, atom::update_v, std::move(v));
          },
          [](const caf::error& e) {
            TENZIR_WARN(
              "index failed to get list of partitions from catalog: {}", e);
          });
    },
    [self](atom::evaluate,
           tenzir::query_context query_context) -> caf::result<query_cursor> {
      // Query handling
      auto sender = self->current_sender();
      // Sanity check.
      if (!sender) {
        TENZIR_WARN("{} ignores an anonymous query", *self);
        return caf::sec::invalid_argument;
      }
      // Abort if the index is already shutting down.
      if (self->state.shutting_down) {
        TENZIR_WARN("{} ignores query {} because it is shutting down", *self,
                    query_context);
        return ec::remote_node_down;
      }
      // If we're not yet ready to start, we delay the query until further
      // notice.
      if (!self->state.accept_queries) {
        TENZIR_VERBOSE("{} delays query {} because it is still starting up",
                       *self, query_context);
        auto rp = self->make_response_promise<query_cursor>();
        self->state.delayed_queries.emplace_back(rp, std::move(query_context));
        return rp;
      }
      // Allows the client to query further results after initial taste.
      if (query_context.id != uuid::null())
        return caf::make_error(ec::logic_error, "query must not have an ID "
                                                "when arriving at the index");
      query_context.id = self->state.pending_queries.create_query_id();
      // Monitor the sender so we can cancel the query in case it goes down.
      if (const auto it = self->state.monitored_queries.find(sender->address());
          it == self->state.monitored_queries.end()) {
        self->state.monitored_queries.emplace_hint(
          it, sender->address(), std::unordered_set{query_context.id});
        self->monitor(sender);
      } else {
        auto& [_, ids] = *it;
        ids.emplace(query_context.id);
      }
      std::vector<std::pair<uuid, type>> candidates;
      candidates.reserve(self->state.active_partitions.size()
                         + self->state.unpersisted.size());
      query_state::type_query_context_map query_contexts;
      auto rp = self->make_response_promise<query_cursor>();
      self
        ->request(self->state.catalog, caf::infinite, atom::candidates_v,
                  query_context)
        .then(
          [=, candidates = std::move(candidates),
           query_contexts = std::move(query_contexts)](
            catalog_lookup_result& lookup_result) mutable {
            for (auto& [id, schema] : candidates) {
              auto new_partition_info = partition_info{
                id, 0u, time{}, schema, version::current_partition_version};
              auto schema_candidate_infos_it
                = lookup_result.candidate_infos.find(schema);
              if (schema_candidate_infos_it
                  == lookup_result.candidate_infos.end()) {
                schema_candidate_infos_it
                  = lookup_result.candidate_infos.insert(
                    schema_candidate_infos_it, {schema, {}});
                schema_candidate_infos_it->second.exp = query_context.expr;
              }
              const auto& schema_candidate_infos
                = schema_candidate_infos_it->second.partition_infos;
              if (std::find_if(
                    schema_candidate_infos.begin(),
                    schema_candidate_infos.end(),
                    [&new_partition_info](const auto& partition_info) {
                      return partition_info.uuid == new_partition_info.uuid;
                    })
                  == schema_candidate_infos.end()) {
                lookup_result.candidate_infos[schema]
                  .partition_infos.emplace_back(new_partition_info);
              }
            }
            for (const auto& [type, lookup_result] :
                 lookup_result.candidate_infos) {
              query_contexts[type] = query_context;
              query_contexts[type].expr = lookup_result.exp;
              TENZIR_DEBUG(
                "{} got initial candidates {} for schema {} and from "
                "catalog {}",
                *self, candidates, type, lookup_result.partition_infos);
            }
            // Allows the client to query further results after initial taste.
            auto query_id = query_context.id;
            auto client = caf::visit(
              detail::overload{
                [&](extract_query_context& extract) {
                  return caf::actor_cast<receiver_actor<atom::done>>(
                    extract.sink);
                },
              },
              query_context.cmd);
            if (lookup_result.empty()) {
              TENZIR_DEBUG("{} returns without result: no partitions qualify",
                           *self);
              rp.deliver(query_cursor{query_id, 0u, 0u});
              self->send(client, atom::done_v);
              return;
            }
            auto num_candidates
              = detail::narrow<uint32_t>(lookup_result.size());
            auto taste_size = query_context.taste
                                ? *query_context.taste
                                : self->state.taste_partitions;
            auto scheduled = std::min(num_candidates, taste_size);
            if (auto err = self->state.pending_queries.insert(
                  query_state{.query_contexts_per_type = query_contexts,
                              .client = client,
                              .candidate_partitions = num_candidates,
                              .requested_partitions = scheduled},
                  std::move(lookup_result)))
              rp.deliver(err);
            rp.deliver(query_cursor{query_id, num_candidates, scheduled});
            const auto num_scheduled = self->state.schedule_lookups();
            TENZIR_DEBUG("{} scheduled {} partitions for lookup after a new "
                         "query came in",
                         *self, num_scheduled);
          },
          [rp](const caf::error& e) mutable {
            rp.deliver(caf::make_error(
              ec::system_error, fmt::format("catalog lookup failed: {}", e)));
          });
      return rp;
    },
    [self](atom::resolve,
           tenzir::expression& expr) -> caf::result<catalog_lookup_result> {
      auto query_context = query_context::make_extract("index", self, expr);
      query_context.id = tenzir::uuid::random();
      return self->delegate(self->state.catalog, atom::candidates_v,
                            std::move(query_context));
    },
    [self](atom::query, const uuid& query_id, uint32_t num_partitions) {
      if (auto err
          = self->state.pending_queries.activate(query_id, num_partitions))
        TENZIR_WARN("{} can't activate unknown query: {}", *self, err);
      const auto num_scheduled = self->state.schedule_lookups();
      TENZIR_DEBUG("{} scheduled {} partitions following the request to "
                   "activate {} partitions for query {}",
                   *self, num_scheduled, num_partitions, query_id);
    },
    [self](atom::erase, uuid partition_id) -> caf::result<atom::done> {
      TENZIR_VERBOSE("{} erases partition {}", *self, partition_id);
      auto rp = self->make_response_promise<atom::done>();
      auto path = self->state.partition_path(partition_id);
      auto synopsis_path = self->state.partition_synopsis_path(partition_id);
      if (!self->state.persisted_partitions.contains(partition_id)) {
        std::error_code err{};
        const auto file_exists = std::filesystem::exists(path, err);
        if (!file_exists) {
          rp.deliver(caf::make_error(
            ec::logic_error, fmt::format("unknown partition for path {}: {}",
                                         path, err.message())));
          return rp;
        }
      }
      self
        ->request<caf::message_priority::high>(
          self->state.catalog, caf::infinite, atom::erase_v, partition_id)
        .then(
          [self, partition_id, path, synopsis_path, rp](atom::ok) mutable {
            TENZIR_DEBUG("{} erased partition {} from catalog", *self,
                         partition_id);
            self->state.persisted_partitions.erase(partition_id);
            // We don't remove the partition from the queue directly because the
            // query API requires clients to keep track of the number of
            // candidate partitions. Removing the partition from the queue
            // would require us to update the partition counters in the query
            // states and the client would go out of sync. That would require
            // the index to deal with a few complicated corner cases.
            self->state.pending_queries.mark_partition_erased(partition_id);
            // Remove the synopsis file. We can already safely do so because
            // the catalog acked the erase.
            self
              ->request<caf::message_priority::high>(self->state.filesystem,
                                                     caf::infinite,
                                                     atom::erase_v,
                                                     synopsis_path)
              .then(
                [self, partition_id](atom::done) {
                  TENZIR_DEBUG("{} erased partition synopsis {} from "
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
              self
                ->request<caf::message_priority::high>(
                  self->state.filesystem, caf::infinite, atom::erase_v, path)
                .then(
                  [self, partition_id](atom::done) {
                    TENZIR_DEBUG("{} erased partition {} from filesystem",
                                 *self, partition_id);
                  },
                  [self, partition_id, path](const caf::error& err) {
                    TENZIR_WARN("{} failed to erase partition {} at {}: {}",
                                *self, partition_id, path, err);
                  });
            };
            auto store_path
              = store_path_for_partition(self->state.dir / "..", partition_id);
            if (store_path) {
              erase_dense_index_file();
              rp.delegate(self->state.filesystem, atom::erase_v, *store_path);
              return;
            }
            // Fallback path: In case the store file is not found
            // at the expected path we need to load the partition
            // and retrieve the correct path from the store header.
            TENZIR_DEBUG("{} did not find a store for partition {}, inspecting "
                         "the store header",
                         *self, partition_id);
            self
              ->request<caf::message_priority::high>(
                self->state.filesystem, caf::infinite, atom::mmap_v, path)
              .then(
                [=](const chunk_ptr& chunk) mutable {
                  TENZIR_DEBUG("{} mmapped partition {} to extract store path "
                               "for erasure",
                               *self, partition_id);
                  if (!chunk) {
                    erase_dense_index_file();
                    rp.deliver(caf::make_error( //
                      ec::filesystem_error,
                      fmt::format("failed to load the state for "
                                  "partition {}",
                                  path)));
                    return;
                  }
                  if (chunk->size() >= FLATBUFFERS_MAX_BUFFER_SIZE
                      && flatbuffers::BufferHasIdentifier(
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
                    = self->state.inmem_partitions.eject(partition_id);
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
        self
          ->request(static_cast<index_actor>(self), caf::infinite,
                    atom::erase_v, id)
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
    [self](atom::apply, pipeline pipe,
           std::vector<partition_info> selected_partitions,
           keep_original_partition keep)
      -> caf::result<std::vector<partition_info>> {
      const auto current_sender = self->current_sender();
      if (selected_partitions.empty())
        return caf::make_error(ec::invalid_argument, "no partitions given");
      TENZIR_DEBUG("{} applies a pipeline to partitions {}", *self,
                   selected_partitions);
      TENZIR_ASSERT(self->state.store_actor_plugin);
      std::erase_if(selected_partitions, [&](const auto& entry) {
        if (self->state.persisted_partitions.contains(entry.uuid)) {
          return false;
        }
        TENZIR_WARN("{} skips unknown partition {} for pipeline {:?}", *self,
                    entry.uuid, pipe);
        return true;
      });
      auto corrected_partitions = catalog_lookup_result{};
      for (const auto& partition : selected_partitions) {
        if (self->state.partitions_in_transformation.insert(partition.uuid)
              .second) {
          corrected_partitions.candidate_infos[partition.schema]
            .partition_infos.emplace_back(partition);
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
      if (corrected_partitions.empty())
        return std::vector<partition_info>{};
      auto store_id = std::string{self->state.store_actor_plugin->name()};
      auto partition_path_template
        = self->state.transformer_partition_path_template();
      auto partition_synopsis_path_template
        = self->state.transformer_partition_synopsis_path_template();
      partition_transformer_actor partition_transfomer
        = self->spawn(partition_transformer, store_id,
                      self->state.synopsis_opts, self->state.index_opts,
                      self->state.catalog, self->state.filesystem, pipe,
                      std::move(partition_path_template),
                      std::move(partition_synopsis_path_template));
      // match_everything == '"" in #schema'
      static const auto match_everything
        = tenzir::predicate{meta_extractor{meta_extractor::schema},
                            relational_operator::ni, data{""}};
      auto query_context = query_context::make_extract(
        fmt::format("{:?}", pipe), partition_transfomer, match_everything);
      auto transform_id = self->state.pending_queries.create_query_id();
      query_context.id = transform_id;
      // We set the query priority for partition transforms to zero so they
      // always get less priority than queries.
      query_context.priority = 0;
      TENZIR_DEBUG("{} emplaces {} for pipeline {:?}", *self, query_context,
                   pipe);
      auto query_contexts = query_state::type_query_context_map{};
      for (const auto& [type, _] : corrected_partitions.candidate_infos) {
        query_contexts[type] = query_context;
      }
      auto input_size
        = detail::narrow_cast<uint32_t>(corrected_partitions.size());
      auto err = self->state.pending_queries.insert(
        query_state{.query_contexts_per_type = query_contexts,
                    .client = caf::actor_cast<receiver_actor<atom::done>>(
                      partition_transfomer),
                    .candidate_partitions = input_size,
                    .requested_partitions = input_size},
        catalog_lookup_result{corrected_partitions});
      TENZIR_ASSERT(err == caf::none);
      const auto num_scheduled = self->state.schedule_lookups();
      TENZIR_DEBUG("{} scheduled {} partitions following a request to "
                   "transform partitions",
                   *self, num_scheduled);
      auto marker_path = self->state.marker_path(transform_id);
      auto rp = self->make_response_promise<std::vector<partition_info>>();
      auto deliver
        = [self, rp, corrected_partitions, marker_path](
            caf::expected<std::vector<partition_info>>&& result) mutable {
            // Erase errors don't matter too much here, leftover in-progress
            // transforms will be cleaned up on next startup.
            self
              ->request(self->state.filesystem, caf::infinite, atom::erase_v,
                        marker_path)
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
                self->state.partitions_in_transformation.erase(partition.uuid);
              }
            }
            if (result)
              rp.deliver(std::move(*result));
            else
              rp.deliver(std::move(result.error()));
          };
      // TODO: Implement some kind of monadic composition instead of these
      // nested requests.
      // TODO: With CAF 0.19 it will no longer be needed to keep
      // partition_transformer alive in the lambda as the promise kept in the
      // state will keep the actor alive
      self->request(partition_transfomer, caf::infinite, atom::persist_v)
        .then(
          [self, deliver, corrected_partitions, keep, marker_path, rp,
           partition_transfomer](
            std::vector<partition_synopsis_pair>& apsv) mutable {
            std::vector<uuid> old_partition_ids;
            old_partition_ids.reserve(corrected_partitions.size());
            for (const auto& [_, candidate_info] :
                 corrected_partitions.candidate_infos) {
              for (const auto& partition : candidate_info.partition_infos) {
                old_partition_ids.emplace_back(partition.uuid);
              }
            }
            std::vector<uuid> new_partition_ids;
            new_partition_ids.reserve(apsv.size());
            for (auto const& [uuid, _] : apsv)
              new_partition_ids.push_back(uuid);
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
            // Record in-progress marker.
            auto marker_chunk
              = create_marker(old_partition_ids, new_partition_ids, keep);
            self
              ->request(self->state.filesystem, caf::infinite, atom::write_v,
                        marker_path, marker_chunk)
              .then(
                [=, apsv = std::move(apsv)](atom::ok) mutable {
                  // Move the written partitions from the `markers/`
                  // directory into the regular index directory.
                  auto renames = std::vector<
                    std::pair<std::filesystem::path, std::filesystem::path>>{};
                  for (auto const& aps : apsv) {
                    auto old_path
                      = self->state.transformer_partition_path(aps.uuid);
                    auto old_synopsis_path
                      = self->state.transformer_partition_synopsis_path(
                        aps.uuid);
                    auto new_path = self->state.partition_path(aps.uuid);
                    auto new_synopsis_path
                      = self->state.partition_synopsis_path(aps.uuid);
                    renames.emplace_back(std::move(old_path),
                                         std::move(new_path));
                    renames.emplace_back(std::move(old_synopsis_path),
                                         std::move(new_synopsis_path));
                  }
                  self
                    ->request(self->state.filesystem, caf::infinite,
                              atom::move_v, std::move(renames))
                    .then(
                      // Delete input partitions if necessary.
                      [=, apsv = std::move(apsv)](atom::done) mutable {
                        if (keep == keep_original_partition::yes) {
                          if (!apsv.empty())
                            self
                              ->request(self->state.catalog, caf::infinite,
                                        atom::merge_v, apsv)
                              .then(
                                [self, deliver, result = std::move(result),
                                 apsv](atom::ok) mutable {
                                  // Update index statistics and list of
                                  // persisted partitions.
                                  for (auto const& aps : apsv) {
                                    self->state.persisted_partitions.emplace(
                                      aps.uuid);
                                  }
                                  self->state.flush_to_disk();
                                  deliver(std::move(result));
                                },
                                [deliver](caf::error& e) mutable {
                                  deliver(std::move(e));
                                });
                          else
                            deliver(result);
                        } else { // keep == keep_original_partition::no
                          self
                            ->request(self->state.catalog, caf::infinite,
                                      atom::replace_v, old_partition_ids, apsv)
                            .then(
                              [self, deliver, old_partition_ids, result,
                               apsv](atom::ok) mutable {
                                for (auto const& aps : apsv) {
                                  self->state.persisted_partitions.emplace(
                                    aps.uuid);
                                }
                                self->state.flush_to_disk();
                                self
                                  ->request(static_cast<index_actor>(self),
                                            caf::infinite, atom::erase_v,
                                            old_partition_ids)
                                  .then(
                                    [=](atom::done) mutable {
                                      deliver(result);
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
                      [self, rp](caf::error& e) mutable {
                        TENZIR_WARN("{} failed to finalize partition "
                                    "transformer "
                                    "output: {}",
                                    *self, e);
                        rp.deliver(std::move(e));
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
      if (self->state.active_partitions.empty()) {
        return {};
      }
      return self->state.flush();
    },
    // -- status_client_actor --------------------------------------------------
    [](atom::status, status_verbosity, duration) -> record {
      return {};
    },
  };
}

} // namespace tenzir
