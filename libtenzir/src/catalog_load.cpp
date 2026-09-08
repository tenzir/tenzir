//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The catalog rebuilds itself from the database directory at startup. It first
// finishes up any transform that was interrupted by the last shutdown (see
// `catalog_transform.cpp` for the marker protocol), then scans the index
// directory and loads one synopsis per partition.

#include "tenzir/catalog.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/concept/parseable/tenzir/uuid.hpp"
#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/fanout_counter.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/error.hpp"
#include "tenzir/fbs/partition.hpp"
#include "tenzir/fbs/partition_synopsis.hpp"
#include "tenzir/fbs/partition_transform.hpp"
#include "tenzir/fbs/utils.hpp"
#include "tenzir/flatbuffer.hpp"
#include "tenzir/io/read.hpp"
#include "tenzir/io/save.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/passive_partition.hpp"
#include "tenzir/try.hpp"
#include "tenzir/version.hpp"

#include <caf/make_copy_on_write.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <map>
#include <span>
#include <thread>
#include <unordered_set>
#include <vector>

namespace tenzir {

namespace {

/// Tests whether the bytes 4-8 of the given file equal `identifier`.
auto test_file_identifier(const std::filesystem::path& file,
                          const char* identifier) -> bool {
  std::byte buffer[8];
  if (auto err = io::read(file, std::span<std::byte>{buffer, sizeof(buffer)});
      err.valid()) {
    return false;
  }
  return std::memcmp(buffer + 4, identifier, 4) == 0;
}

/// Resolves a base directory once instead of paying a `canonical()` (a
/// symlink-resolving `realpath`, i.e. several stats) per partition; the
/// per-partition file names are appended to the resolved base. This matters on
/// networked storage where each such call is a round-trip.
auto resolve_dir(const std::filesystem::path& p) -> std::filesystem::path {
  auto ec = std::error_code{};
  if (auto result = std::filesystem::canonical(p, ec); not ec) {
    return result;
  }
  ec.clear();
  if (auto result = std::filesystem::absolute(p, ec); not ec) {
    return result.lexically_normal();
  }
  return p.lexically_normal();
}

} // namespace

auto extract_partition_synopsis(
  const std::filesystem::path& partition_path,
  const std::filesystem::path& partition_synopsis_path, bool verify)
  -> caf::error {
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
      fmt::format("unknown version {} for partition at {}",
                  static_cast<uint8_t>(partition->partition_type()),
                  partition_path));
  }
  const auto* partition_legacy = partition->partition_as_legacy();
  TENZIR_ASSERT(partition_legacy);
  auto ps = partition_synopsis{};
  if (auto error = unpack(*partition_legacy, ps); error.valid()) {
    return error;
  }
  auto builder = flatbuffers::FlatBufferBuilder{};
  auto ps_offset = pack(builder, ps);
  if (not ps_offset) {
    return ps_offset.error();
  }
  auto ps_builder = fbs::PartitionSynopsisBuilder{builder};
  ps_builder.add_partition_synopsis_type(
    fbs::partition_synopsis::PartitionSynopsis::legacy);
  ps_builder.add_partition_synopsis(ps_offset->Union());
  auto flatbuffer = ps_builder.Finish();
  fbs::FinishPartitionSynopsisBuffer(builder, flatbuffer);
  auto chunk_out = fbs::release(builder);
  // Verify the freshly built buffer when callers intend to read it back
  // without verification, so a corrupt synopsis is caught at the source.
  if (verify) {
    if (auto checked = ::tenzir::flatbuffer<fbs::PartitionSynopsis>::make(
          chunk_ptr{chunk_out});
        not checked) {
      return caf::make_error(
        ec::format_error,
        fmt::format("refusing to write malformed partition synopsis to {}: {}",
                    partition_synopsis_path, checked.error()));
    }
  }
  return io::save(partition_synopsis_path,
                  std::span{chunk_out->data(), chunk_out->size()});
}

auto catalog_state::replay_markers()
  -> caf::expected<std::unordered_set<uuid>> {
  auto replayed_inputs = std::unordered_set<uuid>{};
  auto err = std::error_code{};
  if (not std::filesystem::is_directory(paths.markers_dir, err)) {
    if (err and err != std::errc::no_such_file_or_directory) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("failed to inspect transform markers "
                                         "at {}: {}",
                                         paths.markers_dir, err.message()));
    }
    return replayed_inputs;
  }
  auto marker_iter
    = std::filesystem::directory_iterator(paths.markers_dir, err);
  if (err) {
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to list transform markers at "
                                       "{}: {}",
                                       paths.markers_dir, err.message()));
  }
  // Files in the marker directory that an unfinished replay still needs; the
  // stray sweep at the end must not remove them.
  auto protected_files = std::unordered_set<std::string>{};
  for (const auto& entry : marker_iter) {
    if (entry.path().extension() != ".marker") {
      continue;
    }
    auto chunk = chunk::mmap(entry.path());
    if (not chunk) {
      return caf::make_error(ec::filesystem_error,
                             fmt::format("cannot read transform marker at {}: "
                                         "{}; repair it before "
                                         "restarting",
                                         entry.path(), chunk.error()));
    }
    auto maybe_flatbuffer
      = flatbuffer<fbs::PartitionTransform>::make(std::move(*chunk));
    if (not maybe_flatbuffer) {
      return caf::make_error(
        ec::format_error,
        fmt::format("malformed transform marker at {}: {}; repair it before "
                    "restarting",
                    entry.path(), maybe_flatbuffer.error()));
    }
    auto& transform_flatbuffer = *maybe_flatbuffer;
    if (transform_flatbuffer->transform_type()
        != fbs::partition_transform::PartitionTransform::v0) {
      return caf::make_error(ec::format_error,
                             fmt::format("unknown transform marker version at "
                                         "{}; repair it before "
                                         "restarting",
                                         entry.path()));
    }
    const auto* transform_v0 = transform_flatbuffer->transform_as_v0();
    if (not transform_v0) {
      return caf::make_error(ec::format_error,
                             fmt::format("missing transform marker payload at "
                                         "{}; repair it before "
                                         "restarting",
                                         entry.path()));
    }
    const auto quarantine = transform_v0->quarantine();
    const auto finalized = transform_v0->finalized();
    // A preserve-input marker omits the input vector entirely -- the schema
    // allows it, and create_marker writes it only for replacements -- so the
    // accessor can be null.
    auto marker_input_ids = std::vector<uuid>{};
    if (const auto* marker_inputs = transform_v0->input_partitions()) {
      marker_input_ids.reserve(marker_inputs->size());
      for (const auto* id : *marker_inputs) {
        marker_input_ids.push_back(uuid::from_flatbuffer(*id));
      }
    }
    // A quarantined partition is sentenced no matter how far the file
    // operations got: its store is corrupt, and scanning it back in would
    // only resurrect the failing reads the quarantine exists to end. A
    // *transform's* inputs join the exclusion set further down, only once
    // every output is confirmed -- see there.
    if (quarantine) {
      for (const auto& input : marker_input_ids) {
        replayed_inputs.insert(input);
      }
    }
    // Outputs first, and confirmed: the inputs may only die once every
    // output *partition* is verifiably at its destination -- a successful
    // rename, or a destination that is already there from a previous replay
    // or the transform itself. Anything less, including a source and
    // destination that are both invisible or an `exists` that itself fails,
    // keeps the marker and the inputs: erasing them on an unconfirmed output
    // would destroy the only copy. Synopses are the exception -- the
    // transformer skips writing null ones, and the scan regenerates a missing
    // one from its partition file -- so their placement never blocks.
    auto outputs_in_place = true;
    // A finalized marker's outputs are already merged catalog state;
    // an output that has since vanished did so legitimately (erased or
    // re-transformed), so their placement is not confirmed -- only the
    // input erasures and the policy payload remain to replay.
    if (not finalized) {
      for (const auto* id : *transform_v0->output_partitions()) {
        const auto output = uuid::from_flatbuffer(*id);
        {
          auto ec = std::error_code{};
          std::filesystem::rename(paths.transformer_partition(output),
                                  paths.partition(output), ec);
          if (ec) {
            ec.clear();
            const auto in_place
              = std::filesystem::exists(paths.partition(output), ec);
            if (ec or not in_place) {
              TENZIR_WARN("{} keeps the marker at {} because output partition "
                          "{} is not confirmed at {}",
                          *self, entry.path(), output, paths.partition(output));
              outputs_in_place = false;
            }
          }
        }
        {
          auto ec = std::error_code{};
          std::filesystem::rename(paths.transformer_synopsis(output),
                                  paths.synopsis(output), ec);
          // Tolerated in every failure mode; see above.
        }
      }
    }
    if (not outputs_in_place) {
      // Try again at the next startup. Until then the *inputs* stay
      // queryable -- their files are intact and no erasure runs while the
      // outputs are unconfirmed -- and the outputs stay invisible: excluding
      // any that already moved keeps the scan from loading them next to the
      // inputs whose events they duplicate, and the protected transformer
      // files keep the rest replayable.
      for (const auto* id : *transform_v0->output_partitions()) {
        const auto output = uuid::from_flatbuffer(*id);
        replayed_inputs.insert(output);
        protected_files.insert(
          paths.transformer_partition(output).filename().string());
        protected_files.insert(
          paths.transformer_synopsis(output).filename().string());
      }
      continue;
    }
    // Every output is confirmed, so the erasures below run now -- or, if one
    // fails, at the next startup. Only from this point on must the scan skip
    // the inputs: loading them while their files disappear would leave
    // phantoms in memory next to their finalized replacements.
    {
      auto replayed = replayed_transform{};
      for (const auto& input : marker_input_ids) {
        replayed_inputs.insert(input);
        replayed.inputs.push_back(input);
      }
      if (const auto* token = transform_v0->policy_token()) {
        replayed.policy_token = token->str();
      }
      if (const auto* token_input = transform_v0->token_input()) {
        replayed.token_input = uuid::from_flatbuffer(*token_input);
      }
      replayed.erasure
        = quarantine
          or (not finalized and transform_v0->output_partitions()->size() == 0
              and replayed.policy_token.empty() and not replayed.token_input);
      // This replay may be finishing a transform whose policy callbacks the
      // crash cut off. Hand the replacement -- and the interrupted commit,
      // when the marker carries a token -- to the policy once it exists, so
      // its history follows the data to the current ids. A preserve-input
      // transform has no inputs to replace but still commits.
      if (not replayed.inputs.empty() or not replayed.policy_token.empty()
          or replayed.token_input) {
        // The marker is the interrupted replacement's -- and, with a token,
        // the interrupted commit's -- only durable record. It may go only
        // after the policy has that state on disk *and* every recorded input
        // erasure completed, so it takes one disposal reference per
        // obligation and disappears when the last one releases.
        replayed.marker = entry.path();
        ++markers_in_disposal[entry.path()];
        protected_files.insert(entry.path().filename().string());
        for (const auto* id : *transform_v0->output_partitions()) {
          auto info = partition_info{};
          info.uuid = uuid::from_flatbuffer(*id);
          replayed.outputs.push_back(std::move(info));
        }
        replayed_transforms.push_back(std::move(replayed));
      }
    }
    // Erase the inputs' files directly, and delete the marker only once every
    // one of them is gone. Deleting the marker up front would lose the durable
    // record while the erasures are outstanding -- a crash or a failed
    // deletion would then resurrect the inputs at the next startup. Going
    // through a passive partition instead would not do: its erase handler
    // acknowledges after the store deletion alone and only fire-and-forgets
    // the partition file, exactly the file whose survival resurrects the
    // data. A marker that stays behind replays harmlessly: erasing files that
    // are already gone succeeds, so retries converge.
    if (quarantine) {
      // A quarantine preserves the store for inspection: move it aside before
      // any erasure, exactly like the runtime path. A destination that is
      // already there is a finished move from a previous replay, and the
      // leftover source -- if any -- is a duplicate whose removal converges.
      auto move_failed = false;
      for (const auto& input : marker_input_ids) {
        for (const auto* extension : {"store", "feather", "parquet"}) {
          const auto source
            = paths.archive_dir / fmt::format("{}.{}", input, extension);
          auto ec = std::error_code{};
          const auto present = std::filesystem::exists(source, ec);
          if (ec) {
            move_failed = true;
            continue;
          }
          if (not present) {
            continue;
          }
          const auto destination
            = paths.archive_dir / "quarantined" / source.filename();
          std::filesystem::create_directories(destination.parent_path(), ec);
          if (ec) {
            move_failed = true;
            continue;
          }
          const auto moved = std::filesystem::exists(destination, ec);
          if (ec) {
            move_failed = true;
            continue;
          }
          if (moved) {
            std::filesystem::remove(source, ec);
            if (ec) {
              move_failed = true;
            }
            continue;
          }
          std::filesystem::rename(source, destination, ec);
          if (ec) {
            move_failed = true;
          }
        }
      }
      if (move_failed) {
        // Keep the unfinished move independent of the policy-flush hold. Only
        // a later startup can confirm that the quarantined store was moved.
        ++markers_in_disposal[entry.path()];
        TENZIR_WARN("{} keeps the marker at {} because a quarantine move it "
                    "records did not complete",
                    *self, entry.path());
        continue;
      }
    }
    auto pending_files = std::vector<std::filesystem::path>{};
    auto probe_failed = false;
    for (const auto& input : marker_input_ids) {
      auto probe = [&](const std::filesystem::path& path) {
        auto ec = std::error_code{};
        const auto present = std::filesystem::exists(path, ec);
        if (ec) {
          // Absence must be *confirmed*: mistaking a transient I/O failure
          // for a missing file could erase the marker while the unprobed
          // input survives, and the next startup would resurrect it.
          probe_failed = true;
          return;
        }
        if (present) {
          pending_files.push_back(path);
        }
      };
      probe(paths.partition(input));
      probe(paths.synopsis(input));
      // A quarantined store was moved aside above; only an erasure deletes
      // it. No known store implementation deviates from the default path
      // scheme; the startup scan and disposal rely on the same assumption.
      if (not quarantine) {
        for (const auto* extension : {"store", "feather", "parquet"}) {
          probe(paths.archive_dir / fmt::format("{}.{}", input, extension));
        }
      }
    }
    const auto marker_path = entry.path();
    const auto marker_retained
      = not replayed_transforms.empty()
        and replayed_transforms.back().marker == marker_path;
    if (probe_failed) {
      TENZIR_WARN("{} keeps the marker at {} because it could not confirm "
                  "which input files remain",
                  *self, marker_path);
      if (marker_retained) {
        // An unconfirmed input is an erasure obligation like a failed
        // deletion: without this reference, the policy flush would release
        // the marker's only hold and delete it, and the next restart would
        // load the surviving input next to its finalized outputs. The
        // reference is deliberately never released this lifetime; the next
        // startup's replay probes again.
        ++markers_in_disposal[marker_path];
      }
      continue;
    }
    if (pending_files.empty()) {
      if (not marker_retained) {
        std::filesystem::remove(marker_path, err);
      }
      continue;
    }
    if (marker_retained) {
      // The erasures below are an obligation of their own: releasing the
      // marker on the policy flush alone could delete it while a failed
      // input deletion still needs the replay, and the next startup would
      // scan the surviving input back in next to its replacement.
      ++markers_in_disposal[marker_path];
    }
    auto counter = detail::make_fanout_counter(
      pending_files.size(),
      [this, marker_path, marker_retained]() {
        if (marker_retained) {
          // Every recorded file is gone; drop the erasure reference. The
          // flush reference keeps the marker alive as long as the policy
          // still needs it.
          release_marker_hold(marker_path);
          return;
        }
        self->mail(atom::erase_v, marker_path)
          .request(filesystem, caf::infinite)
          .then([](atom::done) { /* nop */ },
                [self = self, marker_path](const caf::error& e) {
                  // Replayed again at the next startup; harmless.
                  TENZIR_DEBUG("{} failed to erase marker at {}: {}", *self,
                               marker_path, e);
                });
      },
      [this, marker_path](caf::error&& e) {
        TENZIR_WARN("{} keeps the marker at {} because an erasure it records "
                    "did not complete: {}",
                    *self, marker_path, e);
      });
    for (const auto& file : pending_files) {
      self->mail(atom::erase_v, file)
        .request(filesystem, caf::infinite)
        .then(
          [counter](atom::done) {
            counter->receive_success();
          },
          [this, file, counter](caf::error& e) {
            TENZIR_WARN("{} failed to erase {} during startup: {}", *self, file,
                        e);
            counter->receive_error(std::move(e));
          });
    }
  }
  // Everything else in the marker directory is an orphan: outputs of a
  // transform that died before writing its marker, which nothing references.
  // The markers themselves stay until their replay completes above, and the
  // outputs of a replay that could not finish stay with them.
  // TODO: This does not handle store files, which may already have been
  // written. Since a store file may also be written before the partition
  // itself, there does not currently seem to be a bulletproof way of handling
  // this.
  for (const auto& stray :
       std::filesystem::directory_iterator(paths.markers_dir, err)) {
    if (stray.path().extension() != ".marker"
        and not protected_files.contains(stray.path().filename().string())) {
      std::filesystem::remove_all(stray.path(), err);
    }
  }
  return replayed_inputs;
}

auto catalog_state::load_from_disk() -> caf::error {
  // We don't use the filesystem actor here because this runs once during
  // startup, before the catalog installs its behavior.
  auto err = std::error_code{};
  if (not std::filesystem::exists(paths.index_dir, err)) {
    TENZIR_VERBOSE("{} found no prior state, starting with a clean slate",
                   *self);
    return caf::none;
  }
  // Start by finishing up any in-progress transforms.
  const auto replayed_inputs = replay_markers();
  if (not replayed_inputs) {
    return replayed_inputs.error();
  }
  auto dir_iter = std::filesystem::directory_iterator(paths.index_dir, err);
  if (err) {
    return caf::make_error(ec::filesystem_error,
                           fmt::format("failed to list directory contents of "
                                       "{}: {}",
                                       paths.index_dir, err.message()));
  }
  auto partition_ids = std::vector<uuid>{};
  auto synopsis_files = std::vector<uuid>{};
  // Partition index file sizes captured during the scan so that the load below
  // does not need an additional stat per partition.
  auto partition_index_sizes = std::unordered_map<uuid, uint64_t>{};
  for (const auto& entry : dir_iter) {
    const auto stem = entry.path().stem();
    auto partition_uuid = uuid{};
    // Ignore files that don't use UUID for the filename.
    if (not parsers::uuid(stem.string(), partition_uuid)) {
      continue;
    }
    const auto ext = entry.path().extension();
    if (ext == std::filesystem::path{".mdx"}) {
      synopsis_files.push_back(partition_uuid);
      continue;
    }
    if (not ext.empty()) {
      continue;
    }
    auto size_err = std::error_code{};
    const auto file_size = entry.file_size(size_err);
    // Newer partitions are not limited to FLATBUFFERS_MAX_BUFFER_SIZE, this is
    // only a problem for older ones that still have `fbs::Partition` as root
    // type. We cannot load those at all, so we skip them entirely.
    if (not size_err and file_size >= FLATBUFFERS_MAX_BUFFER_SIZE
        and test_file_identifier(entry, fbs::PartitionIdentifier())) {
      const auto store_path
        = paths.archive_dir / fmt::format("{:u}.store", partition_uuid);
      if (not std::filesystem::exists(store_path, err)) {
        TENZIR_WARN("{} did not find a store file for the oversized partition "
                    "{} and won't attempt to recover the data",
                    *self, partition_uuid);
      }
      continue;
    }
    if (replayed_inputs->contains(partition_uuid)) {
      // A marker sentences this partition to erasure; the deletion runs
      // asynchronously (or, after a failure, at the next startup). Loading it
      // now would resurrect it in memory next to its finalized replacements
      // while its files disappear underneath.
      continue;
    }
    partition_ids.push_back(partition_uuid);
    partition_index_sizes.emplace(partition_uuid,
                                  size_err ? uint64_t{0} : file_size);
  }
  std::ranges::sort(partition_ids);
  std::ranges::sort(synopsis_files);
  auto orphans = std::vector<uuid>{};
  std::ranges::set_difference(synopsis_files, partition_ids,
                              std::back_inserter(orphans));
  // Do a bit of housekeeping. MDX files without matching partitions shouldn't
  // be there in the first place.
  TENZIR_DEBUG("{} deletes {} orphaned mdx files", *self, orphans.size());
  for (const auto& orphan : orphans) {
    std::filesystem::remove(paths.index_dir / fmt::format("{}.mdx", orphan),
                            err);
  }
  // We build an in-memory representation of the archive folder for quicker
  // lookup when we add file paths and sizes to the in-memory synopsis. Sizes
  // are captured here so the load below needs no additional stat per store.
  struct store_info {
    std::filesystem::path path;
    uint64_t size = 0;
  };
  const auto store_map = [&] {
    auto result = std::map<uuid, store_info>{};
    if (not std::filesystem::is_directory(paths.archive_dir, err)) {
      return result;
    }
    for (const auto& store_file :
         std::filesystem::directory_iterator{paths.archive_dir}) {
      auto store_uuid = uuid{};
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
  const auto index_dir = resolve_dir(paths.index_dir);
  const auto synopsis_dir = resolve_dir(paths.synopsis_dir);
  const auto archive_dir = resolve_dir(paths.archive_dir);
  const auto lazy = synopsis_opts.lazy_sketches;
  const auto skip_verification = synopsis_opts.skip_synopsis_verification;
  // `synopsis_files` was scanned from the index directory, but synopses live
  // under the synopsis directory. These are the same by default; when they
  // differ we must check the actual synopsis path instead of the scan result,
  // otherwise we would regenerate every synopsis on each startup.
  const auto synopsis_in_index_dir = paths.synopsis_dir == paths.index_dir;
  // Loads a single partition synopsis from disk. This is invoked concurrently
  // from multiple worker threads below, so it must not touch shared mutable
  // state: it only reads data prepared above (all immutable during the load)
  // and the (post-initialization, immutable) synopsis factory, and produces a
  // fresh synopsis. Results are merged into the shared collections after all
  // workers have finished.
  auto load_one =
    [&](const uuid& partition_uuid) -> caf::expected<partition_synopsis_pair> {
    auto part_path = paths.partition(partition_uuid);
    auto synopsis_path = paths.synopsis(partition_uuid);
    // Generate the external partition synopsis file if it doesn't exist. In the
    // common case the synopsis lives in the scanned index directory, so we can
    // use the scan result and avoid a stat; otherwise we check the actual path.
    // When verification is skipped on read, verify the freshly written synopsis.
    const auto synopsis_exists
      = synopsis_in_index_dir
          ? std::ranges::binary_search(synopsis_files, partition_uuid)
          : std::filesystem::exists(synopsis_path);
    if (not synopsis_exists) {
      if (auto error = extract_partition_synopsis(part_path, synopsis_path,
                                                  skip_verification);
          error.valid()) {
        return error;
      }
    }
    auto read_synopsis
      = [&]() -> caf::expected<flatbuffer<fbs::PartitionSynopsis>> {
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
      if ((*maybe_flatbuffer)->partition_synopsis_type()
          != fbs::partition_synopsis::PartitionSynopsis::legacy) {
        return caf::make_error(ec::format_error, "invalid partition synopsis "
                                                 "version");
      }
      return std::move(*maybe_flatbuffer);
    };
    auto maybe_synopsis = read_synopsis();
    if (not maybe_synopsis) {
      // A corrupt synopsis is recoverable as long as the partition file is
      // intact: regenerate it the way a missing one is regenerated. Without
      // the retry the partition would silently drop out of the catalog while
      // its files stay on disk -- invisible to queries and, worse, to the
      // disk-budget eviction, which can only reclaim what the catalog knows.
      TENZIR_VERBOSE("{} regenerates the synopsis of partition {} because it "
                     "does not load: {}",
                     *self, partition_uuid, maybe_synopsis.error());
      if (auto error = extract_partition_synopsis(part_path, synopsis_path,
                                                  skip_verification);
          error.valid()) {
        return error;
      }
      maybe_synopsis = read_synopsis();
      if (not maybe_synopsis) {
        return std::move(maybe_synopsis.error());
      }
    }
    const auto ps_flatbuffer = std::move(*maybe_synopsis);
    TENZIR_ASSERT(ps_flatbuffer->partition_synopsis_as_legacy());
    auto ps
      = partition_synopsis_ptr{caf::make_copy_on_write<partition_synopsis>()};
    const auto& synopsis_legacy
      = *ps_flatbuffer->partition_synopsis_as_legacy();
    if (auto error = unpack(synopsis_legacy, ps.unshared(), lazy);
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
    const auto f = store_map.find(partition_uuid);
    if (f == store_map.end()) {
      // For completeness sake we could open the partition and look if the data
      // is somewhere else entirely, but no known implementation ever deviated
      // from the default path scheme, so we assume filesystem corruption here.
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
  // The catalog actor is detached and blocked here during startup, and no other
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
  const auto lazy_suffix
    = lazy ? std::string{" deferring Bloom-filter sketches;"} : std::string{};
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
  auto next_index = std::atomic<size_t>{0};
  auto loaded = std::atomic<size_t>{0};
  auto work = [&](size_t worker) {
    for (auto idx = next_index.fetch_add(1, std::memory_order_relaxed);
         idx < num_partitions;
         idx = next_index.fetch_add(1, std::memory_order_relaxed)) {
      const auto& partition_uuid = partition_ids[idx];
      try {
        auto result = load_one(partition_uuid);
        if (not result) {
          // Loud on purpose: the partition drops out of the catalog, but its
          // files stay on disk, where they count against the disk budget
          // without being evictable. They are deliberately not deleted -- a
          // transient I/O failure during startup must not destroy data -- so
          // the operator has to decide.
          TENZIR_WARN("{} skips partition {}, whose files remain on disk "
                      "until removed manually: {}",
                      *self, partition_uuid, result.error());
          continue;
        }
        worker_synopses[worker].push_back(std::move(*result));
      } catch (const std::exception& ex) {
        TENZIR_WARN("{} skips partition {}, whose files remain on disk until "
                    "removed manually: {}",
                    *self, partition_uuid, ex.what());
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
    for (auto worker = size_t{0}; worker < concurrency; ++worker) {
      threads.emplace_back(work, worker);
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }
  auto synopses = std::vector<partition_synopsis_pair>{};
  synopses.reserve(num_partitions);
  for (auto& batch : worker_synopses) {
    for (auto& pair : batch) {
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
  // Recommend the user to run 'tenzir-ctl rebuild' if any partition synopses
  // are outdated. We need to nudge them a bit so we can drop support for older
  // partition versions more freely.
  const auto num_outdated = std::ranges::count_if(synopses, [](const auto& x) {
    return x.synopsis->version < version::current_partition_version;
  });
  if (num_outdated > 0) {
    TENZIR_WARN("{} detected {}/{} outdated partitions; consider running "
                "'tenzir-ctl rebuild' to upgrade existing partitions in the "
                "background",
                *self, num_outdated, synopses.size());
  }
  return initialize(std::move(synopses));
}

} // namespace tenzir
