//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

// The partition transform protocol.
//
// clang-format off
//
//   atom::apply, transform              spawn()
// ---------------------------> catalog -----------> partition_transformer
//                                                                    |
//                                                                    \--------------> write index/markers/188427dd-1577-4b2a-b99c-09e91d1c167f
//                                                                    \--------------> write index/markers/188427dd-1577-4b2a-b99c-09e91d1c167f.mdx
//                                                                    |
//                                                                  [...] (2 files per output partition)
//                                      vector<partition_synopsis>    |
//                            catalog <-------------------------------/
//                            |     | -----|
//                                         | write index/markers/{transform_id}.marker
//                                         | (contains list of input and output partitions)
//                            |     | <----/
//                            |     | ~~~~~|
//                                         | atom::move (move output partitions from index/markers/ to index/ )
//                                         | merge or replace the catalog entries
//                                         | atom::erase (for every input partition)
//   atom::done               |     |<~~~~~/
// <--------------------------|     |
//                                  |------|
//                                         |
//                                         | erase index/markers/{transform_id}.marker
//                                    <----/
//
// On startup we first go through the `index/markers/` directory and finish up
// the work recorded in any existing marker files.
//
// clang-format on

#include "tenzir/catalog.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/error.hpp"
#include "tenzir/fbs/partition_transform.hpp"
#include "tenzir/fbs/uuid.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/partition_transformer.hpp"
#include "tenzir/plugin/store.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/uuid.hpp"

#include <flatbuffers/flatbuffers.h>

#include <utility>

namespace tenzir {

namespace {

auto create_marker(const std::vector<uuid>& in, const std::vector<uuid>& out,
                   keep_original_partition keep) -> chunk_ptr {
  auto builder = flatbuffers::FlatBufferBuilder{};
  auto in_offsets
    = flatbuffers::Offset<flatbuffers::Vector<const fbs::UUID*>>{};
  if (keep == keep_original_partition::no) {
    in_offsets = builder.CreateVectorOfStructs<fbs::UUID>(
      in.size(), [&in](size_t i, fbs::UUID* vec) {
        ::memcpy(vec->mutable_data()->Data(), in[i].begin(), uuid::num_bytes);
      });
  }
  auto out_offsets = builder.CreateVectorOfStructs<fbs::UUID>(
    out.size(), [&out](size_t i, fbs::UUID* vec) {
      ::memcpy(vec->mutable_data()->Data(), out[i].begin(), uuid::num_bytes);
    });
  auto v0_offset
    = fbs::partition_transform::Createv0(builder, in_offsets, out_offsets);
  auto transform_offset = fbs::CreatePartitionTransform(
    builder, fbs::partition_transform::PartitionTransform::v0,
    v0_offset.Union());
  fbs::FinishPartitionTransformBuffer(builder, transform_offset);
  return chunk::make(builder.Release());
}

} // namespace

void catalog_state::add_partition_creation_listener(
  partition_creation_listener_actor listener) {
  partition_creation_listeners.push_back(std::move(listener));
}

auto catalog_state::apply(ast::pipeline pipe,
                          std::vector<partition_info> selected,
                          keep_original_partition keep, std::string origin)
  -> caf::result<partition_apply_result> {
  if (selected.empty()) {
    return caf::make_error(ec::invalid_argument, "no partitions given");
  }
  TENZIR_DEBUG("{} applies a pipeline to partitions {}", *self, selected);
  TENZIR_ASSERT(store_actor_plugin);
  auto input_partitions = std::vector<partition_info>{};
  input_partitions.reserve(selected.size());
  std::erase_if(selected, [&](const auto& entry) {
    const auto it = synopses_per_type.find(entry.schema);
    if (it != synopses_per_type.end() and it->second.contains(entry.uuid)) {
      return false;
    }
    TENZIR_WARN("{} skips unknown partition {} for pipeline {:?}", *self,
                entry.uuid, pipe);
    return true;
  });
  auto corrected_partitions = catalog_lookup_result{};
  for (const auto& partition : selected) {
    if (partitions_in_transformation.insert(partition.uuid).second) {
      corrected_partitions.candidate_infos[partition.schema]
        .partition_infos.emplace_back(partition);
      input_partitions.emplace_back(partition);
    } else {
      // Getting overlapping partitions triggers a warning, and we silently
      // ignore the partition at the cost of the transformation being less
      // efficient.
      // TODO: Implement some synchronization mechanism for partition erasure
      // so rebuild, compaction, and aging can properly synchronize.
      TENZIR_WARN("{} refuses to apply transformation '{:?}' to partition {} "
                  "because it is currently being transformed",
                  *self, pipe, partition.uuid);
    }
  }
  if (corrected_partitions.empty()) {
    return partition_apply_result{};
  }
  /// Yummy. Partitioned Foam. :)
  auto transformer
    = self->spawn(partition_transformer,
                  std::string{store_actor_plugin->name()}, synopsis_opts,
                  index_opts, filesystem, std::move(input_partitions), pipe,
                  paths.partition_template(), paths.archive_dir,
                  paths.transformer_partition_template(),
                  paths.transformer_synopsis_template(), std::move(origin));
  /// Monitor the actor to remove it from the collection of active
  /// transformers.
  auto transformer_addr = transformer->address();
  auto completion_disposable
    = self->monitor(transformer, [this, transformer_addr](const caf::error&) {
        const auto it = active_transformers.find(transformer_addr);
        TENZIR_ASSERT(it != active_transformers.end());
        active_transformers.erase(it);
      });
  const auto [_, inserted] = active_transformers.try_emplace(
    std::move(transformer_addr), std::move(completion_disposable));
  TENZIR_ASSERT(inserted);
  auto marker_path = paths.marker(uuid::random());
  auto rp = self->make_response_promise<partition_apply_result>();
  auto deliver = [this, rp, corrected_partitions, marker_path](
                   caf::expected<partition_apply_result>&& result) mutable {
    // Erase errors don't matter too much here, leftover in-progress transforms
    // will be cleaned up on next startup.
    self->mail(atom::erase_v, marker_path)
      .request(filesystem, caf::infinite)
      .then([](atom::done) { /* nop */ },
            [this, marker_path](const caf::error& e) {
              TENZIR_DEBUG("{} failed to erase in-progress marker at {}: {}",
                           *self, marker_path, e);
            });
    for (const auto& [_, candidate_info] :
         corrected_partitions.candidate_infos) {
      for (const auto& partition : candidate_info.partition_infos) {
        partitions_in_transformation.erase(partition.uuid);
      }
    }
    if (result) {
      rp.deliver(std::move(*result));
    } else {
      rp.deliver(std::move(result.error()));
    }
  };
  // TODO: Implement some kind of monadic composition instead of these nested
  // requests.
  self->mail(atom::persist_v)
    .request(transformer, caf::infinite)
    .then(
      [this, deliver, keep, marker_path,
       transformer](partition_transformer_result& transform_result) mutable {
        auto old_partition_ids = std::vector<uuid>{};
        old_partition_ids.reserve(transform_result.input_partitions.size());
        for (const auto& partition : transform_result.input_partitions) {
          old_partition_ids.emplace_back(partition.uuid);
        }
        auto apsv = std::move(transform_result.output_partitions);
        // Point each output synopsis at its final `.mdx` path (the marker is
        // renamed there before the merge below). With lazy sketches the
        // catalog drops the Bloom filters on merge and reloads them on demand
        // from this path, so it must be set or pruning would be lost for
        // transformed/rebuilt partitions until the next restart.
        for (auto& aps : apsv) {
          if (aps.synopsis) {
            aps.synopsis.unshared().sketches_file.url
              = fmt::format("file://{}", paths.synopsis(aps.uuid).string());
          }
        }
        auto new_partition_ids = std::vector<uuid>{};
        new_partition_ids.reserve(apsv.size());
        for (auto const& [uuid, _] : apsv) {
          new_partition_ids.push_back(uuid);
        }
        auto result = std::vector<partition_info>{};
        for (auto const& aps : apsv) {
          // If synopsis was null (ie. all events were deleted), the partition
          // transformer should not have included it in the result.
          TENZIR_ASSERT(aps.synopsis);
          result.emplace_back(aps.uuid, *aps.synopsis);
        }
        auto transformed_input_partitions
          = std::move(transform_result.input_partitions);
        auto input_complete = transform_result.input_complete;
        // Record in-progress marker.
        auto marker_chunk
          = create_marker(old_partition_ids, new_partition_ids, keep);
        self->mail(atom::write_v, marker_path, marker_chunk)
          .request(filesystem, caf::infinite)
          .then(
            [=, this, apsv = std::move(apsv)](atom::ok) mutable {
              // Move the written partitions from the `markers/` directory into
              // the regular index directory.
              auto renames = std::vector<
                std::pair<std::filesystem::path, std::filesystem::path>>{};
              for (auto const& aps : apsv) {
                renames.emplace_back(paths.transformer_partition(aps.uuid),
                                     paths.partition(aps.uuid));
                renames.emplace_back(paths.transformer_synopsis(aps.uuid),
                                     paths.synopsis(aps.uuid));
              }
              self->mail(atom::move_v, std::move(renames))
                .request(filesystem, caf::infinite)
                .then(
                  // Delete input partitions if necessary.
                  [=, this, apsv = std::move(apsv)](atom::done) mutable {
                    // Merging here instead of going through the `atom::merge`
                    // handler keeps the partition creation listeners tied to
                    // ingest; they are not notified about transform outputs.
                    if (keep == keep_original_partition::yes) {
                      if (not apsv.empty()) {
                        std::ignore = merge(std::move(apsv));
                      }
                      deliver(partition_apply_result{
                        .input_partitions
                        = std::move(transformed_input_partitions),
                        .output_partitions = std::move(result),
                        .input_complete = input_complete,
                      });
                      return;
                    }
                    for (const auto& id : old_partition_ids) {
                      erase(id);
                    }
                    std::ignore = merge(std::move(apsv));
                    self->mail(atom::erase_v, old_partition_ids)
                      .request(caf::actor_cast<catalog_actor>(self),
                               caf::infinite)
                      .then(
                        [=](atom::done) mutable {
                          deliver(partition_apply_result{
                            .input_partitions
                            = std::move(transformed_input_partitions),
                            .output_partitions = std::move(result),
                            .input_complete = input_complete,
                          });
                        },
                        [=](const caf::error& e) mutable {
                          deliver(e);
                        });
                  },
                  [deliver, this](caf::error& e) mutable {
                    TENZIR_WARN("{} failed to finalize partition transformer "
                                "output: {}",
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
}

} // namespace tenzir
