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
#include "tenzir/defaults.hpp"
#include "tenzir/detail/weak_run_delayed.hpp"
#include "tenzir/error.hpp"
#include "tenzir/fbs/partition_transform.hpp"
#include "tenzir/fbs/uuid.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/partition_synopsis.hpp"
#include "tenzir/partition_transformer.hpp"
#include "tenzir/plugin/storage_policy.hpp"
#include "tenzir/plugin/store.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/uuid.hpp"

#include <flatbuffers/flatbuffers.h>

#include <utility>

namespace tenzir {

auto create_marker(const std::vector<uuid>& in, const std::vector<uuid>& out,
                   keep_original_partition keep, bool quarantine,
                   std::string_view policy_token, Option<uuid> token_input,
                   bool finalized) -> chunk_ptr {
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
  auto token_offset = flatbuffers::Offset<flatbuffers::String>{};
  if (not policy_token.empty()) {
    token_offset
      = builder.CreateString(policy_token.data(), policy_token.size());
  }
  auto token_uuid = fbs::UUID{};
  if (token_input) {
    ::memcpy(token_uuid.mutable_data()->Data(), token_input->begin(),
             uuid::num_bytes);
  }
  auto v0_offset = fbs::partition_transform::Createv0(
    builder, in_offsets, out_offsets, quarantine, token_offset,
    token_input ? &token_uuid : nullptr, finalized);
  auto transform_offset = fbs::CreatePartitionTransform(
    builder, fbs::partition_transform::PartitionTransform::v0,
    v0_offset.Union());
  fbs::FinishPartitionTransformBuffer(builder, transform_offset);
  return chunk::make(builder.Release());
}

void catalog_state::add_partition_creation_listener(
  partition_creation_listener_actor listener) {
  partition_creation_listeners.push_back(std::move(listener));
}

void catalog_state::retry_finalize_marker(std::filesystem::path marker,
                                          chunk_ptr content) {
  if (not marker_referenced(marker)) {
    // Nothing depends on the marker any more -- neither a hold nor a
    // deferred erasure naming it as its tombstone -- so it is deleted or
    // about to be; recreating it would only leave a stale file behind. The
    // deferred check matters for a tokenless replacement without a policy,
    // whose pinned inputs reference the marker through `deferred` alone.
    return;
  }
  self->mail(atom::write_v, marker, content)
    .request(filesystem, caf::infinite)
    .then(
      [](atom::ok) {
        // Finalized; replay no longer gates on output confirmation.
      },
      [this, marker = std::move(marker),
       content = std::move(content)](const caf::error& e) mutable {
        TENZIR_WARN("{} failed to finalize the transform marker at {} and "
                    "will retry: {}",
                    *self, marker, e);
        detail::weak_run_delayed(self, defaults::disposal_retry_delay,
                                 [this, marker = std::move(marker),
                                  content = std::move(content)]() mutable {
                                   retry_finalize_marker(std::move(marker),
                                                         std::move(content));
                                 });
      });
}

auto catalog_state::apply(ast::pipeline pipe,
                          std::vector<partition_info> selected,
                          keep_original_partition keep, std::string origin,
                          std::string policy_token)
  -> caf::result<partition_apply_result> {
  if (selected.empty()) {
    return caf::make_error(ec::invalid_argument, "no partitions given");
  }
  TENZIR_DEBUG("{} applies a pipeline to partitions {}", *self, selected);
  TENZIR_ASSERT(store_actor_plugin);
  auto input_partitions = std::vector<partition_info>{};
  input_partitions.reserve(selected.size());
  std::erase_if(selected, [&](const auto& entry) {
    const auto it = synopses_per_type->find(entry.schema);
    if (it != synopses_per_type->end()
        and it->second->find(entry.uuid) != it->second->end()) {
      return false;
    }
    TENZIR_WARN("{} skips unknown partition {} for pipeline {:?}", *self,
                entry.uuid, pipe);
    return true;
  });
  auto corrected_partitions = catalog_lookup_result{};
  for (const auto& partition : selected) {
    if (in_transformation.insert(partition.uuid).second) {
      corrected_partitions.candidate_infos[partition.schema]
        .partition_infos.emplace_back(partition);
      input_partitions.emplace_back(partition);
    } else {
      // Getting overlapping partitions triggers a warning, and we silently
      // ignore the partition at the cost of the transformation being less
      // efficient.
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
  // Engaged when the transform ends in a state only a restart can finish:
  // the marker is durable and some outputs may already sit in the index
  // directory, so the marker must survive to replay, and the inputs *it
  // records* must stay claimed -- transforming them again would create a
  // second lineage of the same data for the replay to resurrect next to the
  // first. Inputs the transformer never consumed have no marker entry and no
  // outputs; holding their claims would freeze them out of every rebuild,
  // compaction, and erasure until the restart.
  auto commit_at_restart
    = std::make_shared<Option<std::unordered_set<uuid>>>(None{});
  auto deliver
    = [this, rp, corrected_partitions, marker_path, commit_at_restart](
        caf::expected<partition_apply_result>&& result) mutable {
        if (not *commit_at_restart) {
          // The marker stays if a deferred erasure still needs it as a
          // tombstone.
          erase_marker_if_unreferenced(marker_path);
        }
        for (const auto& [_, candidate_info] :
             corrected_partitions.candidate_infos) {
          for (const auto& partition : candidate_info.partition_infos) {
            if (*commit_at_restart
                and (*commit_at_restart)->contains(partition.uuid)) {
              continue;
            }
            in_transformation.erase(partition.uuid);
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
      [this, deliver, keep, marker_path, commit_at_restart,
       policy_token = std::move(policy_token),
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
        // Record in-progress marker, with the policy token riding along so
        // a crash between the durable marker and the caller's on_committed
        // still lands the commit at the next startup.
        auto token_input = Option<uuid>{};
        if (not policy_token.empty() and not old_partition_ids.empty()) {
          token_input = old_partition_ids.front();
        }
        auto marker_chunk
          = create_marker(old_partition_ids, new_partition_ids, keep, false,
                          policy_token, token_input);
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
                    // A token-carrying transform holds its marker until the
                    // caller reports the policy commit flushed: until then
                    // the marker is the commit's only durable record. A
                    // token-*less* replacement -- a rebuild, a direct apply
                    // -- still calls on_replaced below, whose history update
                    // sits in the same debounce window; its marker is held
                    // too, released by the catalog itself since no caller
                    // knows about it.
                    auto held_marker = std::string{};
                    auto self_release = false;
                    if (not policy_token.empty()) {
                      ++markers_in_disposal[marker_path];
                      held_marker = marker_path.string();
                    } else if (keep == keep_original_partition::no) {
                      // Held even when no policy is configured: the lineage
                      // must survive until a policy exists to fold it, or a
                      // stale history from an earlier policy-enabled lifetime
                      // would keep naming the replaced inputs. Without a
                      // policy the release below keeps the hold for the
                      // lifetime, and the next startup carries the marker
                      // forward the same way.
                      ++markers_in_disposal[marker_path];
                      self_release = true;
                    }
                    if (keep == keep_original_partition::yes) {
                      if (not apsv.empty()) {
                        std::ignore = merge(std::move(apsv));
                      }
                      if (held_marker.empty()) {
                        deliver(partition_apply_result{
                          .input_partitions
                          = std::move(transformed_input_partitions),
                          .output_partitions = std::move(result),
                          .input_complete = input_complete,
                        });
                        return;
                      }
                      // Rewrite as finalized, mirroring the replacement
                      // branch: the outputs are merged catalog state now, and
                      // one that is later legitimately erased or transformed
                      // again must not leave this marker -- and the commit it
                      // carries for the surviving input -- unreplayable
                      // behind an output confirmation that can never succeed.
                      self
                        ->mail(atom::write_v, marker_path,
                               create_marker({}, new_partition_ids, keep, false,
                                             policy_token, token_input, true))
                        .request(filesystem, caf::infinite)
                        .then(
                          [=, this](atom::ok) mutable {
                            deliver(partition_apply_result{
                              .input_partitions
                              = std::move(transformed_input_partitions),
                              .output_partitions = std::move(result),
                              .input_complete = input_complete,
                              .marker = held_marker,
                            });
                          },
                          [=, this](const caf::error& e) mutable {
                            // The transform is committed either way, and the
                            // original marker still replays -- it merely
                            // gates on output confirmation until the rewrite
                            // lands. Retry it for as long as the hold
                            // depends on it, and report the commit, not the
                            // bookkeeping hiccup.
                            TENZIR_WARN("{} failed to finalize the transform "
                                        "marker at {} and will retry: {}",
                                        *self, marker_path, e);
                            detail::weak_run_delayed(
                              self, defaults::disposal_retry_delay,
                              [this, marker_path,
                               content = create_marker(
                                 {}, new_partition_ids, keep, false,
                                 policy_token, token_input, true)]() mutable {
                                retry_finalize_marker(marker_path,
                                                      std::move(content));
                              });
                            deliver(partition_apply_result{
                              .input_partitions
                              = std::move(transformed_input_partitions),
                              .output_partitions = std::move(result),
                              .input_complete = input_complete,
                              .marker = held_marker,
                            });
                          });
                      return;
                    }
                    auto erased = std::vector<partition_synopsis_pair>{};
                    erased.reserve(old_partition_ids.size());
                    // A replacement is not an erasure to the policy: the data
                    // lives on under the output ids, and state a rule recorded
                    // against the inputs has to follow it there -- decided
                    // over the batch as a whole, since the outputs mix every
                    // input's events.
                    if (policy) {
                      policy->on_replaced(old_partition_ids, result);
                    }
                    for (const auto& id : old_partition_ids) {
                      erased.emplace_back(id, find_synopsis(id));
                      erase(id, notify_policy::no);
                    }
                    std::ignore = merge(std::move(apsv));
                    // Both continuations below need `erased`, so it is
                    // copied into each rather than moved into one.
                    // Rewrite the marker to tombstone form. The outputs are in
                    // place, so a replay must not try to move them again; the
                    // inputs are gone from the catalog but their files may
                    // outlive this transform if a retriever still holds them,
                    // and only the tombstone keeps a crash in between from
                    // resurrecting them.
                    self
                      ->mail(atom::write_v, marker_path,
                             create_marker(old_partition_ids, new_partition_ids,
                                           keep_original_partition::no, false,
                                           policy_token, token_input, true))
                      .request(filesystem, caf::infinite)
                      .then(
                        [=, this](atom::ok) mutable {
                          for (auto& [id, synopsis] : erased) {
                            retire_erased(id, std::move(synopsis), marker_path);
                          }
                          deliver(partition_apply_result{
                            .input_partitions
                            = std::move(transformed_input_partitions),
                            .output_partitions = std::move(result),
                            .input_complete = input_complete,
                            .marker = held_marker,
                          });
                          if (self_release) {
                            release_marker_after_flush(marker_path);
                          }
                        },
                        [=, this](const caf::error& e) mutable {
                          // The inputs already left the catalog and their
                          // replacements are in, so their files have to go
                          // regardless. The rewrite failed, but the original
                          // transform marker is still on disk and replaying it
                          // erases the inputs all the same (its output renames
                          // find the outputs already in place and fail
                          // harmlessly) -- so keep the deferred erasures
                          // referencing it rather than parking them with no
                          // durable record, which a crash would turn into
                          // resurrected inputs next to their replacements.
                          TENZIR_WARN("{} failed to record the erasure of the "
                                      "transformed partitions at {} and will "
                                      "retry: {}",
                                      *self, marker_path, e);
                          detail::weak_run_delayed(
                            self, defaults::disposal_retry_delay,
                            [this, marker_path,
                             content = create_marker(
                               old_partition_ids, new_partition_ids,
                               keep_original_partition::no, false, policy_token,
                               token_input, true)]() mutable {
                              retry_finalize_marker(marker_path,
                                                    std::move(content));
                            });
                          for (auto& [id, synopsis] : erased) {
                            retire_erased(id, std::move(synopsis), marker_path);
                          }
                          // The transform itself is committed: the outputs are
                          // merged and the inputs have left the catalog. An
                          // error here would make the caller record a failure
                          // -- no watermark on the outputs, the input
                          // blacklisted -- and a later pass could re-apply a
                          // non-idempotent pipeline to data it already
                          // processed. The failed rewrite costs only a
                          // replayable marker, so report the commit.
                          deliver(partition_apply_result{
                            .input_partitions
                            = std::move(transformed_input_partitions),
                            .output_partitions = std::move(result),
                            .input_complete = input_complete,
                            .marker = held_marker,
                          });
                          if (self_release) {
                            release_marker_after_flush(marker_path);
                          }
                        });
                  },
                  [deliver, commit_at_restart, old_partition_ids,
                   this](caf::error& e) mutable {
                    // Some outputs may already have moved into the index
                    // directory. Deleting the marker now would leave them for
                    // the startup scan to discover next to the still-cataloged
                    // inputs -- duplicate data after a restart. Keeping the
                    // marker makes the restart replay finish the transform
                    // instead, and keeping the recorded inputs claimed stops
                    // a second transform from creating another lineage of the
                    // same data in the meantime.
                    *commit_at_restart = std::unordered_set<uuid>{
                      old_partition_ids.begin(), old_partition_ids.end()};
                    TENZIR_WARN("{} failed to finalize partition transformer "
                                "output and defers the transform to the next "
                                "restart: {}",
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
