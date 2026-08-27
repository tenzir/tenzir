//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/actors.hpp"
#include "tenzir/catalog.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin/base.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/uuid.hpp"

#include <filesystem>
#include <functional>

#include <any>
#include <memory>
#include <string>
#include <vector>

namespace tenzir {

// -- storage policy ----------------------------------------------------------

/// What to do with a partition.
struct storage_action {
  /// The pipeline to run over the partition.
  ast::pipeline pipeline = {};

  /// Whether the input survives the transform.
  keep_original_partition keep = keep_original_partition::no;

  /// Names what asked for this, for logs and metrics.
  std::string origin = {};

  /// Opaque to the catalog, which only stores it and hands it back on commit
  /// or failure. The policy casts it back to record what it asked for, for
  /// example a rule id and the watermark it processed up to.
  std::any token = {};
};

/// What a policy is given when the catalog builds it.
///
/// A policy has no actor context of its own, so persistence is expressed as
/// operations the catalog performs on its behalf, through the filesystem
/// actor and therefore relative to the database directory. Every callback
/// runs on the catalog's thread, so an implementation may touch its own state
/// from them without synchronizing.
struct storage_policy_context {
  /// Reads a file below the database directory. The chunk handed to the
  /// callback is null when the file does not exist or could not be read.
  std::function<void(std::filesystem::path, std::function<void(chunk_ptr)>)>
    read = {};

  /// Writes a file below the database directory.
  std::function<void(std::filesystem::path, chunk_ptr,
                     std::function<void(caf::error)>)>
    write = {};

  /// Schedules work on the catalog's thread, for debouncing writes.
  std::function<void(duration, std::function<void()>)> run_delayed = {};
};

/// Decides what happens to individual partitions.
///
/// Every method answers about **one** partition. A policy never receives or
/// holds a partition set: the catalog does all iterating, batching, ordering,
/// and scheduling, so nothing it decides can go stale between the decision and
/// the work. The catalog calls these from its own actor context, so an
/// implementation must not block -- it reads its own state, answers, and
/// returns.
class storage_policy {
public:
  virtual ~storage_policy() = default;

  /// How often the catalog should run the maintenance pass for this policy.
  /// Zero, the default, disables the pass -- a policy that only contributes
  /// eviction behavior has no periodic work to do.
  ///
  /// The policy owns this because it owns what the pass is *for*: the catalog
  /// cannot know that a rule set implies an hourly sweep.
  virtual auto maintenance_interval() const -> duration;

  /// Periodic, age-driven maintenance. Called while the catalog walks its own
  /// partitions; `none` means "nothing to do for this partition".
  ///
  /// The argument is the synopsis rather than `partition_info` because a rule
  /// may select on a time field of its own, whose range lives in the
  /// synopsis's field-level time synopses. A policy that checks the range here
  /// keeps the catalog from rewriting partitions that hold no matching events.
  virtual auto maintenance_action(const uuid& partition,
                                  const partition_synopsis& synopsis) const
    -> Option<storage_action>;

  /// Eviction ordering when over the disk budget. Higher is evicted sooner;
  /// `none` keeps the catalog's default of oldest `max_import_time` first.
  virtual auto eviction_weight(const uuid& partition,
                               const partition_synopsis& synopsis) const
    -> Option<double>;

  /// What to do with a partition being evicted. `none` means erase it.
  virtual auto eviction_action(const uuid& partition,
                               const partition_synopsis& synopsis) const
    -> Option<storage_action>;

  /// A partition arrived from ingest. Transform outputs do not come through
  /// here -- they arrive at `on_committed` -- so that a policy keeping
  /// per-partition state is not fed the same data twice.
  virtual void on_merged(const partition_synopsis_pair& partition);

  /// A transform replaced `input` with `outputs`. The data lives on under new
  /// ids, so a policy keeping per-partition state carries that state over
  /// rather than dropping it -- `on_erased` is *not* called for a replaced
  /// input, because treating a replacement as an erasure would make the
  /// outputs look untouched to every rule that already processed the input.
  /// Fires for every replacement, whether or not the policy asked for the
  /// transform; a policy-initiated one additionally gets `on_committed`.
  virtual void on_replaced(const uuid& input,
                           const std::vector<partition_info>& outputs);

  /// A transform the policy asked for committed its outputs.
  virtual void on_committed(const std::any& token, const uuid& input,
                            const std::vector<partition_info>& outputs);

  /// A partition left the catalog.
  virtual void on_erased(const uuid& partition);

  /// A transform the policy asked for failed.
  virtual void
  on_failed(const std::any& token, const uuid& input, const caf::error& error);
};

// -- storage policy plugin ---------------------------------------------------

/// A base class for plugins that contribute storage maintenance policy.
/// @relates plugin
class storage_policy_plugin : public virtual plugin {
public:
  /// Builds the policy. Called once, when the catalog starts. Returning
  /// `nullptr` disables this plugin's policy, which is what an unconfigured
  /// implementation returns.
  /// @note This runs in the actor context of the CATALOG.
  virtual auto make_storage_policy(storage_policy_context context) const
    -> std::unique_ptr<storage_policy>
    = 0;
};

} // namespace tenzir
