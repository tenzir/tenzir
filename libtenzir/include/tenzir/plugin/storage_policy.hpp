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
#include "tenzir/chunk.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin/base.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/uuid.hpp"

#include <any>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <variant>
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
  /// Reads a file below the database directory, blocking. This runs only
  /// during the policy's construction, which happens amid the catalog's other
  /// blocking startup reads -- and it is what makes construction settle the
  /// policy's state: nothing can mutate a history that is still on its way
  /// in. A null chunk means the file does not exist -- the ordinary first
  /// start. An error means the file is there but could not be read, which a
  /// policy must treat as "state unavailable" rather than as a clean slate.
  std::function<auto(std::filesystem::path)->caf::expected<chunk_ptr>> read
    = {};

  /// Writes a file below the database directory, blocking. Policy state
  /// files are small, and sequencing their durability against the catalog's
  /// marker lifecycle is what asynchronous writes made needlessly hard
  /// (in-flight tracking, waiter queues, flush joins). Query lookups do not
  /// run on the catalog's thread, so a stalled disk delays maintenance
  /// bookkeeping, not queries.
  std::function<auto(std::filesystem::path, chunk_ptr)->caf::error> write = {};

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

  /// The fallback age-eligibility recheck interval. Zero disables automatic
  /// age-driven actions; eviction and named runs remain independent.
  virtual auto maintenance_interval() const -> duration;

  /// Age-driven maintenance. Called while the catalog walks its own
  /// partitions; `none` means "nothing to do for this partition".
  ///
  /// The argument is the synopsis rather than `partition_info` because a rule
  /// may select on a time field of its own, whose range lives in the
  /// synopsis's field-level time synopses. A policy that checks the range here
  /// keeps the catalog from rewriting partitions that hold no matching events.
  virtual auto
  maintenance_action(const uuid& partition, const partition_synopsis& synopsis,
                     time now = time::clock::now()) const
    -> Option<storage_action>;

  /// Due or failed user policy wins over rebuild, regardless of free slots.
  virtual auto
  blocks_rebuild(const uuid& partition, const partition_synopsis& synopsis,
                 time now = time::clock::now()) const -> bool;

  /// The next time age alone can change eligibility. The default falls back
  /// to the configured interval when the policy cannot determine it exactly.
  virtual auto maintenance_deadline(const uuid& partition,
                                    const partition_synopsis& synopsis,
                                    time now) const -> time;

  /// The policy's effective configuration, for `compaction list` and its kin.
  /// The node answers this rather than the client rendering its own config: a
  /// client connected to a remote node -- or started with a different local
  /// configuration -- must see what the running node actually enforces. The
  /// default is an empty record.
  virtual auto describe() const -> record;

  /// The names of the rules this policy knows. An empty list, the default,
  /// means "cannot say", and the catalog then takes any name at face value.
  ///
  /// This exists so that a one-off run can tell a misspelled rule from a rule
  /// with nothing left to do -- the two are indistinguishable from
  /// `named_action` alone, which returns nothing either way.
  virtual auto rule_names() const -> std::vector<std::string>;

  /// Checks a named run's window overrides before the catalog walks anything.
  /// Only the policy can do this: the effective window merges the overrides
  /// with the rule's configuration, which the catalog never sees. The default
  /// accepts everything.
  virtual auto
  check_named_run(std::string_view rule, Option<duration> older_than,
                  Option<duration> newer_than) const -> caf::error;

  /// The action a *named* rule would take on a partition, outside the periodic
  /// schedule. The durations override the rule's configured window when given.
  /// `none` means the rule does not apply to this partition, or that the
  /// policy has no such rule.
  ///
  /// This exists because a one-off run is the one piece of policy control flow
  /// that does not reduce to "walk everything and ask": the caller names the
  /// rule. The catalog still does the walking and still learns nothing about
  /// what a rule is.
  virtual auto named_action(std::string_view rule, Option<duration> older_than,
                            Option<duration> newer_than, const uuid& partition,
                            const partition_synopsis& synopsis,
                            time now = time::clock::now()) const
    -> Option<storage_action>;

  /// Eviction ordering when over the disk budget. Higher is evicted sooner;
  /// `none` keeps the catalog's default of oldest `max_import_time` first.
  virtual auto
  eviction_weight(const uuid& partition, const partition_synopsis& synopsis,
                  time now = time::clock::now()) const -> Option<double>;

  /// What happens to a partition the disk-budget loop selected when the
  /// policy contributes no pipeline for it.
  enum class eviction_fallback {
    /// Erase the partition outright -- the unconfigured default.
    erase,
    /// Leave the partition alone this round. The right answer when a
    /// configured pipeline cannot be built: deleting data the operator asked
    /// to transform must never be the failure mode of a typo.
    keep,
  };

  /// The policy's answer for a partition being evicted: run a pipeline over
  /// it, erase it outright, or keep it for now.
  using eviction_action_result
    = std::variant<eviction_fallback, storage_action>;

  /// What to do with a partition being evicted. The default is `erase`.
  virtual auto eviction_action(const uuid& partition,
                               const partition_synopsis& synopsis) const
    -> eviction_action_result;

  /// A partition arrived from ingest. Transform outputs do not come through
  /// here -- they arrive at `on_committed` -- so that a policy keeping
  /// per-partition state is not fed the same data twice.
  virtual void on_merged(const partition_synopsis_pair& partition);

  /// A transform replaced `inputs` with `outputs`. The data lives on under new
  /// ids, so a policy keeping per-partition state carries that state over
  /// rather than dropping it -- `on_erased` is *not* called for a replaced
  /// input, because treating a replacement as an erasure would make the
  /// outputs look untouched to every rule that already processed the input.
  /// The whole batch arrives in one call because the outputs mix the inputs'
  /// events: what an output inherits has to be decided across *all* inputs,
  /// and per-partition state that only some inputs carry must not spill onto
  /// events that came from the others. Fires for every replacement, whether or
  /// not the policy asked for the transform; a policy-initiated one
  /// additionally gets `on_committed`.
  virtual void on_replaced(const std::vector<uuid>& inputs,
                           const std::vector<partition_info>& outputs);

  /// A transform the policy asked for committed its outputs.
  virtual void on_committed(const std::any& token, const uuid& input,
                            const std::vector<partition_info>& outputs);

  /// A partition left the catalog.
  virtual void on_erased(const uuid& partition);

  /// A transform the policy asked for failed.
  virtual void
  on_failed(const std::any& token, const uuid& input, const caf::error& error);

  /// Makes pending policy state durable now, without waiting for whatever
  /// debouncing the policy does internally, and reports the outcome. The
  /// catalog calls this when a named run completes -- its caller is about to
  /// hear `done`, so the state backing that answer must be durable first --
  /// and on shutdown, where the write races teardown and the outcome is only
  /// logged. The default reports success immediately.
  virtual auto flush() -> caf::error;

  /// Serializes a maintenance action's token for the transform marker, so a
  /// commit a crash cut off between the durable marker and `on_committed` can
  /// be replayed at the next startup. An empty result means the token is not
  /// replayable, which is what the default provides.
  [[nodiscard]] virtual auto serialize_token(const std::any& token) const
    -> std::string;

  /// Reconstructs a token serialized by `serialize_token`. Returns an empty
  /// `std::any` for input it does not recognize.
  [[nodiscard]] virtual auto
  deserialize_token(std::string_view serialized) const -> std::any;
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
