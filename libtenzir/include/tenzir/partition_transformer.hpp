//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/fwd.hpp"

#include "tenzir/active_partition.hpp"
#include "tenzir/actors.hpp"
#include "tenzir/atomic.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

#include <caf/typed_event_based_actor.hpp>

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

namespace tenzir {

enum class PartitionTransformPhase : uint8_t {
  loading_input,
  finishing_pipeline,
  creating_output,
  persisting_stores,
  packing_partition_metadata,
  writing_partition_files,
  writing_marker,
  moving_partition_files,
  updating_catalog,
  erasing_input_partitions,
  done,
};

auto partition_transform_phase_name(PartitionTransformPhase phase)
  -> std::string_view;

/// A read-only progress snapshot shared with the index actor. The transformer
/// runs its pipeline on Folly's CPU executor, so atomics keep this diagnostic
/// observer from adding messages to the transform's data path.
struct PartitionTransformProgress {
  void set_phase(PartitionTransformPhase next);
  void report_next_phase_transition_from(PartitionTransformPhase previous);

  Atomic<PartitionTransformPhase> phase
    = PartitionTransformPhase::loading_input;
  Atomic<bool> report_next_phase_transition = false;
  std::string id = {};
  Atomic<size_t> current_input = 0;
  Atomic<size_t> loaded_inputs = 0;
  Atomic<size_t> output_partitions = 0;
  Atomic<size_t> stores_launched = 0;
  Atomic<size_t> stores_finished = 0;
  Atomic<size_t> partition_files_written = 0;
  Atomic<size_t> partition_files_total = 0;
};

/// Similar to the active partition, but all contents come in a single
/// stream, a transform is applied and no queries need to be answered
/// while the partition is constructed.
struct partition_transformer_state {
  static constexpr const char* name = "partition-transformer";

  using result_type = partition_transformer_result;
  using promise_type = caf::typed_response_promise<result_type>;
  using partition_tuple = std::tuple<tenzir::uuid, tenzir::type, chunk_ptr>;
  using synopsis_tuple = std::tuple<tenzir::uuid, chunk_ptr>;

  struct stream_data {
    caf::expected<std::vector<partition_tuple>> partition_chunks = caf::error{};
    caf::expected<std::vector<synopsis_tuple>> synopsis_chunks = caf::error{};
  };

  struct path_data {
    promise_type promise = {};
  };

  partition_transformer_state() = default;

  // Update the `type_ids` map with the information of the given slice.
  void
  update_type_ids(std::unordered_map<std::string, ids>& type_ids,
                  const tenzir::uuid& partition_id, const table_slice& slice);

  // Returns the partition in which to insert this slice, maybe creating a new
  // partition.
  active_partition_state::serialization_data&
  create_or_get_partition(const table_slice& slice);

  // Returns the current non-full partition for `schema`, or creates one.
  active_partition_state::serialization_data&
  create_or_get_streaming_partition(const type& schema);

  void fulfill(
    partition_transformer_actor::stateful_pointer<partition_transformer_state>
      self,
    stream_data&&, path_data&&) const;

  /// Actor handle of the catalog.
  catalog_actor catalog = {};

  /// Actor handle of the filesystem actor.
  filesystem_actor fs = {};

  /// The TQL2 AST of the transform to apply to the data.
  ast::pipeline transform = {};

  /// Cached stream error, if the stream terminated abnormally.
  caf::error stream_error = {};

  /// Cached transform error, if the transform returns one.
  caf::error transform_error = {};

  /// The partitions selected as input for the transform.
  std::vector<partition_info> input_partitions = {};

  /// The input partitions that the transformer actually consumed.
  std::vector<partition_info> transformed_input_partitions = {};

  /// Whether the transformer consumed every selected input partition.
  bool input_complete = true;

  /// The maximum number of events per partition. (not really necessary, but
  /// required by the partition synopsis)
  size_t partition_capacity = 0ull;

  /// Total number of rows in all transformed `slices`.
  size_t events = 0ull;

  /// Oldest import timestamp of the input data.
  tenzir::time min_import_time = tenzir::time::max();

  /// Newest import timestamp of the input data.
  tenzir::time max_import_time = tenzir::time::min();

  /// The data of the newly created partition(s).
  std::multimap<type, active_partition_state::serialization_data> data = {};

  /// Auxiliary data required to create the final partition flatbuffer.
  struct buildup {
    /// The store builder.
    store_builder_actor builder = {};

    /// Cached table slices in this partition.
    std::vector<table_slice> slices = {};
  };

  std::unordered_map<uuid, buildup> partition_buildup;

  /// Store id for partitions.
  std::string store_id;

  /// Origin tag for the store metadata ("rebuild", "compaction", etc.).
  std::string origin = "rebuild";

  /// Constraints on the partition-count reduction achieved by the inputs that
  /// the runtime loader actually consumes.
  size_t minimum_partition_reduction = 0;
  double minimum_reduction_ratio = 0;
  std::vector<uuid> required_input_partitions = {};
  uint64_t input_byte_budget = 0;
  size_t rebuild_batch_size = 0;
  bool input_constraints_satisfied = true;

  /// Progress observed by the index while this transform is active.
  std::shared_ptr<PartitionTransformProgress> progress = {};

  /// Options for creating new synopses.
  index_config synopsis_opts = {};

  /// Options for creating new value indices.
  caf::settings index_opts = {};

  // Paths used to read existing partitions and write transformed output files.
  // The template strings can be formatted with a `tenzir::uuid` as the single
  // parameter.
  std::string input_partition_path_template;
  std::filesystem::path archive_dir;
  std::string partition_path_template;
  std::string synopsis_path_template;

  /// The actor waits until both the stream is finished and an `atom::persist`
  /// has arrived. Depending on what happens first, a different set of
  /// variables need to be stored in the meantime.
  std::variant<std::monostate, stream_data, path_data> persist;

  /// Number of stores launched and finished.
  size_t stores_launched = 0ull;
  size_t stores_finished = 0ull;

  struct stores_are_finished {};
  struct transformer_is_finished {
    promise_type promise;
    result_type result;
  };

  /// This actor shuts down when both all stores it spawned have shut down,
  /// and its own result is ready.
  std::variant<std::monostate, stores_are_finished, transformer_is_finished>
    shutdown_state;
};

/// Extracts the uuid of the partition a store read error is attributed to,
/// if the error carries one. Errors from decoding a partition's store (e.g.
/// a corrupt/truncated backing file) are tagged with the offending
/// partition's uuid so that callers processing a batch of partitions (e.g.
/// the rebuilder) can identify exactly which partition failed instead of
/// having to treat the whole batch as suspect.
auto store_error_partition(const caf::error& err) -> Option<uuid>;

/// Spawns a PARTITION TRANSFORMER actor with the given parameters.
///
/// The actor loads the selected partitions, applies the given `table_slice ->
/// table_slice` AST pipeline via the new coroutine executor, and writes the
/// resulting slices into one or more new partitions.
auto partition_transformer(
  partition_transformer_actor::stateful_pointer<partition_transformer_state>,
  std::string store_id, const index_config& synopsis_opts,
  const caf::settings& index_opts, catalog_actor catalog, filesystem_actor fs,
  std::vector<partition_info> input_partitions, ast::pipeline transform,
  std::string input_partition_path_template, std::filesystem::path archive_dir,
  std::string partition_path_template, std::string synopsis_path_template,
  std::string origin, size_t minimum_partition_reduction,
  double minimum_reduction_ratio, std::vector<uuid> required_input_partitions,
  uint64_t input_byte_budget, size_t rebuild_batch_size,
  std::shared_ptr<PartitionTransformProgress> progress)
  -> partition_transformer_actor::behavior_type;

/// Returns whether rewriting the inputs reduces the partition count
/// sufficiently.
inline auto
satisfies_partition_reduction(size_t input_partitions, size_t output_partitions,
                              size_t minimum_partition_reduction,
                              double minimum_reduction_ratio) -> bool {
  if (input_partitions == 0) {
    return false;
  }
  const auto reduction = input_partitions > output_partitions
                           ? input_partitions - output_partitions
                           : size_t{0};
  return reduction >= minimum_partition_reduction
         and static_cast<double>(reduction)
                 / static_cast<double>(input_partitions)
               >= minimum_reduction_ratio;
}

} // namespace tenzir
