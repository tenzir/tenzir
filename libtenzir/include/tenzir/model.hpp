//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/box.hpp"
#include "tenzir/data.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/option.hpp"
#include "tenzir/plugin/base.hpp"
#include "tenzir/result.hpp"
#include "tenzir/type.hpp"
#include "tenzir/view3.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir {

/// Fields shared by every persisted, mergeable model record.
struct model_envelope {
  std::string_view model;
  uint64_t version = 0;
  uint64_t input_count = 0;
  uint64_t count = 0;
  uint64_t null_count = 0;
};

/// Parses and validates the common model envelope. Additional fields are left
/// to the model-specific parser.
auto parse_model_envelope(record_view3 record)
  -> Result<model_envelope, std::string>;

/// Parses and validates the common envelope of an owned model record.
auto parse_model_envelope(record const& record)
  -> Result<model_envelope, std::string>;

auto parse_model_envelope(nova::RowView<nova::Record> record)
  -> Result<model_envelope, std::string>;

/// Reads an unsigned integer from a model record. The Nova overload also
/// accepts nonnegative signed integers, which a JSON round trip produces.
auto model_uint64(data_view3 value) -> Result<uint64_t, std::string>;
auto model_uint64(data const& value) -> Result<uint64_t, std::string>;
auto model_uint64(nova::RowView<nova::Data> value)
  -> Result<uint64_t, std::string>;

/// Reads a double from a model record. The Nova overload also accepts
/// integers, as JSON does not distinguish them from integral doubles.
auto model_double(data_view3 value) -> Result<double, std::string>;
auto model_double(nova::RowView<nova::Data> value)
  -> Result<double, std::string>;

/// Prepends the common envelope to model-specific result fields.
auto model_record_type(std::vector<struct record_type::field> fields) -> type;

/// Mutable, validated state for merging records of one model and configuration.
class model_merge_state {
public:
  virtual ~model_merge_state() = default;

  /// Validates and merges one model. Compatibility errors are returned with a
  /// user-facing explanation and must leave the state semantically unchanged.
  virtual auto merge(record_view3 model) -> Result<void, std::string> = 0;

  /// Returns the validated merged model record.
  virtual auto get() const -> data = 0;

  /// Returns the state serialized in an aggregation checkpoint. Models whose
  /// data representation loses type information may override this method.
  virtual auto get_for_checkpoint() const -> data {
    return get();
  }
};

namespace nova {

/// A comparison error and the operand to highlight in its diagnostic.
struct ModelComparisonError {
  enum class Operand { lhs, rhs };

  std::string message;
  Operand operand = Operand::lhs;

  static auto from_rhs(std::string message) -> ModelComparisonError {
    return {std::move(message), Operand::rhs};
  }
};

/// Mutable model state that consumes borrowed records without Arrow conversion.
class ModelMergeState {
public:
  virtual ~ModelMergeState() = default;
  virtual auto merge(RowView<Record> model) -> Result<void, std::string> = 0;
  virtual auto get() const -> Data = 0;
};

} // namespace nova

/// Dispatch interface implemented by every first-class mergeable model plugin.
/// The same concrete plugin should also implement `aggregation_plugin` to
/// expose the model constructor as a TQL aggregation function.
class model_plugin : public virtual plugin {
public:
  /// The only schema version accepted and emitted by this implementation.
  virtual auto model_version() const -> uint64_t = 0;

  /// Validates the first record and creates merge state initialized from it.
  virtual auto make_model_merge_state(record_view3 model) const
    -> Result<Box<model_merge_state>, std::string>
    = 0;

  /// Restores merge state from an owned model without changing its value
  /// representation. Implementations with heterogeneous nested values should
  /// override this overload.
  virtual auto make_model_merge_state(record const& model) const
    -> Result<Box<model_merge_state>, std::string>;

  virtual auto make_model_merge_state(nova::RowView<nova::Record> model) const
    -> Result<Box<nova::ModelMergeState>, std::string>;
};

/// A model that supports divergence measures.
class model_divergence_plugin : public virtual model_plugin {
public:
  virtual auto model_divergence(record_view3 lhs, record_view3 rhs,
                                std::string_view method) const
    -> Result<Option<double>, std::string>
    = 0;

  virtual auto model_divergence(nova::RowView<nova::Record> lhs,
                                nova::RowView<nova::Record> rhs,
                                std::string_view method) const
    -> Result<Option<double>, nova::ModelComparisonError>;
};

/// A model that supports distance measures.
class model_distance_plugin : public virtual model_plugin {
public:
  virtual auto model_distance(record_view3 lhs, record_view3 rhs,
                              std::string_view method) const
    -> Result<Option<double>, std::string>
    = 0;

  virtual auto
  model_distance(nova::RowView<nova::Record> lhs,
                 nova::RowView<nova::Record> rhs, std::string_view method) const
    -> Result<Option<double>, nova::ModelComparisonError>;
};

/// Finds the provider whose plugin name and version match an envelope.
auto find_model_plugin(model_envelope const& envelope)
  -> Result<model_plugin const*, std::string>;

} // namespace tenzir
