//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/logical_operator.hpp"

#include <caf/error.hpp>

namespace vast {

/// A type-erased, logical representation of a pipeline consisting of a sequence
/// of logical operators with matching input and output element types.
class logical_pipeline final : public runtime_logical_operator {
public:
  /// Default-constructs an empty logical pipeline.
  logical_pipeline() noexcept = default;

  /// Destroys a logical pipeline.
  ~logical_pipeline() noexcept override = default;

  /// Copy-constructs a logical pipeline.
  explicit logical_pipeline(const logical_pipeline& other);

  /// Copy-assigns a logical pipeline.
  logical_pipeline& operator=(const logical_pipeline& rhs);

  /// Move-constructs a logical pipeline.
  logical_pipeline(logical_pipeline&& rhs) noexcept;

  /// Move-assigns a logical pipeline.
  logical_pipeline& operator=(logical_pipeline&& rhs) noexcept;

  /// Parses a logical pipeline from its textual representation.
  static auto parse(std::string_view repr) -> caf::expected<logical_pipeline>;

  /// Creates a logical pipeline from a set of logical operators. Flattenes
  /// nested pipelines to ensure that none of the operators are themselves a
  /// pipeline.
  static auto make(std::vector<logical_operator_ptr> ops)
    -> caf::expected<logical_pipeline>;

  /// Returns the input element type of the logical pipeline's first operator,
  /// or the *void* element type if the logical pipeline is empty.
  [[nodiscard]] auto input_element_type() const noexcept
    -> runtime_element_type override;

  /// Returns the output element type of the logical pipeline's last operator,
  /// or the *void* element type if the logical pipeline is empty.
  [[nodiscard]] auto output_element_type() const noexcept
    -> runtime_element_type override;

  /// Returns whether any of the logical pipeline's operators are detached.
  [[nodiscard]] auto detached() const noexcept -> bool override;

  /// Returns whether the pipeline is closed, i.e., both the input and output
  /// element types are *void*.
  [[nodiscard]] auto closed() const noexcept -> bool;

  /// See *runtime_logical_operator::make_runtime_physical_operatorinput_schema,
  /// ctrl)*
  ///
  /// NOTE: A logical pipeline has no single corresponding physical operator.
  /// Calling this function always returns a logic error. To run a pipeline,
  /// create an executor instead, e.g., by calling *make_local_executor()*.
  [[nodiscard]] auto
  make_runtime_physical_operator(const type& input_schema,
                                 operator_control_plane& ctrl) noexcept
    -> caf::expected<runtime_physical_operator> override;

  /// Returns a textual representation of the logical pipeline.
  [[nodiscard]] auto to_string() const noexcept -> std::string override;

  /// Unwraps the logical pipeline, converting it into its logical operators.
  [[nodiscard]] auto unwrap() && -> std::vector<logical_operator_ptr>;

  /// Support the CAF type inspection API.
  template <class Inspector>
  friend auto inspect(Inspector& f, logical_pipeline& x) -> bool {
    if constexpr (Inspector::is_loading) {
      auto repr = std::string{};
      if (not f.object(x)
                .pretty_name("vast.logical-pipeline")
                .fields(f.field("repr", repr)))
        return false;
      auto result = logical_pipeline::parse(repr);
      if (not result) {
        VAST_WARN("failed to parse pipeline '{}': {}", repr, result.error());
        return false;
      }
      x = std::move(*result);
      return true;
    } else {
      auto repr = x.to_string();
      return f.object(x)
        .pretty_name("vast.logical-pipeline")
        .fields(f.field("repr", repr));
    }
  }

private:
  explicit logical_pipeline(std::vector<logical_operator_ptr> ops)
    : ops_(std::move(ops)) {
  }

  std::vector<logical_operator_ptr> ops_ = {};
};

/// Creates a local executor from the current logical pipeline that allows for
/// running the pipeline in the current thread incrementally.
/// @pre `pipeline.closed()`
auto make_local_executor(logical_pipeline pipeline) noexcept
  -> generator<caf::expected<void>>;

} // namespace vast

template <>
struct fmt::formatter<vast::logical_pipeline>
  : fmt::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const vast::logical_pipeline& value, FormatContext& ctx) const {
    return fmt::formatter<std::string_view>::format(value.to_string(), ctx);
  }
};
