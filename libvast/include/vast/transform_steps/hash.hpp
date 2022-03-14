//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/transform.hpp"
#include "vast/transform_step.hpp"

namespace vast {

/// The configuration of the hash transform step.
struct hash_step_configuration {
  std::string field;
  std::string out;
  std::optional<std::string> salt;

  /// Support type inspection for easy parsing with convertible.
  template <class Inspector>
  friend auto inspect(Inspector& f, hash_step_configuration& x) {
    return f(x.field, x.out, x.salt);
  }

  /// Enable parsing from a record via convertible.
  static inline const record_type& layout() noexcept {
    static auto result = record_type{
      {"field", string_type{}},
      {"out", string_type{}},
      {"salt", string_type{}},
    };
    return result;
  }
};

class hash_step : public transform_step {
public:
  explicit hash_step(hash_step_configuration configuration);

  [[nodiscard]] caf::error
  add(type layout, std::shared_ptr<arrow::RecordBatch> batch) override;
  [[nodiscard]] caf::expected<std::vector<transform_batch>> finish() override;

private:
  /// The slices being transformed.
  std::vector<transform_batch> transformed_;

  /// The underlying configuration of the transformation.
  hash_step_configuration config_ = {};
};

} // namespace vast
