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

class hash_step : public transform_step {
public:
  hash_step(const std::string& fieldname, const std::string& out,
            const std::optional<std::string>& salt = std::nullopt);

  [[nodiscard]] caf::error
  add(vast::id offset, type layout,
      std::shared_ptr<arrow::RecordBatch> batch) override;
  [[nodiscard]] caf::expected<batch_vector> finish() override;

private:
  std::string field_;
  std::string out_;
  std::optional<std::string> salt_;

  /// The slices being transformed.
  batch_vector transformed_;
};

} // namespace vast
