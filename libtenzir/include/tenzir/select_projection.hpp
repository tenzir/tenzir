//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/offset.hpp"
#include "tenzir/type.hpp"

#include <arrow/array.h>
#include <arrow/record_batch.h>

#include <memory>

namespace tenzir {

// class select_projection;

class select_projection {
public:
  std::optional<std::vector<std::string>> fields;
  select_projection() = default;

  select_projection(std::optional<std::vector<std::string>> fields)
    : fields{std::move(fields)} {
  }
};

} // namespace tenzir
