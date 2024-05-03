//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "detail/inspection_common.hpp"
#include "tenzir/atoms.hpp"
#include "tenzir/concept/printable/print.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/operators.hpp"
#include "tenzir/detail/type_traits.hpp"
#include "tenzir/hash/hash.hpp"
#include "tenzir/offset.hpp"
#include "tenzir/operator.hpp"
#include "tenzir/type.hpp"

#include <caf/default_sum_type_access.hpp>
#include <caf/detail/type_list.hpp>
#include <caf/none.hpp>
#include <caf/variant.hpp>

#include <memory>
#include <string>
#include <type_traits>
#include <vector>

namespace tenzir {

class selection {
public:
  std::optional<std::vector<std::string>> fields;

  selection() = default;
  selection(std::optional<std::vector<std::string>> fields)
    : fields{std::move(fields)} {
  }
};

} // namespace tenzir
