//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/expression.hpp"

#include <fmt/core.h>

#include <vector>

namespace vast {

/// An operator in a pipeline.
struct pipeline_operator {
  /// TODO: implement this type properly
  std::vector<std::string> xs;
};

/// A data flow that processes data in stages.
struct pipeline {
  expression root;
  std::vector<pipeline_operator> operators;
};

} // namespace vast

namespace fmt {

template <>
struct formatter<vast::pipeline_operator> : formatter<std::string> {
  template <class FormatContext>
  auto format(const vast::pipeline_operator& x, FormatContext& ctx) {
    auto out = ctx.out();
    out = format_to(out, "{}", fmt::join(x.xs, " "));
    return out;
  }
};

template <>
struct formatter<vast::pipeline> : formatter<std::string> {
  template <class FormatContext>
  auto format(const vast::pipeline& x, FormatContext& ctx) {
    auto out = ctx.out();
    out = format_to(out, "{}", x.root);
    for (const auto& x : x.operators)
      out = format_to(out, " | {}", x);
    return out;
  }
};

} // namespace fmt
