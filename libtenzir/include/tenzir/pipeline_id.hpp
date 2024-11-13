//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/data.hpp"
#include "tenzir/type.hpp"

#include <fmt/format.h>

#include <cstdint>
#include <string>
#include <vector>

namespace tenzir {

// An structure to make operator instances identifiable within a pipeline.
struct operator_index {
  // id fragment used to identify an instantiated subpipeline.
  // This can be a pipeline_id or a constant that was used to initialize a sub
  // pipeline.
  // TODO: consider using a data instead.
  std::string parent_id = {};

  // A unique run id for the (nested) pipeline with the same parent_id.
  uint64_t run = 0;

  // The operator position.
  uint64_t position = 0;

  friend auto
  operator<=>(const operator_index& lhs, const operator_index& rhs) noexcept
    -> std::strong_ordering {
    if (lhs.parent_id != rhs.parent_id) {
      return lhs.parent_id <=> rhs.parent_id;
    }
    if (lhs.run != rhs.run) {
      return lhs.run <=> rhs.run;
    }
    return lhs.position <=> rhs.position;
  }

  inline static auto layout() -> record_type {
    return record_type{
      {"parent_id", string_type{}},
      {"run", uint64_type{}},
      {"position", uint64_type{}},
    };
  }

  auto to_record() const -> record {
    return record{
      {"parent_id", parent_id},
      {"parent_id", run},
      {"parent_id", position},
    };
  }

  friend auto inspect(auto& f, operator_index& x) -> bool {
    return f.object(x)
      .pretty_name("tenzir.operator_index")
      .fields(f.field("parent_id", x.parent_id), f.field("run", x.run),
              f.field("position", x.position));
  }
};

// A list of operator ids can be used to fully identify operator instances in
// nested pipelines.
struct pipeline_path : std::vector<operator_index> {
  using vector::vector;

  inline static auto layout() -> list_type {
    return list_type{type{operator_index::layout()}};
  }

  auto to_list() const -> list {
    list result = {};
    for (const auto& x : *this) {
      result.emplace_back(x.to_record());
    }
    return result;
  }

  template <class Inspector>
  friend auto inspect(Inspector& f, pipeline_path& xs) {
    return f.object(xs)
      .pretty_name("tenzir.pipeline_path")
      .fields(f.field("pipeline_path",
                      static_cast<std::vector<operator_index>&>(xs)));
  }
};

} // namespace tenzir

namespace fmt {

template <>
struct formatter<tenzir::operator_index> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const tenzir::operator_index& x, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "[{}:{}:{}]", x.parent_id, x.run,
                          x.position);
  }
};

template <>
struct formatter<tenzir::pipeline_path> {
  template <class ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    return ctx.begin();
  }

  template <typename FormatContext>
  auto format(const tenzir::pipeline_path& xs, FormatContext& ctx) const {
    return fmt::format_to(ctx.out(), "{}", fmt::join(xs, ""));
  }
};

} // namespace fmt
