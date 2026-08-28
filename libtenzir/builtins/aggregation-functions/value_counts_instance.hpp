//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "../models/value_counts.hpp"

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <string_view>
#include <tuple>
#include <vector>

namespace tenzir::detail {

class value_counts_instance : public aggregation_instance {
public:
  explicit value_counts_instance(ast::expression expr, std::string_view name)
    : expr_{std::move(expr)}, name_{name} {
  }

  auto update(table_slice const& input, session ctx) -> void override {
    for (auto& arg : eval(expr_, input, ctx)) {
      if (is<null_type>(arg.type)) {
        continue;
      }
      for (auto i = int64_t{0}; i < arg.array->length(); ++i) {
        if (arg.array->IsValid(i)) {
          std::ignore = state_.add(view_at(*arg.array, i));
        }
      }
    }
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    auto offsets
      = std::vector<flatbuffers::Offset<fbs::aggregation::ValueCount>>{};
    offsets.reserve(state_.counts().size());
    for (auto const& [value, count] : state_.counts()) {
      offsets.push_back(
        fbs::aggregation::CreateValueCount(fbb, pack(fbb, value), count));
    }
    auto result = fbb.CreateVector(offsets);
    auto aggregation
      = fbs::aggregation::CreateModeValueCountsEntropy(fbb, result);
    fbb.Finish(aggregation);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    auto fb = flatbuffer<fbs::aggregation::ModeValueCountsEntropy>::make(
      std::move(chunk));
    if (not fb) {
      TENZIR_WARN("failed to restore `{}` aggregation instance: invalid "
                  "FlatBuffer",
                  name_);
      return false;
    }
    auto const* result = (*fb)->result();
    if (not result) {
      TENZIR_WARN("failed to restore `{}` aggregation instance: missing field "
                  "`result`",
                  name_);
      return false;
    }
    state_.clear();
    state_.reserve(result->size());
    for (auto const* element : *result) {
      if (not element) {
        TENZIR_WARN("failed to restore `{}` aggregation instance: missing "
                    "element in field `result`",
                    name_);
        return false;
      }
      auto const* packed_value = element->value();
      if (not packed_value) {
        TENZIR_WARN("failed to restore `{}` aggregation instance: missing "
                    "value for element in field `result`",
                    name_);
        return false;
      }
      auto value = data{};
      if (auto err = unpack(*packed_value, value); err.valid()) {
        TENZIR_WARN("failed to restore `{}` aggregation instance: {}", name_,
                    err);
        return false;
      }
      if (not state_.insert(std::move(value), element->count())) {
        TENZIR_WARN("failed to restore `{}` aggregation instance: duplicate "
                    "value",
                    name_);
        return false;
      }
    }
    return true;
  }

  auto reset() -> void override {
    state_.clear();
  }

protected:
  auto state() const -> value_counts_state<int64_t> const& {
    return state_;
  }

private:
  ast::expression const expr_;
  std::string_view const name_;
  value_counts_state<int64_t> state_;
};

} // namespace tenzir::detail
