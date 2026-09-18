//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/array_base.hpp"

#include <utility>

namespace tenzir::nova {

template <class Builder>
class MaskedArrayBuilder {
public:
  [[nodiscard]] auto value() -> Builder& {
    bit_builder_.emplace_back(true);
    return data_builder_;
  }

  auto skip() -> void {
    data_builder_.skip();
    bit_builder_.emplace_back(false);
  }

  /// Appends `count` absent rows at once.
  auto skip_n(storage::Index count) -> void {
    data_builder_.skip_n(count);
    bit_builder_.append_n(false, count);
  }

  auto size() const -> storage::Index {
    return bit_builder_.size();
  }

  /// Removes the last row, which must be present, and returns its value.
  auto take_last() -> Data {
    const auto present = bit_builder_.pop_back();
    TENZIR_ASSERT(present);
    return data_builder_.take_last();
  }

  auto finish() -> MaskedArray<decltype(std::declval<Builder>().finish())> {
    auto data = data_builder_.finish();
    return {.data = std::move(data), .present = bit_builder_.finish()};
  }

private:
  Builder data_builder_;
  storage::BitMap::Builder bit_builder_;
};

} // namespace tenzir::nova
