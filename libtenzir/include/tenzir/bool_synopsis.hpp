//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2019 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/synopsis.hpp"

namespace tenzir {

// A synopsis for a [bool type](@ref bool_type).
class bool_synopsis : public synopsis {
public:
  explicit bool_synopsis(tenzir::type x);

  bool_synopsis(bool true_, bool false_);

  [[nodiscard]] synopsis_ptr clone() const override;

  void add(const series& x) override;

  [[nodiscard]] std::optional<bool>
  lookup(relational_operator op, data_view rhs) const override;

  [[nodiscard]] bool equals(const synopsis& other) const noexcept override;

  [[nodiscard]] size_t memusage() const override;

  bool inspect_impl(supported_inspectors& inspector) override;

  bool any_true();

  bool any_false();

private:
  bool true_ = false;
  bool false_ = false;
};

} // namespace tenzir
