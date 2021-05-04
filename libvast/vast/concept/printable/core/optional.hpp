//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/support/detail/attr_fold.hpp"

#include <optional>

namespace vast {

template <class Printer>
class optional_printer : public printer<optional_printer<Printer>> {
public:
  using inner_attribute = detail::attr_fold_t<typename Printer::attribute>;

  using attribute
    = std::conditional_t<std::is_same_v<inner_attribute, unused_type>,
                         unused_type, std::optional<inner_attribute>>;

  explicit optional_printer(Printer p)
    : printer_{std::move(p)} {
  }

  template <class Iterator>
  bool print(Iterator& out, unused_type) const {
    printer_.print(out, unused);
    return true;
  }

  template <class Iterator, class Attribute>
  bool print(Iterator& out, const Attribute& a) const {
    return !a || printer_.print(out, *a);
  }

private:
  Printer printer_;
};

} // namespace vast
