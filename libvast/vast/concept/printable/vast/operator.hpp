//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"
#include "vast/concept/printable/string/string.hpp"
#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/operator.hpp"

namespace vast {

struct arithmetic_operator_printer : printer<arithmetic_operator_printer> {
  using attribute = arithmetic_operator;

  template <class Iterator>
  bool print(Iterator& out, const arithmetic_operator& op) const {
    switch (op) {
      default:
        die("missing case for arithmetic operator");
      case arithmetic_operator::positive:
      case arithmetic_operator::plus:
        return printers::any.print(out, '+');
      case arithmetic_operator::minus:
      case arithmetic_operator::negative:
        return printers::any.print(out, '-');
      case arithmetic_operator::bitwise_not:
        return printers::any.print(out, '~');
      case arithmetic_operator::bitwise_or:
        return printers::any.print(out, '|');
      case arithmetic_operator::bitwise_xor:
        return printers::any.print(out, '^');
      case arithmetic_operator::bitwise_and:
        return printers::any.print(out, '|');
      case arithmetic_operator::times:
        return printers::any.print(out, '*');
      case arithmetic_operator::divides:
        return printers::any.print(out, '/');
      case arithmetic_operator::mod:
        return printers::any.print(out, '%');
    }
  }
};

struct relational_operator_printer : printer<relational_operator_printer> {
  using attribute = relational_operator;

  template <class Iterator>
  bool print(Iterator& out, const relational_operator& op) const {
    switch (op) {
      default:
        die("missing case for relational operator");
      case relational_operator::match:
        return printers::str.print(out, "~");
      case relational_operator::not_match:
        return printers::str.print(out, "!~");
      case relational_operator::in:
        return printers::str.print(out, "in");
      case relational_operator::not_in:
        return printers::str.print(out, "!in");
      case relational_operator::ni:
        return printers::str.print(out, "ni");
      case relational_operator::not_ni:
        return printers::str.print(out, "!ni");
      case relational_operator::equal:
        return printers::str.print(out, "==");
      case relational_operator::not_equal:
        return printers::str.print(out, "!=");
      case relational_operator::less:
        return printers::str.print(out, "<");
      case relational_operator::less_equal:
        return printers::str.print(out, "<=");
      case relational_operator::greater:
        return printers::str.print(out, ">");
      case relational_operator::greater_equal:
        return printers::str.print(out, ">=");
    }
  }
};

struct bool_operator_printer : printer<bool_operator_printer> {
  using attribute = bool_operator;

  template <class Iterator>
  bool print(Iterator& out, const bool_operator& op) const {
    switch (op) {
      default:
        die("missing case for boolean operator");
      case bool_operator::logical_not:
        return printers::str.print(out, "!");
      case bool_operator::logical_and:
        return printers::str.print(out, "&&");
      case bool_operator::logical_or:
        return printers::str.print(out, "||");
    }
  }
};

template <>
struct printer_registry<arithmetic_operator> {
  using type = arithmetic_operator_printer;
};

template <>
struct printer_registry<relational_operator> {
  using type = relational_operator_printer;
};

template <>
struct printer_registry<bool_operator> {
  using type = bool_operator_printer;
};

} // namespace vast

