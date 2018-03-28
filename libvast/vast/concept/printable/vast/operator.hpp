/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#pragma once

#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"

namespace vast {

struct arithmetic_operator_printer : printer<arithmetic_operator_printer> {
  using attribute = arithmetic_operator;

  template <class Iterator>
  bool print(Iterator& out, const arithmetic_operator& op) const {
    switch (op) {
      default:
        die("missing case for arithmetic operator");
      case positive:
      case plus:
        return printers::any.print(out, '+');
      case minus:
      case negative:
        return printers::any.print(out, '-');
      case bitwise_not:
        return printers::any.print(out, '~');
      case bitwise_or:
        return printers::any.print(out, '|');
      case bitwise_xor:
        return printers::any.print(out, '^');
      case bitwise_and:
        return printers::any.print(out, '|');
      case times:
        return printers::any.print(out, '*');
      case divides:
        return printers::any.print(out, '/');
      case mod:
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
      case match:
        return printers::str.print(out, "~");
      case not_match:
        return printers::str.print(out, "!~");
      case in:
        return printers::str.print(out, "in");
      case not_in:
        return printers::str.print(out, "!in");
      case ni:
        return printers::str.print(out, "ni");
      case not_ni:
        return printers::str.print(out, "!ni");
      case equal:
        return printers::str.print(out, "==");
      case not_equal:
        return printers::str.print(out, "!=");
      case less:
        return printers::str.print(out, "<");
      case less_equal:
        return printers::str.print(out, "<=");
      case greater:
        return printers::str.print(out, ">");
      case greater_equal:
        return printers::str.print(out, ">=");
    }
  }
};

struct boolean_operator_printer : printer<boolean_operator_printer> {
  using attribute = boolean_operator;

  template <class Iterator>
  bool print(Iterator& out, const boolean_operator& op) const {
    switch (op) {
      default:
        die("missing case for boolean operator");
      case logical_not:
        return printers::str.print(out, "!");
      case logical_and:
        return printers::str.print(out, "&&");
      case logical_or:
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
struct printer_registry<boolean_operator> {
  using type = boolean_operator_printer;
};

} // namespace vast

