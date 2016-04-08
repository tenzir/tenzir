#ifndef VAST_CONCEPT_PRINTABLE_VAST_OPERATOR_HPP
#define VAST_CONCEPT_PRINTABLE_VAST_OPERATOR_HPP

#include "vast/die.hpp"
#include "vast/error.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/string/any.hpp"

namespace vast {

struct arithmetic_operator_printer : printer<arithmetic_operator_printer> {
  using attribute = arithmetic_operator;

  template <typename Iterator>
  bool print(Iterator& out, arithmetic_operator const& op) const {
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

  template <typename Iterator>
  bool print(Iterator& out, relational_operator const& op) const {
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

  template <typename Iterator>
  bool print(Iterator& out, boolean_operator const& op) const {
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

#endif
