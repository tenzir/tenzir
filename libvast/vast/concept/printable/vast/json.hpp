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

#include "vast/json.hpp"

#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/core/printer.hpp"

#include "vast/detail/string.hpp"

namespace vast {

//struct json_type_printer : printer<json_type_printer> {
//  using attribute = json::type;
//
//  template <class Iterator>
//  bool print(Iterator& out, const json::type& t) {
//    using namespace printers;
//    switch (t) {
//      default:
//        return str.print(out, "<invalid>");
//      case json::type::null:
//        return str.print(out, "null");
//      case json::type::boolean:
//        return str.print(out, "bool");
//      case json::type::number:
//        return str.print(out, "number");
//      case json::type::string:
//        return str.print(out, "string");
//      case json::type::array:
//        return str.print(out, "array");
//      case json::type::object:
//        return str.print(out, "object");
//    }
//  }
//};

namespace policy {

struct tree {};
struct oneline {};

} // namespace policy

template <class TreePolicy, int Indent = 2, int Padding = 0>
struct json_printer : printer<json_printer<TreePolicy, Indent, Padding>> {
  using attribute = json;

  static constexpr bool tree = std::is_same_v<TreePolicy, policy::tree>;

  static_assert(Padding >= 0, "padding must not be negative");

  template <class Iterator>
  struct print_visitor {
    print_visitor(Iterator& out) : out_{out} {}

    bool operator()(json::null) {
      return printers::str.print(out_, "null");
    }

    bool operator()(json::boolean b) {
      return printers::str.print(out_, b ? "true" : "false");
    }

    bool operator()(json::number n) {
      auto str = std::to_string(n);
      json::number i;
      if (std::modf(n, &i) == 0.0)
        // Do not show 0 as 0.0.
        str.erase(str.find('.'), std::string::npos);
      else
        // Avoid no trailing zeros.
        str.erase(str.find_last_not_of('0') + 1, std::string::npos);
      return printers::str.print(out_, str);
    }

    bool operator()(const json::string& str) {
      return printers::str.print(out_, detail::json_escape(str));
    }

    bool operator()(const json::array& a) {
      using namespace printers;
      if (depth_ == 0 && !pad())
        return false;
      if (!any.print(out_, '['))
        return false;
      if (!a.empty() && tree) {
        ++depth_;
        any.print(out_, '\n');
      }
      auto begin = a.begin();
      auto end = a.end();
      while (begin != end) {
        if (!indent())
          return false;
        if (!caf::visit(*this, *begin))
          return false;
        ;
        ++begin;
        if (begin != end)
          if (!str.print(out_, tree ? ",\n" : ", "))
            return false;
      }
      if (!a.empty() && tree) {
        --depth_;
        any.print(out_, '\n');
        if (!indent())
          return false;
      }
      return any.print(out_, ']');
    }

    bool operator()(const json::object& o) {
      using namespace printers;
      if (depth_ == 0 && !pad())
        return false;
      if (!any.print(out_, '{'))
        return false;
      if (!o.empty() && tree) {
        ++depth_;
        if (!any.print(out_, '\n'))
          return false;
      }
      auto begin = o.begin();
      auto end = o.end();
      while (begin != end) {
        if (!indent())
          return false;
        if (!(*this)(begin->first))
          return false;
        if (!str.print(out_, ": "))
          return false;
        if (!caf::visit(*this, begin->second))
          return false;
        ++begin;
        if (begin != end)
          if (!str.print(out_, tree ? ",\n" : ", "))
            return false;
      }
      if (!o.empty() && tree) {
        --depth_;
        if (!any.print(out_, '\n'))
          return false;
        if (!indent())
          return false;
      }
      return any.print(out_, '}');
    }

    bool pad() {
      if (Padding > 0)
        for (auto i = 0; i < Padding; ++i)
          if (!printers::any.print(out_, ' '))
            return false;
      return true;
    }

    bool indent() {
      if (!pad())
        return false;
      if (!tree)
        return true;
      for (auto i = 0; i < depth_ * Indent; ++i)
        if (!printers::any.print(out_, ' '))
          return false;
      return true;
    }

    Iterator& out_;
    int depth_ = 0;
  };

  // Overload for concrete JSON types.
  template <class Iterator, class T>
  bool print(Iterator& out, const T& x) const {
    return print_visitor<Iterator>{out}(x);
  }

  template <class Iterator>
  bool print(Iterator& out, const json& j) const {
    return caf::visit(print_visitor<Iterator>{out}, j);
  }
};

template <class TreePolicy, int Indent, int Padding>
constexpr bool json_printer<TreePolicy, Indent, Padding>::tree;

template <>
struct printer_registry<json::array> {
  using type = json_printer<policy::tree, 2>;
};

template <>
struct printer_registry<json::object> {
  using type = json_printer<policy::tree, 2>;
};

template <>
struct printer_registry<json> {
  using type = json_printer<policy::tree, 2>;
};

namespace printers {

template <class Policy>
auto json = json_printer<Policy>{};

} // namespace printers
} // namespace vast

