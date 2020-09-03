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

#include "vast/concept/printable/core/operators.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/core/sequence.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/concept/printable/vast/port.hpp"
#include "vast/detail/escapers.hpp"
#include "vast/json.hpp"
#include "vast/time.hpp"
#include "vast/view.hpp"

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

    bool operator()(const json::null&) {
      return printers::str.print(out_, "null");
    }

    bool operator()(const view<data>& x) {
      return caf::visit(*this, x);
    }

    template <class T>
    bool operator()(const T& x) {
      if constexpr (std::is_arithmetic_v<T>) {
        auto str = std::to_string(x);
        json::number i;
        if constexpr (std::is_floating_point_v<T>) {
          if (std::modf(x, &i) == 0.0)
            // Do not show 0 as 0.0.
            str.erase(str.find('.'), std::string::npos);
          else
            // Avoid trailing zeros.
            str.erase(str.find_last_not_of('0') + 1, std::string::npos);
        }
        return printers::str.print(out_, str);
      } else {
        json y;
        return convert(x, y) && caf::visit(*this, y);
      }
    }

    bool operator()(const json::boolean& b) {
      return printers::str.print(out_, b ? "true" : "false");
    }

    bool operator()(const std::string_view& str) {
      static auto p = '"' << printers::escape(detail::json_escaper) << '"';
      return p.print(out_, str);
    }

    bool operator()(const std::string& str) {
      return (*this)(std::string_view{str});
    }

    bool operator()(const view<pattern>& x) {
      return (*this)(x.string());
    }

    bool operator()(const view<port>& x) {
      static auto p = '"' << make_printer<port>{} << '"';
      return p.print(out_, x);
    }

    bool operator()(const view<duration>& x) {
      static auto p = '"' << make_printer<duration>{} << '"';
      return p.print(out_, x);
    }

    bool operator()(const view<time>& x) {
      static auto p = '"' << make_printer<time>{} << '"';
      return p.print(out_, x);
    }

    bool operator()(const std::pair<std::string_view, view<data>>& kvp) {
      using namespace printers;
      if (!(*this)(kvp.first))
        return false;
      if (!str.print(out_, ": "))
        return false;
      return caf::visit(*this, kvp.second);
    }

    template <class ForwardIterator>
    bool print_array(ForwardIterator begin, ForwardIterator end) {
      using namespace printers;
      if (depth_ == 0 && !pad())
        return false;
      if (begin == end)
        return str.print(out_, "[]");
      if (!printers::any.print(out_, '['))
        return false;
      if constexpr (tree) {
        ++depth_;
        printers::any.print(out_, '\n');
      }
      while (begin != end) {
        if (!indent())
          return false;
        if (!caf::visit(*this, *begin))
          return false;
        ++begin;
        if (begin != end)
          if (!str.print(out_, tree ? ",\n" : ", "))
            return false;
      }
      if constexpr (tree) {
        --depth_;
        if (!printers::any.print(out_, '\n') || !indent())
          return false;
      }
      return printers::any.print(out_, ']');
    }

    bool operator()(const json::array& xs) {
      return print_array(xs.begin(), xs.end());
    }

    bool operator()(const view<list>& xs) {
      return print_array(xs.begin(), xs.end());
    }

    template <class ForwardIterator>
    bool print_object(ForwardIterator begin, ForwardIterator end) {
      using namespace printers;
      if (depth_ == 0 && !pad())
        return false;
      if (begin == end)
        return str.print(out_, "{}");
      if (!printers::any.print(out_, '{'))
        return false;
      if constexpr (tree) {
        ++depth_;
        if (!printers::any.print(out_, '\n'))
          return false;
      }
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
      if constexpr (tree) {
        --depth_;
        if (!printers::any.print(out_, '\n'))
          return false;
        if (!indent())
          return false;
      }
      return printers::any.print(out_, '}');
    }

    bool operator()(const json::object& xs) {
      return print_object(xs.begin(), xs.end());
    }

    bool operator()(const view<map>& xs) {
      // FIXME: maps are currently treated the same as records. This feels
      // wrong. We should reconsider rendering of VAST maps, e.g., as list of
      // key-value pairs: [[a, b], [c, d]].
      return print_object(xs.begin(), xs.end());
    }

    bool operator()(const view<record>& xs) {
      return print_object(xs.begin(), xs.end());
    }

    bool pad() {
      if constexpr (Padding > 0)
        for (auto i = 0; i < Padding; ++i)
          if (!printers::any.print(out_, ' '))
            return false;
      return true;
    }

    bool indent() {
      if (!pad())
        return false;
      if constexpr (!tree)
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

