//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/concept/printable/core/operators.hpp"
#include "vast/concept/printable/core/printer.hpp"
#include "vast/concept/printable/core/sequence.hpp"
#include "vast/concept/printable/print.hpp"
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/concept/printable/string.hpp"
#include "vast/data.hpp"
#include "vast/detail/escapers.hpp"
#include "vast/time.hpp"
#include "vast/view.hpp"

namespace vast {

namespace policy {

// Tree policies.
struct tree {};
struct oneline {};

// Duration policies.
struct numeric_durations {};
struct human_readable_durations {};

} // namespace policy

template <class TreePolicy, class DurationPolicy, int Indent = 2,
          int Padding = 0>
struct json_printer
  : printer_base<json_printer<TreePolicy, DurationPolicy, Indent, Padding>> {
  inline static constexpr bool tree = std::is_same_v<TreePolicy, policy::tree>;
  inline static constexpr bool human_readable_durations
    = std::is_same_v<DurationPolicy, policy::human_readable_durations>;

  static_assert(Padding >= 0, "padding must not be negative");

  template <class Iterator>
  struct print_visitor {
    print_visitor(Iterator& out) : out_{out} {
    }

    bool operator()(const caf::none_t&) {
      return printers::str.print(out_, "null");
    }

    bool operator()(const integer& x) {
      return printers::str.print(out_, std::to_string(x.value));
    }

    bool operator()(const address& x) {
      static auto p = '"' << make_printer<address>{} << '"';
      return p.print(out_, x);
    }

    bool operator()(const data& x) {
      return caf::visit(*this, x);
    }

    bool operator()(const view<data>& x) {
      return caf::visit(*this, x);
    }

    template <class T>
    bool operator()(const T& x) {
      if constexpr (human_readable_durations
                    && std::is_same_v<T, view<duration>>) {
        static auto p = '"' << make_printer<duration>{} << '"';
        return p.print(out_, x);
      }
      if constexpr (std::is_arithmetic_v<T>) {
        // Print non-finite numbers as `null`.
        if (!std::isfinite(x))
          return printers::str.print(out_, "null");
        auto str = std::to_string(x);
        real i;
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
        data y;
        return convert(x, y) && caf::visit(*this, y);
      }
    }

    bool operator()(const bool& b) {
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

    bool operator()(const list& xs) {
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

    bool operator()(const record& xs) {
      return print_object(xs.begin(), xs.end());
    }

    bool operator()(const view<record>& xs) {
      return print_object(xs.begin(), xs.end());
    }

    bool operator()(const map& xs) {
      // FIXME: maps are currently treated the same as records. This feels
      // wrong. We should reconsider rendering of VAST maps, e.g., as list of
      // key-value pairs: [[a, b], [c, d]].
      return print_object(xs.begin(), xs.end());
    }

    bool operator()(const view<map>& xs) {
      // FIXME: maps are currently treated the same as records. This feels
      // wrong. We should reconsider rendering of VAST maps, e.g., as list of
      // key-value pairs: [[a, b], [c, d]].
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
  bool print(Iterator& out, const data& d) const {
    return caf::visit(print_visitor<Iterator>{out}, d);
  }
};

namespace printers {

template <class TreePolicy, class DurationPolicy>
auto json = json_printer<TreePolicy, DurationPolicy>{};

} // namespace printers
} // namespace vast
