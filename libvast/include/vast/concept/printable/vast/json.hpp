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
#include "vast/concept/printable/std/chrono.hpp"
#include "vast/data.hpp"
#include "vast/detail/escapers.hpp"
#include "vast/view.hpp"

#include <fmt/format.h>

namespace vast {

struct json_printer : printer_base<json_printer> {
  struct options {
    /// The number of spaces used for indentation.
    uint8_t indentation = 2;

    /// Print NDJSON rather than JSON.
    bool oneline = false;

    /// Print nested objects as flattened.
    bool flattened = false;

    /// Print numeric rather than human-readable durations.
    bool numeric_durations = false;

    /// Omit null values when printing.
    bool omit_nulls = false;

    /// Omit empty records when printing.
    bool omit_empty_records = false;

    /// Omit empty lists when printing.
    bool omit_empty_lists = false;

    /// Omit empty maps when printing.
    bool omit_empty_maps = false;
  };

  explicit json_printer(struct options options) noexcept
    : printer_base(), options_{options} {
    // nop
  }

  template <class Iterator>
  struct print_visitor {
    print_visitor(Iterator& out, const options& options) noexcept
      : out_{out}, options_{options} {
      // nop
    }

    bool operator()(caf::none_t) noexcept {
      out_ = fmt::format_to(out_, "null");
      return true;
    }

    bool operator()(view<bool> x) noexcept {
      out_ = x ? fmt::format_to(out_, "true") : fmt::format_to(out_, "false");
      return true;
    }

    bool operator()(view<integer> x) noexcept {
      out_ = fmt::format_to(out_, "{}", x.value);
      return true;
    }

    bool operator()(view<count> x) noexcept {
      out_ = fmt::format_to(out_, "{}", x);
      return true;
    }

    bool operator()(view<real> x) noexcept {
      if (real i; std::modf(x, &i) == 0.0) // NOLINT
        out_ = fmt::format_to(out_, "{}.0", i);
      else
        out_ = fmt::format_to(out_, "{}", x);
      return true;
    }

    bool operator()(view<duration> x) noexcept {
      if (options_.numeric_durations) {
        const auto seconds
          = std::chrono::duration_cast<std::chrono::duration<real>>(x).count();
        return (*this)(seconds);
      }
      static auto p = '"' << make_printer<duration>{} << '"';
      return p.print(out_, x);
    }

    bool operator()(view<time> x) noexcept {
      static auto p = '"' << make_printer<time>{} << '"';
      return p.print(out_, x);
    }

    bool operator()(view<std::string> x) noexcept {
      static auto p = '"' << printers::escape(detail::json_escaper) << '"';
      return p.print(out_, x);
    }

    bool operator()(view<pattern> x) noexcept {
      return (*this)(x.string());
    }

    bool operator()(view<address> x) noexcept {
      static auto p = '"' << make_printer<address>{} << '"';
      return p.print(out_, x);
    }

    bool operator()(view<subnet> x) noexcept {
      static auto p = '"' << make_printer<subnet>{} << '"';
      return p.print(out_, x);
    }

    bool operator()(view<enumeration> x) noexcept {
      // We shouldn't ever arrive here as users should transform the enumeration
      // to its textual representation first, but you never really know, so
      // let's just print the number.
      out_ = fmt::format_to(out_, "{}", x);
      return true;
    }

    bool operator()(const view<list>& x) noexcept {
      bool printed_once = false;
      out_ = fmt::format_to(out_, "[");
      for (const auto& element : x) {
        if (should_skip(element))
          continue;
        if (!printed_once) {
          indent();
          newline();
          printed_once = true;
        } else {
          separator();
          newline();
        }
        if (!caf::visit(*this, element))
          return false;
      }
      if (printed_once) {
        dedent();
        newline();
      }
      out_ = fmt::format_to(out_, "]");
      return true;
    }

    bool operator()(const view<map>& x) noexcept {
      bool printed_once = false;
      out_ = fmt::format_to(out_, "[");
      for (const auto& element : x) {
        if (should_skip(element.second))
          continue;
        if (!printed_once) {
          indent();
          newline();
          printed_once = true;
        } else {
          separator();
          newline();
        }
        out_ = fmt::format_to(out_, "{{");
        indent();
        newline();
        out_ = fmt::format_to(out_, "\"key\": ");
        if (!caf::visit(*this, element.first))
          return false;
        separator();
        newline();
        out_ = fmt::format_to(out_, "\"value\": ");
        if (!caf::visit(*this, element.second))
          return false;
        dedent();
        newline();
        out_ = fmt::format_to(out_, "}}");
      }
      if (printed_once) {
        dedent();
        newline();
      }
      out_ = fmt::format_to(out_, "]");
      return true;
    }

    bool
    operator()(const view<record>& x, std::string_view prefix = {}) noexcept {
      bool printed_once = false;
      if (!options_.flattened || prefix.empty())
        out_ = fmt::format_to(out_, "{{");
      for (const auto& element : x) {
        if (should_skip(element.second))
          continue;
        if (!printed_once) {
          if (!options_.flattened) {
            indent();
            newline();
          }
          printed_once = true;
        } else {
          separator();
          newline();
        }
        if (options_.flattened) {
          const auto name = prefix.empty()
                              ? std::string{element.first}
                              : fmt::format("{}.{}", prefix, element.first);
          if (const auto* r = caf::get_if<view<record>>(&element.second)) {
            if (!(*this)(*r, name))
              return false;
          } else {
            if (!(*this)(name))
              return false;
            out_ = fmt::format_to(out_, ": ");
            if (!caf::visit(*this, element.second)) {
              return false;
            }
          }
        } else {
          if (!(*this)(element.first))
            return false;
          out_ = fmt::format_to(out_, ": ");
          if (!caf::visit(*this, element.second))
            return false;
        }
      }
      if (printed_once && !options_.flattened) {
        dedent();
        newline();
      }
      if (!options_.flattened || prefix.empty())
        out_ = fmt::format_to(out_, "}}");
      return true;
    }

  private:
    bool should_skip(view<data> x) noexcept {
      if (options_.omit_nulls && caf::holds_alternative<caf::none_t>(x)) {
        return true;
      }
      if (options_.omit_empty_lists && caf::holds_alternative<view<list>>(x)) {
        const auto& ys = caf::get<view<list>>(x);
        return std::all_of(ys.begin(), ys.end(),
                           [this](const view<data>& y) noexcept {
                             return should_skip(y);
                           });
      }
      if (options_.omit_empty_maps && caf::holds_alternative<view<map>>(x)) {
        const auto& ys = caf::get<view<map>>(x);
        return std::all_of(
          ys.begin(), ys.end(),
          [this](const view<map>::view_type::value_type& y) noexcept {
            return should_skip(y.second);
          });
      }
      if (options_.omit_empty_records
          && caf::holds_alternative<view<record>>(x)) {
        const auto& ys = caf::get<view<record>>(x);
        return std::all_of(
          ys.begin(), ys.end(),
          [this](const view<record>::view_type::value_type& y) noexcept {
            return should_skip(y.second);
          });
      }
      return false;
    }

    void indent() noexcept {
      indentation_ += options_.indentation;
    }

    void dedent() noexcept {
      VAST_ASSERT_EXPENSIVE(indentation_ >= options_.indentation,
                            "imbalanced calls between indent() and dedent()");
      indentation_ -= options_.indentation;
    }

    void separator() noexcept {
      if (options_.oneline)
        out_ = fmt::format_to(out_, ", ");
      else
        out_ = fmt::format_to(out_, ",");
    }

    void newline() noexcept {
      if (!options_.oneline)
        out_ = fmt::format_to(out_, "\n{: >{}}", "", indentation_);
    }

    Iterator& out_;
    const options& options_;
    uint32_t indentation_ = 0;
  };

  template <class Iterator>
  bool print(Iterator& out, const view<data>& d) const noexcept {
    return caf::visit(print_visitor{out, options_}, d);
  }

  template <class Iterator, class T>
    requires caf::detail::tl_contains<view<data>::types, T>::value bool
  print(Iterator& out, const T& d) const noexcept {
    return print_visitor{out, options_}(d);
  }

  template <class Iterator>
  bool print(Iterator& out, const data& d) const noexcept {
    return print(out, make_view(d));
  }

  template <class Iterator, class T>
    requires(!caf::detail::tl_contains<view<data>::types, T>::value
             && caf::detail::tl_contains<data::types, T>::value) bool
  print(Iterator& out, const T& d) const noexcept {
    return print(out, make_view(d));
  }

private:
  options options_ = {};
};

} // namespace vast
