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
#include "vast/concept/printable/to_string.hpp"
#include "vast/data.hpp"
#include "vast/detail/string.hpp"
#include "vast/view.hpp"

#include <fmt/color.h>
#include <fmt/format.h>

#include <optional>

namespace vast {

struct json_style {
  fmt::text_style null_;
  fmt::text_style false_;
  fmt::text_style true_;
  fmt::text_style number;
  fmt::text_style string;
  fmt::text_style array;
  fmt::text_style object;
  fmt::text_style field;
  fmt::text_style comma;
};

// Defined in
// https://github.com/jqlang/jq/blob/c99981c5b2e7e7d4d6d1463cf564bb99e9f18ed9/src/jv_print.c#L27
inline auto jq_style() -> json_style {
  return {
    .null_ = fmt::emphasis::bold | fg(fmt::terminal_color::black),
    .false_ = fg(fmt::terminal_color::white),
    .true_ = fg(fmt::terminal_color::white),
    .number = fg(fmt::terminal_color::white),
    .string = fg(fmt::terminal_color::green),
    .array = fmt::emphasis::bold | fg(fmt::terminal_color::white),
    .object = fmt::emphasis::bold | fg(fmt::terminal_color::white),
    .field = fmt::emphasis::bold | fg(fmt::terminal_color::blue),
    .comma = fmt::emphasis::bold | fg(fmt::terminal_color::white),
  };
}

inline auto no_style() -> json_style {
  return {};
}

inline auto default_style() -> json_style {
  // TODO: perform TTY detection here.
  return no_style();
}

struct json_printer : printer_base<json_printer> {
  struct options {
    /// The number of spaces used for indentation.
    uint8_t indentation = 2;

    /// Colorize the output like `jq`.
    json_style style = no_style();

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

    auto operator()(caf::none_t) noexcept -> bool {
      out_ = fmt::format_to(out_, options_.style.null_, "null");
      return true;
    }

    auto operator()(view<bool> x) noexcept -> bool {
      out_ = x ? fmt::format_to(out_, options_.style.true_, "true")
               : fmt::format_to(out_, options_.style.false_, "false");
      return true;
    }

    auto operator()(view<int64_t> x) noexcept -> bool {
      out_ = fmt::format_to(out_, options_.style.number, "{}", x);
      return true;
    }

    auto operator()(view<uint64_t> x) noexcept -> bool {
      out_ = fmt::format_to(out_, options_.style.number, "{}", x);
      return true;
    }

    auto operator()(view<double> x) noexcept -> bool {
      if (double i; std::modf(x, &i) == 0.0) // NOLINT
        out_ = fmt::format_to(out_, options_.style.number, "{}.0", i);
      else
        out_ = fmt::format_to(out_, options_.style.number, "{}", x);
      return true;
    }

    auto operator()(view<duration> x) noexcept -> bool {
      if (options_.numeric_durations) {
        const auto seconds
          = std::chrono::duration_cast<std::chrono::duration<double>>(x).count();
        return (*this)(seconds);
      }
      out_
        = fmt::format_to(out_, options_.style.string, "\"{}\"", to_string(x));
      return true;
    }

    auto operator()(view<time> x) noexcept -> bool {
      out_
        = fmt::format_to(out_, options_.style.string, "\"{}\"", to_string(x));
      return true;
    }

    auto operator()(view<std::string> x) noexcept -> bool {
      out_ = fmt::format_to(out_, options_.style.string, "{}",
                            detail::json_escape(x));
      return true;
    }

    auto operator()(view<pattern> x) noexcept -> bool {
      return (*this)(x.string());
    }

    auto operator()(view<ip> x) noexcept -> bool {
      out_
        = fmt::format_to(out_, options_.style.string, "\"{}\"", to_string(x));
      return true;
    }

    auto operator()(view<subnet> x) noexcept -> bool {
      out_
        = fmt::format_to(out_, options_.style.string, "\"{}\"", to_string(x));
      return true;
    }

    auto operator()(view<enumeration> x) noexcept -> bool {
      // We shouldn't ever arrive here as users should transform the enumeration
      // to its textual representation first, but you never really know, so
      // let's just print the number.
      out_ = fmt::format_to(out_, options_.style.number, "{}", x);
      return true;
    }

    auto operator()(const view<list>& x) noexcept -> bool {
      bool printed_once = false;
      out_ = fmt::format_to(out_, options_.style.array, "[");
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
      out_ = fmt::format_to(out_, options_.style.array, "]");
      return true;
    }

    auto operator()(const view<map>& x) noexcept -> bool {
      bool printed_once = false;
      out_ = fmt::format_to(out_, options_.style.array, "[");
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
        out_ = fmt::format_to(out_, options_.style.object, "{{");
        indent();
        newline();
        out_ = fmt::format_to(out_, options_.style.field, "\"key\": ");
        if (!caf::visit(*this, element.first))
          return false;
        separator();
        newline();
        out_ = fmt::format_to(out_, options_.style.field, "\"value\": ");
        if (!caf::visit(*this, element.second))
          return false;
        dedent();
        newline();
        out_ = fmt::format_to(out_, options_.style.object, "}}");
      }
      if (printed_once) {
        dedent();
        newline();
      }
      out_ = fmt::format_to(out_, options_.style.array, "]");
      return true;
    }

    auto operator()(const view<record>& x, std::string_view prefix
                                           = {}) noexcept -> bool {
      bool printed_once = false;
      if (!options_.flattened || prefix.empty())
        out_ = fmt::format_to(out_, options_.style.object, "{{");
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
                              ? fmt::format(options_.style.field, "{}",
                                            std::string{element.first})
                              : fmt::format(options_.style.field, "{}.{}",
                                            prefix, element.first);
          if (const auto* r = caf::get_if<view<record>>(&element.second)) {
            if (!(*this)(*r, name))
              return false;
          } else {
            if (!(*this)(name))
              return false;
            out_ = fmt::format_to(out_, options_.style.object, ": ");
            if (!caf::visit(*this, element.second)) {
              return false;
            }
          }
        } else {
          out_ = fmt::format_to(out_, options_.style.field, "{}",
                                detail::json_escape(element.first));
          out_ = fmt::format_to(out_, options_.style.object, ": ");
          if (!caf::visit(*this, element.second))
            return false;
        }
      }
      if (printed_once && !options_.flattened) {
        dedent();
        newline();
      }
      if (!options_.flattened || prefix.empty())
        out_ = fmt::format_to(out_, options_.style.object, "}}");
      return true;
    }

  private:
    auto should_skip(view<data> x) noexcept -> bool {
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
        out_ = fmt::format_to(out_, options_.style.comma, ", ");
      else
        out_ = fmt::format_to(out_, options_.style.comma, ",");
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
  auto print(Iterator& out, const view<data>& d) const noexcept -> bool {
    return caf::visit(print_visitor{out, options_}, d);
  }

  template <class Iterator, class T>
    requires caf::detail::tl_contains<view<data>::types, T>::value
  auto print(Iterator& out, const T& d) const noexcept -> bool {
    return print_visitor{out, options_}(d);
  }

  template <class Iterator>
  auto print(Iterator& out, const data& d) const noexcept -> bool {
    return print(out, make_view(d));
  }

  template <class Iterator, class T>
    requires(!caf::detail::tl_contains<view<data>::types, T>::value
             && caf::detail::tl_contains<data::types, T>::value)
  auto print(Iterator& out, const T& d) const noexcept -> bool {
    return print(out, make_view(d));
  }

private:
  options options_ = {};
};

} // namespace vast
