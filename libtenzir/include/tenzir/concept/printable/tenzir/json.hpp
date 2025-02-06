//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/core/operators.hpp"
#include "tenzir/concept/printable/core/printer.hpp"
#include "tenzir/concept/printable/tenzir/json_printer_options.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/base64.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/tql2/tokens.hpp"
#include "tenzir/view.hpp"

#include <fmt/format.h>

#include <cmath>
#include <optional>
#include <string_view>

namespace tenzir {

struct json_printer : printer_base<json_printer> {
  explicit json_printer(json_printer_options options) noexcept
    : printer_base(), options_{options} {
    // nop
  }

  template <class Iterator>
  struct print_visitor {
    print_visitor(Iterator& out, const json_printer_options& options) noexcept
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
      switch (std::fpclassify(x)) {
        case FP_NORMAL:
        case FP_SUBNORMAL:
        case FP_ZERO:
          break;
        default:
          return (*this)(caf::none);
      }
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
      if (options_.tql) {
        out_
          = fmt::format_to(out_, options_.style.duration, "{}", to_string(x));
      } else {
        out_
          = fmt::format_to(out_, options_.style.string, "\"{}\"", to_string(x));
      }
      return true;
    }

    auto operator()(view<time> x) noexcept -> bool {
      if (options_.tql) {
        out_ = fmt::format_to(out_, options_.style.time, "{}", to_string(x));
      } else {
        out_
          = fmt::format_to(out_, options_.style.string, "\"{}\"", to_string(x));
      }
      return true;
    }

    auto operator()(view<std::string> x) noexcept -> bool {
      out_ = fmt::format_to(out_, options_.style.string, "{}",
                            detail::json_escape(x));
      return true;
    }

    auto operator()(view<blob> x) noexcept -> bool {
      if (options_.tql) {
        out_ = fmt::format_to(
          out_, options_.style.blob, "b{}",
          detail::json_escape(
            {reinterpret_cast<const char*>(x.data()), x.size()}));
        return true;
      } else {
        return (*this)(detail::base64::encode(x));
      }
    }

    auto operator()(view<pattern> x) noexcept -> bool {
      return (*this)(x.string());
    }

    auto operator()(view<ip> x) noexcept -> bool {
      if (options_.tql) {
        out_ = fmt::format_to(out_, options_.style.ip, "{}", to_string(x));
      } else {
        out_
          = fmt::format_to(out_, options_.style.string, "\"{}\"", to_string(x));
      }
      return true;
    }

    auto operator()(view<subnet> x) noexcept -> bool {
      if (options_.tql) {
        out_ = fmt::format_to(out_, options_.style.subnet, "{}", to_string(x));
      } else {
        out_
          = fmt::format_to(out_, options_.style.string, "\"{}\"", to_string(x));
      }
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
        if (should_skip(element, true)) {
          continue;
        }
        if (!printed_once) {
          indent();
          newline();
          printed_once = true;
        } else {
          separator();
          newline();
        }
        if (!match(element, *this))
          return false;
      }
      if (printed_once) {
        trailing_comma();
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
        if (should_skip(element.second, false)) {
          continue;
        }
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
        if (options_.oneline) {
          out_ = fmt::format_to(out_, options_.style.field, "\"key\":");
        } else {
          out_ = fmt::format_to(out_, options_.style.field, "\"key\": ");
        }
        if (!match(element.first, *this))
          return false;
        separator();
        newline();
        if (options_.oneline) {
          out_ = fmt::format_to(out_, options_.style.field, "\"value\":");
        } else {
          out_ = fmt::format_to(out_, options_.style.field, "\"value\": ");
        }
        if (!match(element.second, *this))
          return false;
        dedent();
        newline();
        out_ = fmt::format_to(out_, options_.style.object, "}}");
      }
      if (printed_once) {
        trailing_comma();
        dedent();
        newline();
      }
      out_ = fmt::format_to(out_, options_.style.array, "]");
      return true;
    }

    auto operator()(const view<record>& x) noexcept -> bool {
      bool printed_once = false;
      out_ = fmt::format_to(out_, options_.style.object, "{{");
      for (const auto& [key, value] : x) {
        if (should_skip(value, false)) {
          continue;
        }
        if (!printed_once) {
          indent();
          newline();
          printed_once = true;
        } else {
          separator();
          newline();
        }
        if (options_.tql) {
          auto x = tokenize_permissive(key);
          if (x.size() == 1 and x.front().kind == token_kind::identifier) {
            out_ = fmt::format_to(out_, options_.style.field, "{}", key);
          } else {
            out_ = fmt::format_to(out_, options_.style.string, "{}",
                                  detail::json_escape(key));
          }
        } else {
          out_ = fmt::format_to(out_, options_.style.field, "{}",
                                detail::json_escape(key));
        }
        if (options_.oneline) {
          out_ = fmt::format_to(out_, options_.style.colon, ":");
        } else {
          out_ = fmt::format_to(out_, options_.style.colon, ": ");
        }
        if (!match(value, *this)) {
          return false;
        }
      }
      if (printed_once) {
        trailing_comma();
        dedent();
        newline();
      }
      out_ = fmt::format_to(out_, options_.style.object, "}}");
      return true;
    }

  private:
    auto should_skip(view<data> x, bool in_list) noexcept -> bool {
      if (in_list and options_.omit_nulls_in_lists && is<caf::none_t>(x)) {
        return true;
      }
      if (options_.omit_null_fields && is<caf::none_t>(x)) {
        return true;
      }
      if (options_.omit_empty_lists && is<view<list>>(x)) {
        const auto& ys = as<view<list>>(x);
        return std::all_of(ys.begin(), ys.end(),
                           [this](const view<data>& y) noexcept {
                             return should_skip(y, true);
                           });
      }
      if (options_.omit_empty_maps && is<view<map>>(x)) {
        const auto& ys = as<view<map>>(x);
        return std::all_of(
          ys.begin(), ys.end(),
          [this](const view<map>::view_type::value_type& y) noexcept {
            return should_skip(y.second, false);
          });
      }
      if (options_.omit_empty_records && is<view<record>>(x)) {
        const auto& ys = as<view<record>>(x);
        return std::all_of(
          ys.begin(), ys.end(),
          [this](const view<record>::view_type::value_type& y) noexcept {
            return should_skip(y.second, false);
          });
      }
      return false;
    }

    void indent() noexcept {
      indentation_ += options_.indentation;
    }

    void dedent() noexcept {
      TENZIR_ASSERT(indentation_ >= options_.indentation,
                    "imbalanced calls between indent() and dedent()");
      indentation_ -= options_.indentation;
    }

    auto trailing_comma() -> void {
      auto print = false;
      if (options_.trailing_commas) {
        print = *options_.trailing_commas;
      } else {
        print = options_.tql and not options_.oneline;
      }
      if (print) {
        out_ = fmt::format_to(out_, options_.style.comma, ",");
      }
    }

    void separator() noexcept {
      if (options_.oneline) {
        out_ = fmt::format_to(out_, options_.style.comma, ",");
      } else {
        out_ = fmt::format_to(out_, options_.style.comma, ", ");
      }
    }

    void newline() noexcept {
      if (!options_.oneline)
        out_ = fmt::format_to(out_, "\n{: >{}}", "", indentation_);
    }

    Iterator& out_;
    const json_printer_options& options_;
    uint32_t indentation_ = 0;
  };

  template <class Iterator>
  auto print(Iterator& out, const view<data>& d) const noexcept -> bool {
    return match(d, print_visitor{out, options_});
  }

  template <class Iterator, class T>
    requires detail::tl_contains<view<data>::types, T>::value
  auto print(Iterator& out, const T& d) const noexcept -> bool {
    return print_visitor{out, options_}(d);
  }

  template <class Iterator>
  auto print(Iterator& out, const data& d) const noexcept -> bool {
    return print(out, make_view(d));
  }

  template <class Iterator, class T>
    requires(!detail::tl_contains<view<data>::types, T>::value
             && detail::tl_contains<data::types, T>::value)
  auto print(Iterator& out, const T& d) const noexcept -> bool {
    return print(out, make_view(d));
  }

private:
  json_printer_options options_ = {};
};

} // namespace tenzir
