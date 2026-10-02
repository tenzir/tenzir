//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/tql2/tokens.hpp"
#include "tenzir/variant.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <simdjson.h>
#include <span>
#include <string>
#include <string_view>
#include <utility>

namespace tenzir::nova {

/// Recursively prints nova row/value views as JSON or TQL.
///
/// Output without any style goes into a simdjson string builder, which escapes
/// strings with SIMD. Styled output goes through `fmt`.
class json_printer {
public:
  explicit json_printer(json_printer_options options = {})
    : options_{std::move(options)}, styled_{is_styled(options_.style)} {
  }

  auto print(nova::RowView<nova::Data> const& row) -> void {
    if (styled_) {
      buffer_.clear();
    } else {
      builder_.clear();
    }
    print_row(row);
  }

  auto bytes() const noexcept -> std::span<const std::byte> {
    if (styled_) {
      return {
        reinterpret_cast<std::byte const*>(buffer_.data()),
        buffer_.size(),
      };
    }
    auto const view = builder_.view();
    TENZIR_ASSERT(not view.error());
    return {
      reinterpret_cast<std::byte const*>(view.value_unsafe().data()),
      view.value_unsafe().size(),
    };
  }

private:
  static auto is_styled(json_style const& style) -> bool {
    auto const styled = [](fmt::text_style const& x) {
      return x.has_foreground() or x.has_background() or x.has_emphasis();
    };
    return styled(style.null_) or styled(style.false_) or styled(style.true_)
           or styled(style.number) or styled(style.string)
           or styled(style.array) or styled(style.object) or styled(style.field)
           or styled(style.comma) or styled(style.duration)
           or styled(style.time) or styled(style.subnet) or styled(style.ip)
           or styled(style.blob) or styled(style.colon);
  }

  // Every token goes through one of the functions below.

  /// Appends `text` verbatim.
  auto put(fmt::text_style style, std::string_view text) -> void {
    if (styled_) {
      fmt::format_to(std::back_inserter(buffer_), style, "{}", text);
    } else {
      builder_.append_raw(text);
    }
  }

  /// Appends `text` as a quoted, escaped JSON string.
  auto put_string(fmt::text_style style, std::string_view text) -> void {
    if (styled_) {
      fmt::format_to(std::back_inserter(buffer_), style, "{}",
                     json_string_fmt_wrapper{text});
    } else {
      builder_.escape_and_append_with_quotes(text);
    }
  }

  template <class T>
  auto put_number(fmt::text_style style, T x) -> void {
    if (styled_) {
      fmt::format_to(std::back_inserter(buffer_), style, "{}", x);
    } else {
      builder_.append(x);
    }
  }

  auto print_double(double x) -> void {
    switch (std::fpclassify(x)) {
      case FP_NAN:
      case FP_INFINITE:
        put(options_.style.null_, "null");
        return;
      default:
        break;
    }
    auto str = fmt::to_string(x);
    if (str.find_first_not_of("-0123456789") == std::string::npos) {
      str += ".0";
    }
    put({}, str);
  }

  auto print_row(nova::RowView<nova::Data> const& row) -> void {
    match(
      row,
      [&](nova::RowView<nova::Null>) {
        put(options_.style.null_, "null");
      },
      [&](nova::RowView<nova::Bool> v) {
        if (*v) {
          put(options_.style.true_, "true");
        } else {
          put(options_.style.false_, "false");
        }
      },
      [&](nova::RowView<nova::Int> v) {
        put_number(options_.style.number, *v);
      },
      [&](nova::RowView<nova::UInt> v) {
        put_number(options_.style.number, *v);
      },
      [&](nova::RowView<nova::Float> v) {
        print_double(*v);
      },
      [&](nova::RowView<nova::String> v) {
        put_string(options_.style.string, *v);
      },
      [&](nova::RowView<nova::Blob> v) {
        auto const bytes = *v;
        if (not options_.tql) {
          put_string(options_.style.string, detail::base64::encode(bytes));
          return;
        }
        auto const text = std::string_view{
          reinterpret_cast<const char*>(bytes.data()), bytes.size()};
        if (styled_) {
          fmt::format_to(std::back_inserter(buffer_), options_.style.blob,
                         "b{}", json_string_fmt_wrapper{text});
        } else {
          builder_.append('b');
          builder_.escape_and_append_with_quotes(text);
        }
      },
      [&](nova::RowView<nova::Secret>) {
        put(options_.style.string, "\"***\"");
      },
      [&](nova::RowView<nova::Ip> v) {
        if (options_.tql) {
          put(options_.style.ip, to_string(*v));
        } else {
          put_string(options_.style.string, to_string(*v));
        }
      },
      [&](nova::RowView<nova::Subnet> v) {
        if (options_.tql) {
          put(options_.style.subnet, to_string(*v));
        } else {
          put_string(options_.style.string, to_string(*v));
        }
      },
      [&](nova::RowView<nova::Time> v) {
        if (options_.tql) {
          put(options_.style.time, to_string(*v));
        } else {
          put_string(options_.style.string, to_string(*v));
        }
      },
      [&](nova::RowView<nova::Duration> v) {
        if (options_.numeric_durations) {
          auto const seconds
            = std::chrono::duration_cast<std::chrono::duration<double>>(*v)
                .count();
          print_double(seconds);
        } else if (options_.tql) {
          put(options_.style.duration, to_string(*v));
        } else {
          put_string(options_.style.string, to_string(*v));
        }
      },
      [&](nova::RowView<nova::List> const& list) {
        put(options_.style.array, "[");
        auto printed_once = false;
        for (auto element : list) {
          if (should_skip(element, true)) {
            continue;
          }
          if (not printed_once) {
            indent();
            newline();
            printed_once = true;
          } else {
            list_separator();
            newline();
          }
          print_row(element);
        }
        if (printed_once) {
          trailing_comma();
          dedent();
          newline();
        }
        put(options_.style.array, "]");
      },
      [&](nova::RowView<nova::Record> const& record) {
        put(options_.style.object, "{");
        auto printed_once = false;
        for (auto [key, value] : record) {
          if (should_skip(value, false)) {
            continue;
          }
          if (not printed_once) {
            indent();
            newline();
            printed_once = true;
          } else {
            list_separator();
            newline();
          }
          if (options_.tql) {
            auto tokens = tokenize_permissive(key);
            if (tokens.size() == 1
                and tokens.front().kind == token_kind::identifier) {
              put(options_.style.field, key);
            } else {
              put_string(options_.style.string, key);
            }
          } else {
            put_string(options_.style.field, key);
          }
          put(options_.style.colon, options_.oneline ? ":" : ": ");
          print_row(value);
        }
        if (printed_once) {
          trailing_comma();
          dedent();
          newline();
        }
        put(options_.style.object, "}");
      });
  }

  auto should_skip(nova::RowView<nova::Data> const& row, bool in_list) -> bool {
    return match(
      row,
      [&](nova::RowView<nova::Null>) {
        return options_.omit_null_fields
               or (in_list and options_.omit_nulls_in_lists);
      },
      [&](nova::RowView<nova::List> const& list) {
        if (not options_.omit_empty_lists) {
          return false;
        }
        for (auto element : list) {
          if (not should_skip(element, true)) {
            return false;
          }
        }
        return true;
      },
      [&](nova::RowView<nova::Record> const& record) {
        if (not options_.omit_empty_records) {
          return false;
        }
        for (auto field : record) {
          if (not should_skip(field.second, false)) {
            return false;
          }
        }
        return true;
      },
      [](auto) {
        return false;
      });
  }

  auto indent() -> void {
    indentation_ += options_.indentation;
  }

  auto dedent() -> void {
    TENZIR_ASSERT(indentation_ >= options_.indentation,
                  "imbalanced calls between indent() and dedent()");
    indentation_ -= options_.indentation;
  }

  auto trailing_comma() -> void {
    auto print = options_.trailing_commas.value_or(options_.tql
                                                   and not options_.oneline);
    if (print) {
      list_separator();
    }
  }

  auto list_separator() -> void {
    put(options_.style.comma, ",");
  }

  auto newline() -> void {
    if (options_.oneline) {
      return;
    }
    if (styled_) {
      fmt::format_to(std::back_inserter(buffer_), "\n{: >{}}", "",
                     indentation_);
      return;
    }
    constexpr auto spaces
      = std::string_view{"                                "};
    builder_.append('\n');
    for (auto left = size_t{indentation_}; left > 0;) {
      auto const count = std::min(left, spaces.size());
      builder_.append_raw(spaces.substr(0, count));
      left -= count;
    }
  }

  json_printer_options options_;
  bool styled_;
  /// The output of styled printing.
  std::string buffer_;
  /// The output of unstyled printing.
  simdjson::builder::string_builder builder_;
  uint32_t indentation_ = 0;
};

} // namespace tenzir::nova
