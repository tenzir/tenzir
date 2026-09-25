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

#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>
#include <utility>

namespace tenzir::nova {

/// Recursively prints nova row/value views as JSON or TQL.
class json_printer {
public:
  explicit json_printer(json_printer_options options = {})
    : options_{std::move(options)} {
  }

  auto print(nova::RowView<nova::Data> const& row) -> void {
    buffer_.clear();
    print_row(row);
  }

  auto bytes() const noexcept -> std::span<const std::byte> {
    return {
      reinterpret_cast<std::byte const*>(buffer_.data()),
      buffer_.size(),
    };
  }

private:
  auto print_double(double x) -> void {
    switch (std::fpclassify(x)) {
      case FP_NAN:
      case FP_INFINITE:
        append(options_.style.null_, "null");
        return;
      default:
        break;
    }
    auto str = fmt::to_string(x);
    if (str.find_first_not_of("-0123456789") == std::string::npos) {
      str += ".0";
    }
    buffer_.append(str);
  }

  auto print_row(nova::RowView<nova::Data> const& row) -> void {
    match(
      row,
      [&](nova::RowView<nova::Null>) {
        append(options_.style.null_, "null");
      },
      [&](nova::RowView<nova::Bool> v) {
        if (*v) {
          append(options_.style.true_, "true");
        } else {
          append(options_.style.false_, "false");
        }
      },
      [&](nova::RowView<nova::Int> v) {
        append(options_.style.number, "{}", *v);
      },
      [&](nova::RowView<nova::UInt> v) {
        append(options_.style.number, "{}", *v);
      },
      [&](nova::RowView<nova::Float> v) {
        print_double(*v);
      },
      [&](nova::RowView<nova::String> v) {
        append(options_.style.string, "{}", json_string_fmt_wrapper{*v});
      },
      [&](nova::RowView<nova::Blob> v) {
        auto const bytes = *v;
        if (options_.tql) {
          append(options_.style.blob, "b{}",
                 json_string_fmt_wrapper{std::string_view{
                   reinterpret_cast<const char*>(bytes.data()), bytes.size()}});
        } else {
          append(options_.style.string, "{}",
                 json_string_fmt_wrapper{detail::base64::encode(bytes)});
        }
      },
      [&](nova::RowView<nova::Secret>) {
        append(options_.style.string, "\"***\"");
      },
      [&](nova::RowView<nova::Ip> v) {
        if (options_.tql) {
          append(options_.style.ip, "{}", to_string(*v));
        } else {
          append(options_.style.string, "{}",
                 json_string_fmt_wrapper{to_string(*v)});
        }
      },
      [&](nova::RowView<nova::Subnet> v) {
        if (options_.tql) {
          append(options_.style.subnet, "{}", to_string(*v));
        } else {
          append(options_.style.string, "{}",
                 json_string_fmt_wrapper{to_string(*v)});
        }
      },
      [&](nova::RowView<nova::Time> v) {
        if (options_.tql) {
          append(options_.style.time, "{}", to_string(*v));
        } else {
          append(options_.style.string, "{}",
                 json_string_fmt_wrapper{to_string(*v)});
        }
      },
      [&](nova::RowView<nova::Duration> v) {
        if (options_.numeric_durations) {
          auto const seconds
            = std::chrono::duration_cast<std::chrono::duration<double>>(*v)
                .count();
          print_double(seconds);
        } else if (options_.tql) {
          append(options_.style.duration, "{}", to_string(*v));
        } else {
          append(options_.style.string, "{}",
                 json_string_fmt_wrapper{to_string(*v)});
        }
      },
      [&](nova::RowView<nova::List> const& list) {
        append(options_.style.array, "[");
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
        append(options_.style.array, "]");
      },
      [&](nova::RowView<nova::Record> const& record) {
        append(options_.style.object, "{{");
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
              append(options_.style.field, "{}", key);
            } else {
              append(options_.style.string, "{}", json_string_fmt_wrapper{key});
            }
          } else {
            append(options_.style.field, "{}", json_string_fmt_wrapper{key});
          }
          if (options_.oneline) {
            append(options_.style.colon, ":");
          } else {
            append(options_.style.colon, ": ");
          }
          print_row(value);
        }
        if (printed_once) {
          trailing_comma();
          dedent();
          newline();
        }
        append(options_.style.object, "}}");
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

  template <class... Args>
  auto append(fmt::text_style style, fmt::format_string<Args...> format,
              Args&&... args) -> void {
    fmt::format_to(std::back_inserter(buffer_), style, format,
                   std::forward<Args>(args)...);
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
    append(options_.style.comma, ",");
  }

  auto newline() -> void {
    if (not options_.oneline) {
      fmt::format_to(std::back_inserter(buffer_), "\n{: >{}}", "",
                     indentation_);
    }
  }

  json_printer_options options_;
  std::string buffer_;
  uint32_t indentation_ = 0;
};

} // namespace tenzir::nova
