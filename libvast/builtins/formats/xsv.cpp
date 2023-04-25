//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/printable/vast/view.hpp"

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/printable/vast/json.hpp>
#include <vast/plugin.hpp>
#include <vast/view.hpp>

#include <arrow/record_batch.h>
#include <fmt/core.h>

#include <algorithm>
#include <cctype>
#include <iterator>

namespace vast::plugins::xsv {

template <class Iterator>
struct xsv_printer {
  auto operator()(caf::none_t) noexcept -> bool {
    out_ = std::copy(null_value_.begin(), null_value_.end(), out_);
    return true;
  }

  auto operator()(auto x) noexcept -> bool {
    // TODO: Avoid the data_view cast.
    return data_view_printer{}.print(out_, make_data_view(x));
  }

  auto operator()(view<pattern>) noexcept -> bool {
    die("unreachable");
  }

  auto operator()(view<map>) noexcept -> bool {
    die("unreachable");
  }

  auto operator()(view<std::string> x) noexcept -> bool {
    auto needs_escaping = std::any_of(x.begin(), x.end(), [this](auto c) {
      return std::iscntrl(static_cast<unsigned char>(c)) || c == row_separator_
             || c == '"';
    });
    if (needs_escaping) {
      static auto p = '"' << printers::escape(detail::json_escaper) << '"';
      return p.print(out_, x);
    }
    out_ = std::copy(x.begin(), x.end(), out_);
    return true;
  }

  auto operator()(const view<list>& x) noexcept -> bool {
    auto json_list = std::string{};
    auto l = std::back_inserter(json_list);
    auto first = true;
    for (const auto& v : x) {
      if (!first) {
        ++out_ = list_separator_;
      } else {
        first = false;
      }
      //(*this)(v);
      if (!printer_.print(l, v)) {
        return false;
      }
    }
    return true;
  }

  auto operator()(const view<record>& x) noexcept -> bool {
    auto first = true;
    for (const auto& [_, v] : x) {
      if (!first) {
        ++out_ = row_separator_;
      } else {
        first = false;
      }
      auto json_record = std::string{};
      auto l = std::back_inserter(json_record);
      if (!printer_.print(l, v)) {
        return false;
      }
      (*this)(json_record);
    }
    return true;
  }

  auto print_header(const view<record>& x) noexcept -> bool {
    auto first = true;
    for (const auto& [k, _] : x) {
      if (!first) {
        ++out_ = row_separator_;
      } else {
        first = false;
      }
      (*this)(k);
    }
    return true;
  }

  auto print_values(const view<record>& x) noexcept -> bool {
    auto first = true;
    for (const auto& [_, v] : x) {
      if (!first) {
        ++out_ = row_separator_;
      } else {
        first = false;
      }
      caf::visit(*this, v);
    }
    return true;
  }

  Iterator& out_;
  // TODO:
  // must not overlap with null value
  // must not be alphanumeric
  // must not be a dot (single or double)
  // must not be a bracket/brace
  // must not be a double quote
  // must not be + or -
  // -> allow space, comma, tab, semicolon, \0
  // with shorthands
  char row_separator_{','};
  char list_separator_{';'};
  std::string null_value_{};
  json_printer printer_{{.oneline = true}};

  xsv_printer(Iterator& out, char separator = ',', std::string null_value = "")
    : out_(out),
      row_separator_(separator),
      null_value_(std::move(null_value)),
      printer_{{.oneline = true}} {
  }
};

class plugin final : public virtual printer_plugin {
  auto make_printer([[maybe_unused]] std::span<std::string const> args,
                    type input_schema, operator_control_plane&) const
    -> caf::expected<printer> override {
    auto input_type = caf::get<record_type>(input_schema);
    return [input_type](table_slice slice) -> generator<chunk_ptr> {
      auto buffer = std::vector<char>{};
      auto out_iter = std::back_inserter(buffer);
      // TODO: Custom args for sep and null values.
      auto printer = xsv_printer{out_iter};
      auto resolved_slice = resolve_enumerations(slice);
      auto array
        = to_record_batch(resolved_slice)->ToStructArray().ValueOrDie();
      auto first = true;
      for (const auto& row : values(input_type, *array)) {
        VAST_ASSERT_CHEAP(row);
        if (first) {
          printer.print_header(*row);
          first = false;
          out_iter = fmt::format_to(out_iter, "\n");
        }
        const auto ok = printer.print_values(*row);
        VAST_ASSERT_CHEAP(ok);
        out_iter = fmt::format_to(out_iter, "\n");
      }
      auto chunk = chunk::make(std::move(buffer));
      co_yield std::move(chunk);
    };
  }

  auto default_saver(std::span<std::string const>) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"stdout", {}};
  }

  auto printer_allows_joining() const -> bool override {
    return false;
  };

  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "xsv";
  }
};

} // namespace vast::plugins::xsv

VAST_REGISTER_PLUGIN(vast::plugins::xsv::plugin)
