//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/adaptive_table_slice_builder.hpp"
#include "vast/concept/parseable/string/quoted_string.hpp"
#include "vast/concept/printable/string/escape.hpp"
#include "vast/concept/printable/vast/view.hpp"
#include "vast/detail/zip_iterator.hpp"

#include <vast/arrow_table_slice.hpp>
#include <vast/concept/printable/vast/json.hpp>
#include <vast/detail/string_literal.hpp>
#include <vast/plugin.hpp>
#include <vast/view.hpp>

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <fmt/core.h>

#include <algorithm>
#include <cctype>
#include <iterator>

namespace vast::plugins::zeek_tsv {

namespace {

auto to_lines(generator<chunk_ptr> input) -> generator<std::string_view> {
  auto buffer = std::string{};
  bool ended_on_linefeed = false;
  for (auto&& chunk : input) {
    if (!chunk || chunk->size() == 0) {
      co_yield {};
      continue;
    }
    const auto* begin = reinterpret_cast<const char*>(chunk->data());
    const auto* const end = begin + chunk->size();
    if (ended_on_linefeed && *begin == '\n') {
      ++begin;
    };
    ended_on_linefeed = false;
    for (const auto* current = begin; current != end; ++current) {
      if (*current != '\n' && *current != '\r') {
        continue;
      }
      if (buffer.empty()) {
        co_yield {begin, current};
      } else {
        buffer.append(begin, current);
        co_yield buffer;
        buffer.clear();
      }
      if (*current == '\r') {
        auto next = current + 1;
        if (next == end) {
          ended_on_linefeed = true;
        } else if (*next == '\n') {
          ++current;
        }
      }
      begin = current + 1;
    }
    buffer.append(begin, end);
    co_yield {};
  }
  if (!buffer.empty()) {
    co_yield std::move(buffer);
  }
}

class plugin : public virtual parser_plugin, public virtual printer_plugin {
public:
  auto
  make_parser(std::span<std::string const> args, generator<chunk_ptr> loader,
              operator_control_plane& ctrl) const
    -> caf::expected<parser> override {
    if (args.size() != 1) {
      return caf::make_error(
        ec::syntax_error,
        fmt::format("{} parser requires exactly 1 argument but "
                    "got {}: [{}]",
                    name(), args.size(), fmt::join(args, ", ")));
    }
    return std::invoke(
      [](generator<std::string_view> lines, operator_control_plane& ctrl) -> generator<table_slice> {
        // Parse header.
        auto sep = ',';
        auto it = lines.begin();
        auto header = std::string_view{};
        while (it != lines.end()) {
          header = *it;
          if (!header.empty())
            break;
          co_yield {};
        }
        if (header.empty())
          co_return;
        auto split_parser
          = ((+(parsers::any - sep))
             % sep);
        auto fields = std::vector<std::string>{};
        if (!split_parser(*it, fields)) {
          ctrl.abort(caf::make_error(ec::parse_error,
                                     fmt::format("zeek tsv parser failed to parse "
                                                 "header of zeek tsv input")));
          co_return;
        }
        ++it;
        auto b = adaptive_table_slice_builder{};
        for (; it != lines.end(); ++it) {
          auto line = *it;
          if (line.empty()) {
            co_yield b.finish();
            continue;
          }
          auto row = b.push_row();
          auto values = std::vector<std::string>{};
          if (!split_parser(*it, values)) {
            ctrl.warn(caf::make_error(ec::parse_error,
                                      fmt::format("zeek tsv parser skipped line: "
                                                  "parsing line failed")));
            continue;
          }
          if (values.size() != fields.size()) {
            ctrl.warn(caf::make_error(
              ec::parse_error,
              fmt::format("zeek tsv parser skipped line: expected {} fields but got "
                          "{}",
                          fields.size(), values.size())));
            continue;
          }
          for (const auto& [field, value] : detail::zip(fields, values)) {
            auto field_guard = row.push_field(field);
            field_guard.add(value);
            // TODO: Check what add() does with strings.
          }
        }
      },
      to_lines(std::move(loader)), ctrl);
  }

  auto default_loader([[maybe_unused]] std::span<std::string const> args) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"stdin", {}};
  }

  auto make_printer(std::span<std::string const> args, type input_schema,
                    operator_control_plane&) const
    -> caf::expected<printer> override {
    auto input_type = caf::get<record_type>(input_schema);
    return [input_type = std::move(input_type)](
             table_slice slice) -> generator<chunk_ptr> {

    };
  }

  auto default_saver(std::span<std::string const>) const
    -> std::pair<std::string, std::vector<std::string>> override {
    return {"directory", {"."}};
  }

  auto printer_allows_joining() const -> bool override {
    return false;
  };

  auto initialize(const record&, const record&) -> caf::error override {
    return {};
  }

  auto name() const -> std::string override {
    return "zeek";
  }
};

} // namespace
} // namespace vast::plugins::zeek_tsv

VAST_REGISTER_PLUGIN(vast::plugins::zeek_tsv::plugin)
