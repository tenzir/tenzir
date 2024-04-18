//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/format/reader_factory.hpp>
#include <tenzir/format/syslog.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/to_lines.hpp>

#include <ranges>

namespace tenzir::plugins::syslog {

namespace {

template <typename Message>
struct syslog_row {
  syslog_row(Message msg, size_t line_no)
    : parsed(std::move(msg)), starting_line_no(line_no) {
  }

  void emit_diag(std::string_view parser_name, diagnostic_handler& diag) const {
    if (line_count == 1) {
      diagnostic::error("syslog parser ({}) failed", parser_name)
        .note("input line number {}", starting_line_no)
        .emit(diag);
      return;
    }
    diagnostic::error("syslog parser ({}) failed", parser_name)
      .note("input lines number {} to {}", starting_line_no,
            starting_line_no + line_count - 1)
      .emit(diag);
  }

  Message parsed;
  size_t starting_line_no;
  size_t line_count{1};
};

struct syslog_builder {
public:
  using message_type = format::syslog::message;
  using row_type = syslog_row<message_type>;

  syslog_builder() = default;

  auto add_new(row_type&& row) -> void {
    rows_.emplace_back(std::move(row));
  }

  auto add_line_to_latest(std::string_view line) -> void {
    TENZIR_ASSERT(not rows_.empty());
    auto& latest = rows_.back();
    if (not latest.parsed.msg) {
      latest.parsed.msg.emplace(line);
    } else {
      latest.parsed.msg->push_back('\n');
      latest.parsed.msg->append(line);
    }
    ++latest.line_count;
  }

  auto rows() -> size_t {
    return rows_.size();
  }

  auto finish_all_but_last(diagnostic_handler& diag)
    -> std::optional<std::vector<table_slice>> {
    series_builder builder{format::syslog::make_syslog_type()};
    for (auto& row : std::views::take(rows_, rows_.size() - 1)) {
      if (not finish_single(row, builder, diag)) {
        return std::nullopt;
      }
    }
    if (not rows_.empty()) {
      rows_.erase(rows_.begin(), rows_.end() - 1);
    }
    return builder.finish_as_table_slice();
  }

  auto finish_all(diagnostic_handler& diag)
    -> std::optional<std::vector<table_slice>> {
    series_builder builder{format::syslog::make_syslog_type()};
    for (auto& row : rows_) {
      if (not finish_single(row, builder, diag)) {
        return std::nullopt;
      }
    }
    rows_.clear();
    return builder.finish_as_table_slice();
  }

private:
  static auto finish_single(const row_type& row, series_builder& builder,
                            diagnostic_handler& diag) -> bool {
    auto& msg = row.parsed;
    const record r{
      {"facility", msg.hdr.facility},
      {"severity", msg.hdr.severity},
      {"version", msg.hdr.version},
      {"timestamp", msg.hdr.ts},
      {"hostname", std::move(msg.hdr.hostname)},
      {"app_name", std::move(msg.hdr.app_name)},
      {"process_id", msg.hdr.process_id},
      {"message_id", msg.hdr.msg_id},
      {"structured_data", std::move(msg.data)},
      {"message", std::move(msg.msg)},
    };
    if (not builder.try_data(r)) {
      row.emit_diag("RFC 5242", diag);
      return false;
    }
    return true;
  }

  std::vector<row_type> rows_{};
};

struct legacy_syslog_builder {
public:
  using message_type = format::syslog::legacy_message;
  using row_type = syslog_row<message_type>;

  legacy_syslog_builder() = default;

  auto add_new(row_type&& row) -> void {
    rows_.emplace_back(std::move(row));
  }

  auto add_line_to_latest(std::string_view line) -> void {
    TENZIR_ASSERT(not rows_.empty());
    auto& latest = rows_.back();
    latest.parsed.content.push_back('\n');
    latest.parsed.content.append(line);
    ++latest.line_count;
  }

  auto rows() -> size_t {
    return rows_.size();
  }

  auto finish_all_but_last(diagnostic_handler& diag)
    -> std::optional<std::vector<table_slice>> {
    table_slice_builder builder{format::syslog::make_legacy_syslog_type()};
    for (auto& row : std::views::take(rows_, rows_.size() - 1)) {
      if (not finish_single(row, builder, diag)) {
        return std::nullopt;
      }
    }
    if (not rows_.empty()) {
      rows_.erase(rows_.begin(), rows_.end() - 1);
    }
    return std::vector{builder.finish()};
  }

  auto finish_all(diagnostic_handler& diag)
    -> std::optional<std::vector<table_slice>> {
    table_slice_builder builder{format::syslog::make_legacy_syslog_type()};
    for (auto& row : rows_) {
      if (not finish_single(row, builder, diag)) {
        return std::nullopt;
      }
    }
    rows_.clear();
    return std::vector{builder.finish()};
  }

private:
  static auto finish_single(const row_type& row, table_slice_builder& builder,
                            diagnostic_handler& diag) -> bool {
    auto& msg = row.parsed;
    if (not builder.add(msg.facility, msg.severity, msg.timestamp, msg.host,
                        msg.app_name, msg.process_id, msg.content)) {
      row.emit_diag("RFC 3164", diag);
      return false;
    }
    return true;
  }

  std::vector<row_type> rows_{};
};

struct unknown_syslog_builder {
public:
  using message_type = std::string;
  using row_type = syslog_row<message_type>;

  unknown_syslog_builder() = default;

  auto add_new(row_type&& row) -> void {
    rows_.emplace_back(std::move(row.parsed));
  }

  static auto add_line_to_latest(std::string_view) -> void {
    TENZIR_UNREACHABLE();
  }

  auto rows() -> size_t {
    return rows_.size();
  }

  static auto finish_all_but_last(diagnostic_handler&)
    -> std::optional<std::vector<table_slice>> {
    TENZIR_UNREACHABLE();
  }

  auto finish_all(diagnostic_handler&)
    -> std::optional<std::vector<table_slice>> {
    table_slice_builder builder{format::syslog::make_unknown_type()};
    for (auto& row : rows_) {
      // Adding a `syslog.unknown` can never fail,
      // it's just a field containing a string.
      auto r = builder.add(row);
      TENZIR_ASSERT(r);
    }
    rows_.clear();
    return std::vector{builder.finish()};
  }

private:
  std::vector<std::string> rows_;
};

auto impl(generator<std::optional<std::string_view>> lines,
          operator_control_plane& ctrl) -> generator<table_slice> {
  std::variant<syslog_builder, legacy_syslog_builder, unknown_syslog_builder>
    builder{std::in_place_type<unknown_syslog_builder>};
  const auto finish_all = [&]() {
    return std::visit(
      [&](auto& b) {
        return b.finish_all(ctrl.diagnostics());
      },
      builder);
  };
  const auto rows = [&]() {
    return std::visit(
      [&](auto& b) {
        return b.rows();
      },
      builder);
  };
  const auto add_new = [&]<typename Message>(Message&& msg, size_t line_no) {
    std::visit(
      [&]<typename Builder>(Builder& b) {
        if constexpr (std::is_constructible_v<typename Builder::message_type,
                                              std::remove_cvref_t<Message>>) {
          b.add_new(
            typename Builder::row_type{std::forward<Message>(msg), line_no});
        } else {
          TENZIR_UNREACHABLE();
        }
      },
      builder);
  };
  const auto change_builder
    = [&]<typename Builder>(
        tag<Builder>) -> std::optional<std::vector<table_slice>> {
    if (std::holds_alternative<Builder>(builder)) {
      return std::vector<table_slice>{};
    }
    auto finished = finish_all();
    builder.template emplace<Builder>();
    return finished;
  };
  auto last_finish = std::chrono::steady_clock::now();
  auto line_nr = size_t{0};
  for (auto&& line : lines) {
    const auto now = std::chrono::steady_clock::now();
    if (rows() >= defaults::import::table_slice_size
        or last_finish + defaults::import::batch_timeout < now) {
      last_finish = now;
      // Don't yield the last row contained in a builder other than
      // `unknown_syslog_builder`:
      // It's possible it's a multiline message, that would get cut in half
      const auto finish_on_periodic_yield = [&]() {
        return std::visit(detail::overload{
                            [&](auto& b) {
                              return b.finish_all_but_last(ctrl.diagnostics());
                            },
                            [&](unknown_syslog_builder& b) {
                              return b.finish_all(ctrl.diagnostics());
                            },
                          },
                          builder);
      };
      if (auto slices = finish_on_periodic_yield()) {
        for (auto&& slice : *slices) {
          if (slice.rows() > 0) {
            co_yield std::move(slice);
          }
        }
      } else {
        co_return;
      }
    }
    if (not line) {
      if (last_finish != now) {
        co_yield {};
      }
      continue;
    }
    ++line_nr;
    if (line->empty()) {
      continue;
    }
    const auto* f = line->begin();
    const auto* const l = line->end();
    format::syslog::message msg{};
    format::syslog::legacy_message legacy_msg{};
    if (auto parser = format::syslog::message_parser{}; parser(f, l, msg)) {
      // This line is a valid new-RFC (5424) syslog message.
      // Store it in the builder
      if (auto slices = change_builder(tag_v<syslog_builder>)) {
        for (auto&& slice : *slices) {
          if (slice.rows() > 0) {
            co_yield std::move(slice);
          }
        }
      } else {
        co_return;
      }
      add_new(std::move(msg), line_nr);
    } else if (auto legacy_parser = format::syslog::legacy_message_parser{};
               legacy_parser(f, l, legacy_msg)) {
      // Same as above, except it's an old-RFC (3164) syslog message.
      if (auto slices = change_builder(tag_v<legacy_syslog_builder>)) {
        for (auto&& slice : *slices) {
          if (slice.rows() > 0) {
            co_yield std::move(slice);
          }
        }
      } else {
        co_return;
      }
      add_new(std::move(legacy_msg), line_nr);
    } else if (std::holds_alternative<unknown_syslog_builder>(builder)) {
      // This line is not a valid syslog message.
      // The current builder is `unknown_syslog_builder`,
      // so this line will also become an event of type `syslog.unknown`.
      add_new(std::string{*line}, line_nr);
    } else {
      // This line is not a valid syslog message,
      // but the previous line was.
      // Let's assume that we have a multiline syslog message,
      // and append this current line to the previous message.
      std::visit(
        [&](auto& b) {
          b.add_line_to_latest(*line);
        },
        builder);
    }
  }
  if (auto slices = finish_all()) {
    for (auto&& slice : *slices) {
      if (slice.rows() > 0) {
        co_yield std::move(slice);
      }
    }
  }
}

class syslog_parser final : public plugin_parser {
public:
  syslog_parser() = default;

  auto name() const -> std::string override {
    return "syslog";
  }

  auto
  instantiate(generator<chunk_ptr> input, operator_control_plane& ctrl) const
    -> std::optional<generator<table_slice>> override {
    return impl(to_lines(std::move(input)), ctrl);
  }

  friend auto inspect(auto& f, syslog_parser& x) -> bool {
    return f.object(x).pretty_name("syslog_parser").fields();
  }
};

class plugin final : public virtual parser_plugin<syslog_parser> {
public:
  auto parse_parser(parser_interface& p) const
    -> std::unique_ptr<plugin_parser> override {
    auto parser = argument_parser{
      name(), fmt::format("https://docs.tenzir.com/formats/{}", name())};
    parser.parse(p);
    return std::make_unique<syslog_parser>();
  }
};

} // namespace

} // namespace tenzir::plugins::syslog

TENZIR_REGISTER_PLUGIN(tenzir::plugins::syslog::plugin)
