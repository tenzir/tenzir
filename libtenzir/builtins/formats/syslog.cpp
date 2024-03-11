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

namespace tenzir::plugins::syslog {

namespace {

struct syslog_builder {
public:
  using message_type = format::syslog::message;

  syslog_builder() : builder_(format::syslog::make_syslog_type()) {
  }

  auto add(const message_type& msg) -> bool {
    record r{
      {"facility", msg.hdr.facility},     {"severity", msg.hdr.severity},
      {"version", msg.hdr.version},       {"timestamp", msg.hdr.ts},
      {"hostname", msg.hdr.hostname},     {"app_name", msg.hdr.app_name},
      {"process_id", msg.hdr.process_id}, {"message_id", msg.hdr.msg_id},
      {"structured_data", msg.data},      {"message", msg.msg},
    };
    return builder_.try_data(r).operator bool();
  }

  auto rows() -> size_t {
    return static_cast<size_t>(builder_.length());
  }

  auto finish() -> std::vector<table_slice> {
    return builder_.finish_as_table_slice();
  }

private:
  series_builder builder_;
};

struct legacy_syslog_builder {
public:
  using message_type = format::syslog::legacy_message;

  legacy_syslog_builder()
    : builder_(format::syslog::make_legacy_syslog_type()) {
  }

  auto add(const message_type& msg) -> bool {
    return builder_.add(msg.facility, msg.severity, msg.timestamp, msg.host,
                        msg.app_name, msg.process_id, msg.content);
  }

  auto rows() -> size_t {
    return builder_.rows();
  }

  auto finish() -> std::vector<table_slice> {
    if (rows() == 0) {
      return {};
    }
    return {builder_.finish()};
  }

private:
  table_slice_builder builder_;
};

struct unknown_syslog_builder {
public:
  using message_type = std::string_view;

  unknown_syslog_builder() : builder_(format::syslog::make_unknown_type()) {
  }

  auto add(message_type line) -> bool {
    return builder_.add(line);
  }

  auto rows() -> size_t {
    return builder_.rows();
  }

  auto finish() -> std::vector<table_slice> {
    if (rows() == 0) {
      return {};
    }
    return {builder_.finish()};
  }

private:
  table_slice_builder builder_;
};

auto impl(generator<std::optional<std::string_view>> lines,
          operator_control_plane& ctrl) -> generator<table_slice> {
  std::variant<syslog_builder, legacy_syslog_builder, unknown_syslog_builder>
    builder{std::in_place_type<unknown_syslog_builder>};
  const auto finish = [&]() {
    return std::visit(
      [&](auto& b) {
        return b.finish();
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
  const auto add = [&](const auto& msg) {
    return std::visit(
      [&](auto& b) -> bool {
        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(msg)>,
                                     typename std::remove_reference_t<
                                       decltype(b)>::message_type>) {
          return b.add(msg);
        } else {
          TENZIR_UNREACHABLE();
        }
      },
      builder);
  };
  const auto change_builder
    = [&]<typename Builder>(tag<Builder>) -> std::vector<table_slice> {
    if (std::holds_alternative<Builder>(builder)) {
      return {};
    }
    auto finished = finish();
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
      for (auto&& slice : finish()) {
        co_yield std::move(slice);
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
      for (auto&& slice : change_builder(tag_v<syslog_builder>)) {
        co_yield std::move(slice);
      }
      if (not add(msg)) {
        diagnostic::error(
          "syslog parser (RFC 5242) failed to produce table slice")
          .note("line number {}", line_nr)
          .hint("line: `{}`", *line)
          .emit(ctrl.diagnostics());
        co_return;
      }
    } else if (auto legacy_parser = format::syslog::legacy_message_parser{};
               legacy_parser(f, l, legacy_msg)) {
      for (auto&& slice : change_builder(tag_v<legacy_syslog_builder>)) {
        co_yield std::move(slice);
      }
      if (not add(legacy_msg)) {
        diagnostic::error(
          "syslog parser (RFC 3164) failed to produce table slice")
          .note("line number {}", line_nr)
          .hint("line: `{}`", *line)
          .emit(ctrl.diagnostics());
        co_return;
      }
    } else {
      for (auto&& slice : change_builder(tag_v<unknown_syslog_builder>)) {
        co_yield std::move(slice);
      }
      if (not add(*line)) {
        diagnostic::error(
          "syslog parser (unknown format) failed to produce table slice")
          .note("line number {}", line_nr)
          .hint("line: `{}`", *line)
          .emit(ctrl.diagnostics());
        co_return;
      }
    }
  }
  for (auto slices = finish(); auto&& slice : slices) {
    co_yield std::move(slice);
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
      name(), fmt::format("https://docs.tenzir.com/docs/formats/{}", name())};
    parser.parse(p);
    return std::make_unique<syslog_parser>();
  }
};

} // namespace

} // namespace tenzir::plugins::syslog

TENZIR_REGISTER_PLUGIN(tenzir::plugins::syslog::plugin)
