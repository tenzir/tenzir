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
#include <tenzir/table_slice_builder.hpp>
#include <tenzir/to_lines.hpp>

namespace tenzir::plugins::syslog {

namespace {

auto impl(generator<std::optional<std::string_view>> lines,
          operator_control_plane& ctrl) -> generator<table_slice> {
  std::optional<table_slice_builder> builder{std::nullopt};
  auto syslog_type = format::syslog::make_syslog_type();
  auto legacy_syslog_type = format::syslog::make_legacy_syslog_type();
  auto unknown_type = format::syslog::make_unknown_type();
  auto last_finish = std::chrono::steady_clock::now();
  auto line_nr = size_t{0};
  auto change_builder_schema = [&](const type& t) -> table_slice {
    if (builder && builder->schema() == t) {
      return {};
    }
    if (builder) {
      TENZIR_ASSERT_EXPENSIVE(builder->schema() != t);
      auto slice = builder->finish();
      builder.emplace(t);
      return slice;
    }
    builder.emplace(t);
    return {};
  };
  for (auto&& line : lines) {
    const auto now = std::chrono::steady_clock::now();
    if (builder
        and (builder->rows() >= defaults::import::table_slice_size
             or last_finish + defaults::import::batch_timeout < now)) {
      last_finish = now;
      co_yield builder->finish();
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
      if (auto slice = change_builder_schema(syslog_type); slice.rows() > 0) {
        co_yield std::move(slice);
      }
      if (not builder->add(msg.hdr.facility, msg.hdr.severity, msg.hdr.version,
                           msg.hdr.ts, msg.hdr.hostname, msg.hdr.app_name,
                           msg.hdr.process_id, msg.hdr.msg_id, msg.msg)) {
        diagnostic::error(
          "syslog parser (RFC 5242) failed to produce table slice")
          .note("line number {}", line_nr)
          .hint("line: `{}`", *line)
          .emit(ctrl.diagnostics());
        co_return;
      }
    } else if (auto legacy_parser = format::syslog::legacy_message_parser{};
               legacy_parser(f, l, legacy_msg)) {
      if (auto slice = change_builder_schema(legacy_syslog_type);
          slice.rows() > 0) {
        co_yield std::move(slice);
      }
      if (not builder->add(legacy_msg.facility, legacy_msg.severity,
                           legacy_msg.timestamp, legacy_msg.host,
                           legacy_msg.tag, legacy_msg.content)) {
        diagnostic::error(
          "syslog parser (RFC 3164) failed to produce table slice")
          .note("line number {}", line_nr)
          .hint("line: `{}`", *line)
          .emit(ctrl.diagnostics());
        co_return;
      }
    } else {
      if (auto slice = change_builder_schema(unknown_type); slice.rows() > 0) {
        co_yield std::move(slice);
      }
      if (not builder->add(*line)) {
        diagnostic::error(
          "syslog parser (unknown format) failed to produce table slice")
          .note("line number {}", line_nr)
          .hint("line: `{}`", *line)
          .emit(ctrl.diagnostics());
        co_return;
      }
    }
  }
  if (builder && builder->rows() > 0)
    co_yield builder->finish();
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
