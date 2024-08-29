//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "sigma/parse.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/bitmap.hpp>
#include <tenzir/concept/convertible/data.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/io/read.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice_builder.hpp>

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

namespace tenzir::plugins::sigma {

class sigma_operator final : public crtp_operator<sigma_operator> {
public:
  sigma_operator() = default;

  explicit sigma_operator(duration refresh_interval, std::string path)
    : refresh_interval_{refresh_interval}, path_{std::move(path)} {
  }

  struct monitor_state {
    auto update(const std::filesystem::path& path, operator_control_plane& ctrl)
      -> void {
      auto old_rules = std::exchange(rules, {});
      if (std::filesystem::is_directory(path)) {
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
          update(entry.path(), ctrl);
        }
        return;
      }
      if (path.extension() != ".yml" && path.extension() != ".yaml") {
        // We silently ignore non-yaml files.
        return;
      }
      auto query = tenzir::io::read(path);
      if (not query) {
        diagnostic::warning("sigma operator ignores rule '{}'", path.string())
          .note("failed to read file: {}", query.error())
          .emit(ctrl.diagnostics());
        return;
      }
      auto query_str = std::string_view{
        reinterpret_cast<const char*>(query->data()),
        reinterpret_cast<const char*>(query->data() + query->size())}; // NOLINT
      auto yaml = from_yaml(query_str);
      if (not yaml) {
        diagnostic::warning("sigma operator ignores rule '{}'", path.string())
          .note("failed to parse yaml: {}", yaml.error())
          .emit(ctrl.diagnostics());
        return;
      }
      if (not caf::holds_alternative<record>(*yaml)) {
        diagnostic::warning("sigma operator ignores rule '{}'", path.string())
          .note("rule is not a YAML dictionary")
          .emit(ctrl.diagnostics());
        return;
      }
      auto rule = parse_rule(*yaml);
      if (not rule) {
        diagnostic::warning("sigma operator ignores rule '{}'", path.string())
          .note("failed to parse sigma rule: {}", rule.error())
          .emit(ctrl.diagnostics());
        return;
      }
      rules[path.string()] = {std::move(*yaml), std::move(*rule)};
      for (const auto& [path, rule] : rules) {
        const auto old_rule = old_rules.find(path);
        if (old_rule == old_rules.end()) {
          TENZIR_VERBOSE("added Sigma rule {}", path);
        } else if (old_rule->second != rule) {
          TENZIR_VERBOSE("updated Sigma rule {}", path);
        }
      }
      for (const auto& [path, _] : old_rules) {
        if (not rules.contains(path)) {
          TENZIR_VERBOSE("removed Sigma rule {}", path);
        }
      }
    }

    std::filesystem::path path;
    std::unordered_map<std::string, std::pair<data, expression>> rules = {};
  };

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto state = monitor_state{};
    state.path = path_;
    state.update(state.path, ctrl);
    auto last_update = std::chrono::steady_clock::now();
    co_yield {}; // signal that we're done initializing
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (last_update + refresh_interval_ < std::chrono::steady_clock::now()) {
        state.update(state.path, ctrl);
        last_update = std::chrono::steady_clock::now();
      }
      for (const auto& [path, entry] : state.rules) {
        const auto& [yaml, rule] = entry;
        auto expr = tailor(rule, slice.schema());
        if (not expr) {
          continue;
        }
        if (auto event = filter(slice, *expr)) {
          auto [event_schema, event_array] = offset{}.get(*event);
          auto [rule_schema, rule_array] = [&] {
            auto rule_builder = series_builder{};
            for (size_t i = 0; i < event->rows(); ++i) {
              rule_builder.data(yaml);
            }
            return rule_builder.finish_assert_one_array();
          }();
          const auto result_schema = type{
            "tenzir.sigma",
            record_type{
              {"event", event_schema},
              {"rule", rule_schema},
            },
          };
          auto batch = arrow::RecordBatch::Make(
            result_schema.to_arrow_schema(),
            detail::narrow_cast<int64_t>(event->rows()),
            {std::move(event_array), std::move(rule_array)});
          co_yield table_slice{batch, result_schema};
        }
      }
    }
  }

  auto name() const -> std::string override {
    return "sigma";
  }

  auto location() const -> operator_location override {
    // The operator is referring to files, and the user likely assumes that to
    // be relative to the current process, so we default to local here.
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)order;
    (void)filter;
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, sigma_operator& x) -> bool {
    return f.object(x)
      .pretty_name("sigma_operator")
      .fields(f.field("refresh_interval", x.refresh_interval_),
              f.field("path", x.path_));
  }

private:
  duration refresh_interval_ = {};
  std::string path_ = {};
};

class plugin final : public virtual operator_plugin<sigma_operator>,
                     public virtual operator_factory_plugin {
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto refresh_interval = std::optional<located<duration>>{};
    auto path = std::string{};
    argument_parser2::operator_("sigma")
      .add(path, "<rule-or-directory>")
      .add("refresh_interval", refresh_interval)
      .parse(inv, ctx)
      .ignore();
    if (not refresh_interval) {
      refresh_interval->inner = std::chrono::seconds(5);
    } else if (refresh_interval->inner.count() < 0) {
      diagnostic::error("refresh_interval must be greater than 0")
        .primary(refresh_interval.value())
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<sigma_operator>(refresh_interval->inner,
                                            std::move(path));
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"sigma", "https://docs.tenzir.com/"
                                           "operators/batch"};
    auto refresh_interval = duration{std::chrono::seconds{5}};
    auto refresh_interval_arg = std::optional<located<std::string>>{};
    auto path = std::string{};
    parser.add("--refresh-interval", refresh_interval_arg,
               "<refresh-interval>");
    parser.add(path, "<rule-or-directory>");
    parser.parse(p);
    if (refresh_interval_arg) {
      if (not parsers::duration(refresh_interval_arg->inner,
                                refresh_interval)) {
        diagnostic::error("refresh interval is not a valid duration")
          .primary(refresh_interval_arg->source)
          .throw_();
      }
    }
    return std::make_unique<sigma_operator>(refresh_interval, std::move(path));
  }
};

} // namespace tenzir::plugins::sigma

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::plugin)
