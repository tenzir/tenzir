//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/tql2/plugin.hpp"

#include "sigma/parse.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/bitmap.hpp>
#include <tenzir/concept/convertible/to.hpp>
#include <tenzir/concept/parseable/string/char_class.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/data.hpp>
#include <tenzir/error.hpp>
#include <tenzir/io/read.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>

#include <arrow/record_batch.h>
#include <caf/error.hpp>
#include <caf/expected.hpp>
#include <caf/typed_event_based_actor.hpp>
#include <fmt/format.h>

#include <chrono>
#include <filesystem>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace tenzir::plugins::sigma {

namespace {

struct RuleEntry {
  data yaml;
  expression rule;
};

using RuleMap = std::unordered_map<std::string, RuleEntry>;

auto load_rules(const std::filesystem::path& path, RuleMap& rules,
                diagnostic_handler& dh) -> void {
  if (std::filesystem::is_directory(path)) {
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
      load_rules(entry.path(), rules, dh);
    }
    return;
  }
  if (path.extension() != ".yml" and path.extension() != ".yaml") {
    // We silently ignore non-yaml files.
    return;
  }
  auto query = tenzir::io::read(path);
  if (not query) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to read file: {}", query.error())
      .emit(dh);
    return;
  }
  auto query_str = std::string_view{
    reinterpret_cast<const char*>(query->data()),
    reinterpret_cast<const char*>(query->data() + query->size())}; // NOLINT
  auto yaml = from_yaml(query_str);
  if (not yaml) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to parse yaml: {}", yaml.error())
      .emit(dh);
    return;
  }
  if (not is<record>(*yaml)) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("rule is not a YAML dictionary")
      .emit(dh);
    return;
  }
  auto rule = parse_rule(*yaml);
  if (not rule) {
    diagnostic::warning("sigma operator ignores rule '{}'", path.string())
      .note("failed to parse sigma rule: {}", rule.error())
      .emit(dh);
    return;
  }
  rules[path.string()] = {std::move(*yaml), std::move(*rule)};
}

auto update_rules(const std::filesystem::path& path, RuleMap& rules,
                  diagnostic_handler& dh) -> void {
  auto old_rules = std::exchange(rules, {});
  load_rules(path, rules, dh);
  for (const auto& [rule_path, rule] : rules) {
    const auto old_rule = old_rules.find(rule_path);
    if (old_rule == old_rules.end()) {
      TENZIR_VERBOSE("added Sigma rule {}", rule_path);
    } else if (old_rule->second.yaml != rule.yaml
               or old_rule->second.rule != rule.rule) {
      TENZIR_VERBOSE("updated Sigma rule {}", rule_path);
    }
  }
  for (const auto& [rule_path, _] : old_rules) {
    if (not rules.contains(rule_path)) {
      TENZIR_VERBOSE("removed Sigma rule {}", rule_path);
    }
  }
}

auto make_sigma_slice(const table_slice& input, const data& yaml,
                      const expression& rule) -> std::optional<table_slice> {
  auto expr = tailor(rule, input.schema());
  if (not expr) {
    return std::nullopt;
  }
  auto event = filter(input, *expr);
  if (not event) {
    return std::nullopt;
  }
  auto [event_schema, event_array] = offset{}.get(*event);
  auto [rule_schema, rule_array] = [&] {
    auto rule_builder = series_builder{};
    for (auto i = size_t{0}; i < event->rows(); ++i) {
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
  auto batch
    = arrow::RecordBatch::Make(result_schema.to_arrow_schema(), event->rows(),
                               {std::move(event_array), std::move(rule_array)});
  return table_slice{batch, result_schema};
}

class sigma_operator final : public crtp_operator<sigma_operator> {
public:
  sigma_operator() = default;

  explicit sigma_operator(duration refresh_interval, std::string path)
    : refresh_interval_{refresh_interval}, path_{std::move(path)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto rules = RuleMap{};
    auto path = std::filesystem::path{path_};
    update_rules(path, rules, ctrl.diagnostics());
    auto last_update = std::chrono::steady_clock::now();
    co_yield {}; // signal that we're done initializing
    for (auto&& slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto now = std::chrono::steady_clock::now();
      if (now - last_update > refresh_interval_) {
        update_rules(path, rules, ctrl.diagnostics());
        last_update = now;
      }
      for (const auto& [_, entry] : rules) {
        if (auto result = make_sigma_slice(slice, entry.yaml, entry.rule)) {
          co_yield std::move(*result);
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
    TENZIR_UNUSED(filter, order);
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

struct SigmaArgs {
  std::string path;
  duration refresh_interval = std::chrono::seconds{5};
};

class Sigma final : public Operator<table_slice, table_slice> {
public:
  explicit Sigma(SigmaArgs args) : args_{std::move(args)}, path_{args_.path} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    update_rules(path_, rules_, ctx.dh());
    last_update_ = std::chrono::steady_clock::now();
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto now = std::chrono::steady_clock::now();
    if (now - last_update_ > args_.refresh_interval) {
      update_rules(path_, rules_, ctx.dh());
      last_update_ = now;
    }
    for (const auto& [_, entry] : rules_) {
      if (auto result = make_sigma_slice(input, entry.yaml, entry.rule)) {
        co_await push(std::move(*result));
      }
    }
  }

  auto snapshot(Serde& serde) -> void override {
    TENZIR_UNUSED(serde);
    // Rules are reloaded from disk in `start()`, and `steady_clock`
    // timestamps are not portable across restarts.
  }

private:
  SigmaArgs args_;
  std::filesystem::path path_;
  RuleMap rules_;
  std::chrono::steady_clock::time_point last_update_ = {};
};

class plugin final : public virtual operator_plugin<sigma_operator>,
                     public virtual operator_factory_plugin,
                     public virtual OperatorPlugin {
public:
  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto make(operator_factory_invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto refresh_interval = std::optional<located<duration>>{};
    auto path = std::string{};
    argument_parser2::operator_("sigma")
      .positional("path", path)
      .named("refresh_interval", refresh_interval)
      .parse(inv, ctx)
      .ignore();
    auto interval
      = refresh_interval ? refresh_interval->inner : std::chrono::seconds{5};
    if (refresh_interval and interval <= duration::zero()) {
      diagnostic::error("`refresh_interval` must be a positive duration")
        .primary(refresh_interval.value())
        .emit(ctx);
      return failure::promise();
    }
    return std::make_unique<sigma_operator>(interval, std::move(path));
  }

  auto describe() const -> Description override {
    auto d = Describer<SigmaArgs, Sigma>{};
    d.positional("path", &SigmaArgs::path);
    auto refresh_interval
      = d.named_optional("refresh_interval", &SigmaArgs::refresh_interval);
    d.validate([refresh_interval](DescribeCtx& ctx) -> Empty {
      if (auto value = ctx.get(refresh_interval);
          value and *value <= duration::zero()) {
        diagnostic::error("`refresh_interval` must be a positive duration")
          .primary(ctx.get_location(refresh_interval).value())
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"sigma", "https://docs.tenzir.com/"
                                           "reference/operators"};
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
      if (refresh_interval <= duration::zero()) {
        diagnostic::error("`refresh_interval` must be a positive duration")
          .primary(refresh_interval_arg->source)
          .throw_();
      }
    }
    return std::make_unique<sigma_operator>(refresh_interval, std::move(path));
  }
};

} // namespace

} // namespace tenzir::plugins::sigma

TENZIR_REGISTER_PLUGIN(tenzir::plugins::sigma::plugin)
