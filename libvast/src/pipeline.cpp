//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/parseable/vast/pipeline.hpp"

#include "vast/modules.hpp"
#include "vast/pipeline.hpp"
#include "vast/plugin.hpp"

namespace vast {

class local_control_plane final : public operator_control_plane {
public:
  auto get_error() const -> caf::error {
    return error_;
  }

  auto self() noexcept -> caf::event_based_actor& override {
    die("not implemented");
  }

  auto abort(caf::error error) noexcept -> void override {
    VAST_ASSERT(error != caf::none);
    error_ = error;
  }

  auto warn(caf::error error) noexcept -> void override {
    VAST_WARN("{}", error);
  }

  auto emit(table_slice) noexcept -> void override {
    die("not implemented");
  }

  auto demand(type = {}) const noexcept -> size_t override {
    die("not implemented");
  }

  auto schemas() const noexcept -> const std::vector<type>& override {
    return vast::modules::schemas();
  }

  auto concepts() const noexcept -> const concepts_map& override {
    return vast::modules::concepts();
  }

private:
  caf::error error_{};
};

pipeline::pipeline(std::vector<operator_ptr> operators) {
  operators_.reserve(operators.size());
  for (auto&& op : operators) {
    if (auto sub_pipeline = dynamic_cast<pipeline*>(&*op)) {
      auto sub_ops = std::move(*sub_pipeline).unwrap();
      operators_.insert(operators_.end(), std::move_iterator{sub_ops.begin()},
                        std::move_iterator{sub_ops.end()});
    } else {
      operators_.push_back(std::move(op));
    }
  }
}

namespace {

auto parse_impl(std::string_view repr, const vast::record& config,
                std::unordered_set<std::string>& recursed)
  -> caf::expected<pipeline> {
  auto ops = std::vector<operator_ptr>{};
  // plugin name parser
  using parsers::plugin_name, parsers::chr, parsers::space,
    parsers::optional_ws_or_comment, parsers::end_of_pipeline_operator;
  const auto operator_name_parser = optional_ws_or_comment >> plugin_name;
  // TODO: allow more empty string
  while (!repr.empty()) {
    // 1. parse a single word as operator plugin name
    const auto* f = repr.begin();
    const auto* const l = repr.end();
    auto operator_name = std::string{};
    if (!operator_name_parser(f, l, operator_name)) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("failed to parse pipeline '{}': "
                                         "operator name is invalid",
                                         repr));
    }
    // 2a. find plugin using operator name
    const auto* plugin = plugins::find<operator_plugin>(operator_name);
    // 2b. find alias definition in `vast.operators` (and `vast.operators`)
    auto new_config_prefix = "vast.operators";
    auto old_config_prefix = "vast.pipelines";
    auto definition = static_cast<const std::string*>(nullptr);
    auto used_config_prefix = std::string{};
    auto used_config_key = std::string{};
    for (auto prefix : {new_config_prefix, old_config_prefix}) {
      auto key = fmt::format("{}.{}", prefix, operator_name);
      auto type_clash = bool{};
      definition = get_if<std::string>(&config, key, &type_clash);
      if (type_clash) {
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("malformed config: `{}` must "
                                           "be a record that maps to strings",
                                           prefix));
      }
      if (definition) {
        if (prefix == old_config_prefix) {
          VAST_WARN("configuring operator aliases with `{}` is deprecated, use "
                    "`{}` instead",
                    old_config_prefix, new_config_prefix);
        }
        used_config_prefix = prefix;
        used_config_key = key;
        break;
      }
    }
    if (plugin && definition) {
      return caf::make_error(ec::lookup_error,
                             "the operator {} is defined by a plugin, but also "
                             "by the `{}` config",
                             operator_name, used_config_prefix);
    }
    if (plugin) {
      // 3a. ask the plugin to parse itself from the remainder
      auto [remaining_repr, op] = plugin->make_operator(std::string_view{f, l});
      if (!op)
        return caf::make_error(ec::unspecified,
                               fmt::format("failed to parse pipeline '{}': {}",
                                           repr, op.error()));
      ops.push_back(std::move(*op));
      repr = remaining_repr;
    } else if (definition) {
      // 3b. parse the definition of the operator recursively
      auto [_, inserted] = recursed.emplace(operator_name);
      if (!inserted)
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("the definition of "
                                           "`{}` is recursive",
                                           used_config_key));
      auto result = parse_impl(*definition, config, recursed);
      recursed.erase(operator_name);
      if (!result)
        return caf::make_error(ec::invalid_configuration,
                               fmt::format("{} (while parsing `{}`)",
                                           result.error(), used_config_key));
      ops.push_back(std::make_unique<pipeline>(std::move(*result)));
      auto rest = optional_ws_or_comment >> end_of_pipeline_operator;
      if (!rest(f, l, unused))
        return caf::make_error(
          ec::unspecified,
          fmt::format("expected end of operator while parsing '{}'", repr));
      repr = std::string_view{f, l};
    } else {
      return caf::make_error(ec::syntax_error,
                             fmt::format("failed to parse pipeline '{}': "
                                         "operator '{}' does not exist",
                                         repr, operator_name));
    }
  }
  return pipeline{std::move(ops)};
}

} // namespace

auto pipeline::parse(std::string_view repr, const vast::record& config)
  -> caf::expected<pipeline> {
  auto recursed = std::unordered_set<std::string>{};
  return parse_impl(repr, config, recursed);
}

auto pipeline::parse_as_operator(std::string_view repr,
                                 const vast::record& config)
  -> caf::expected<operator_ptr> {
  auto result = parse(repr, config);
  if (not result)
    return std::move(result.error());
  return std::make_unique<pipeline>(std::move(*result));
}

auto pipeline::copy() const -> operator_ptr {
  auto copied = std::make_unique<pipeline>();
  copied->operators_.reserve(operators_.size());
  for (const auto& op : operators_) {
    copied->operators_.push_back(op->copy());
  }
  return copied;
}

auto pipeline::to_string() const -> std::string {
  if (operators_.empty()) {
    return "pass";
  }
  return fmt::to_string(fmt::join(operators_, " | "));
}

auto pipeline::instantiate(operator_input input,
                           operator_control_plane& control) const
  -> caf::expected<operator_output> {
  if (operators_.empty()) {
    auto f = detail::overload{
      [](std::monostate) -> operator_output {
        return generator<std::monostate>{};
      },
      []<class Input>(generator<Input> input) -> operator_output {
        return input;
      },
    };
    return std::visit(f, std::move(input));
  }
  auto it = operators_.begin();
  auto end = operators_.end();
  while (true) {
    auto output = (*it)->instantiate(std::move(input), control);
    if (!output) {
      return output.error();
    }
    ++it;
    if (it == end) {
      return output;
    }
    auto f = detail::overload{
      [](generator<std::monostate>) -> operator_input {
        return std::monostate{};
      },
      []<class Output>(generator<Output> output) -> operator_input {
        return output;
      },
    };
    input = std::visit(f, std::move(*output));
    if (std::holds_alternative<std::monostate>(input)) {
      return caf::make_error(ec::type_clash, "pipeline ended before all "
                                             "operators were used");
    }
  }
}

auto pipeline::predicate_pushdown(const expression& expr) const
  -> std::optional<std::pair<expression, operator_ptr>> {
  auto result = predicate_pushdown_pipeline(expr);
  if (!result) {
    return {};
  }
  return std::pair{std::move(result->first),
                   std::make_unique<pipeline>(std::move(result->second))};
}

auto pipeline::predicate_pushdown_pipeline(expression const& expr) const
  -> std::optional<std::pair<expression, pipeline>> {
  auto new_rev = std::vector<operator_ptr>{};

  auto current = expr;
  for (auto it = operators_.rbegin(); it != operators_.rend(); ++it) {
    if (auto result = (*it)->predicate_pushdown(current)) {
      auto& [new_expr, replacement] = *result;
      if (replacement) {
        new_rev.push_back(std::move(replacement));
      }
      current = new_expr;
    } else {
      if (current != trivially_true_expression()) {
        // TODO: We just want to create a `where current` operator. However, we
        // currently only have the interface for parsing this from a string.
        auto where_plugin = plugins::find<operator_plugin>("where");
        auto string = fmt::format(" {}", current);
        auto [rest, op] = where_plugin->make_operator(string);
        VAST_ASSERT(rest.empty());
        VAST_ASSERT(op);
        new_rev.push_back(std::move(*op));
        current = trivially_true_expression();
      }
      auto copy = (*it)->copy();
      VAST_ASSERT(copy);
      new_rev.push_back(std::move(copy));
    }
  }

  std::reverse(new_rev.begin(), new_rev.end());
  return std::pair{std::move(current), pipeline{std::move(new_rev)}};
}

auto operator_base::infer_type_impl(operator_type input) const
  -> caf::expected<operator_type> {
  auto ctrl = local_control_plane{};
  auto f = [&]<class Input>(tag<Input>) {
    if constexpr (std::is_same_v<Input, void>) {
      return instantiate(std::monostate{}, ctrl);
    } else {
      return instantiate(generator<Input>{}, ctrl);
    }
  };
  auto output = std::visit(f, input);
  if (!output) {
    return output.error();
  }
  return std::visit(
    [&]<class Output>(generator<Output>&) -> operator_type {
      if constexpr (std::is_same_v<Output, std::monostate>) {
        return tag_v<void>;
      } else {
        return tag_v<Output>;
      }
    },
    *output);
}

auto pipeline::is_closed() const -> bool {
  return !!check_type<void, void>();
}

auto pipeline::infer_type_impl(operator_type input) const
  -> caf::expected<operator_type> {
  auto current = input;
  for (auto& op : operators_) {
    auto first = &op == &operators_.front();
    if (!first && current.is<void>()) {
      return caf::make_error(
        ec::type_clash,
        fmt::format("pipeline continues with {} after sink", op->to_string()));
    }
    auto next = op->infer_type(current);
    if (!next) {
      return next.error();
    }
    current = *next;
  }
  return current;
}

auto make_local_executor(pipeline p) -> generator<caf::expected<void>> {
  local_control_plane ctrl;
  auto dynamic_gen = p.instantiate(std::monostate{}, ctrl);
  if (!dynamic_gen) {
    co_yield std::move(dynamic_gen.error());
    co_return;
  }
  auto gen = std::get_if<generator<std::monostate>>(&*dynamic_gen);
  if (!gen) {
    co_yield caf::make_error(ec::logic_error,
                             "right side of pipeline is not closed");
    co_return;
  }
  for (auto monostate : *gen) {
    if (auto error = ctrl.get_error()) {
      co_yield std::move(error);
      co_return;
    }
    (void)monostate;
    co_yield {};
  }
  if (auto error = ctrl.get_error()) {
    co_yield std::move(error);
  }
}

pipeline::pipeline(pipeline const& other) {
  operators_.reserve(other.operators_.size());
  for (const auto& op : other.operators_) {
    operators_.push_back(op->copy());
  }
}

auto pipeline::operator=(pipeline const& other) -> pipeline& {
  if (this != &other) {
    *this = pipeline{other};
  }
  return *this;
}

} // namespace vast
