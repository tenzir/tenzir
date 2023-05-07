//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline.hpp"

#include "vast/collect.hpp"
#include "vast/modules.hpp"
#include "vast/plugin.hpp"

namespace vast {

class local_control_plane final : public operator_control_plane {
public:
  auto get_error() const -> caf::error {
    return error_;
  }

  auto self() noexcept -> system::execution_node_actor::base& override {
    die("not implemented");
  }

  auto node() noexcept -> system::node_actor override {
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

auto pipeline::parse(std::string_view repr) -> caf::expected<pipeline> {
  // Get all query languages, but make sure that VAST is at the front.
  // TODO: let the user choose exactly one language instead.
  auto languages = collect(plugins::get<language_plugin>());
  if (const auto* vast = plugins::find<language_plugin>("VAST")) {
    const auto it = std::find(languages.begin(), languages.end(), vast);
    VAST_ASSERT_CHEAP(it != languages.end());
    std::rotate(languages.begin(), it, it + 1);
  }
  auto first_error = caf::error{};
  for (const auto& language : languages) {
    if (auto parsed = language->parse_query(repr))
      return parsed;
    else {
      VAST_DEBUG("failed to parse query as {} language: {}", language->name(),
                 parsed.error());
      if (!first_error) {
        first_error = std::move(parsed.error());
      }
    }
  }
  return caf::make_error(ec::syntax_error,
                         fmt::format("invalid query: {}", first_error));
}

auto pipeline::parse_as_operator(std::string_view repr)
  -> caf::expected<operator_ptr> {
  auto result = parse(repr);
  if (not result)
    return std::move(result.error());
  return std::make_unique<pipeline>(std::move(*result));
}

void pipeline::append(operator_ptr op) {
  if (auto* sub_pipeline = dynamic_cast<pipeline*>(&*op)) {
    auto sub_ops = std::move(*sub_pipeline).unwrap();
    operators_.insert(operators_.end(), std::move_iterator{sub_ops.begin()},
                      std::move_iterator{sub_ops.end()});
  } else {
    operators_.push_back(std::move(op));
  }
}

void pipeline::prepend(operator_ptr op) {
  if (auto* sub_pipeline = dynamic_cast<pipeline*>(&*op)) {
    auto sub_ops = std::move(*sub_pipeline).unwrap();
    operators_.insert(operators_.begin(), std::move_iterator{sub_ops.begin()},
                      std::move_iterator{sub_ops.end()});
  } else {
    operators_.insert(operators_.begin(), std::move(op));
  }
}

auto pipeline::unwrap() && -> std::vector<operator_ptr> {
  return std::move(operators_);
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
  VAST_DEBUG("instantiating '{}' for {}", *this, operator_type_name(input));
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
  for (const auto& op : operators_) {
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
