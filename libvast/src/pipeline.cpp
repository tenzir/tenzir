//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline.hpp"

#include "vast/collect.hpp"
#include "vast/detail/stack_vector.hpp"
#include "vast/lazy.hpp"
#include "vast/modules.hpp"
#include "vast/plugin.hpp"

namespace vast {

namespace {

class local_control_plane final : public operator_control_plane {
public:
  auto get_error() const -> caf::error {
    return error_;
  }

  auto self() noexcept -> system::execution_node_actor::base& override {
    // TODO: Can we return an error from here instead?
    die("operator_control_plane::self() must not be called for operators with "
        "location::anywhere()");
  }

  auto node() noexcept -> system::node_actor override {
    // TODO: Can we return an error from here instead?
    die("operator_control_plane::node() must not be called for operators with "
        "location::anywhere()");
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

auto make_local_executor_impl(pipeline self) -> generator<caf::expected<void>> {
  for (const auto& [_, op] : self.unwrap()) {
    if (op->location() != operator_location::anywhere) {
      co_yield caf::make_error(ec::logic_error,
                               fmt::format("operator '{}' must be run locally "
                                           "or remotely, which is not allowed "
                                           "in this context",
                                           *op));
      co_return;
    }
    if (op->detached()) {
      co_yield caf::make_error(
        ec::logic_error, fmt::format("operator '{}' must be run detached, "
                                     "which is not allowed in this context",
                                     *op));
      co_return;
    }
  }
  local_control_plane ctrl;
  auto dynamic_gen = self.instantiate(std::monostate{}, ctrl);
  if (not dynamic_gen) {
    co_yield std::move(dynamic_gen.error());
    co_return;
  }
  auto* gen = std::get_if<generator<std::monostate>>(&*dynamic_gen);
  if (not gen) {
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

} // namespace

pipeline::pipeline(std::vector<operator_ptr> operators,
                   std::optional<std::string> definition)
  : definition_{std::move(definition)
                  .value_or(
                    VAST_LAZY(fmt::to_string(fmt::join(operators, " | "))))},
    operators_{std::move(operators)} {
}

auto pipeline::parse(std::string definition) -> caf::expected<pipeline> {
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
    if (auto parsed = language->parse_query(definition))
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

auto pipeline::parse_as_operator(std::string definition)
  -> caf::expected<operator_ptr> {
  auto result = parse(std::move(definition));
  if (not result)
    return std::move(result.error());
  if (result->operators_.size() == 1) {
    return std::move(result->operators_.front());
  }
  return std::make_unique<pipeline>(std::move(*result));
}

void pipeline::push_back(operator_ptr op) {
  definition_ = fmt::format("{} | {}", definition_, op);
  operators_.push_back(std::move(op));
}

void pipeline::push_front(operator_ptr op) {
  definition_ = fmt::format("{} | {}", op, definition_);
  operators_.insert(operators_.begin(), std::move(op));
}

auto pipeline::unwrap()
  const& -> generator<std::pair<offset, const operator_base*>> {
  auto index = offset{0};
  auto history = detail::stack_vector<const pipeline*, 64>{this};
  while (not index.empty()) {
    VAST_ASSERT(history.size() == index.size());
    if (index.back() == history.back()->operators_.size()) {
      index.pop_back();
      index.back() += 1;
      history.pop_back();
      continue;
    }
    const auto& current = history.back()->operators_[index.back()];
    if (const auto* nested_pipeline
        = dynamic_cast<const pipeline*>(current.get())) {
      index.push_back(0);
      history.push_back(nested_pipeline);
      continue;
    }
    co_yield {index, current.get()};
    index.back() += 1;
  }
}

auto pipeline::copy() const -> operator_ptr {
  auto copied = std::make_unique<pipeline>();
  copied->definition_ = definition_;
  copied->operators_.reserve(operators_.size());
  for (const auto& op : operators_) {
    copied->operators_.push_back(op->copy());
  }
  return copied;
}

auto pipeline::to_string() const -> std::string {
  return definition_.empty() ? "pass" : definition_;
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
  return std::pair{
    std::move(result->first),
    std::make_unique<pipeline>(std::move(result->second)),
  };
}

auto pipeline::predicate_pushdown_pipeline(expression const& expr) const
  -> std::optional<std::pair<expression, pipeline>> {
  auto new_operators_ = std::vector<operator_ptr>{};
  auto current = expr;
  for (auto it = operators_.rbegin(); it != operators_.rend(); ++it) {
    if (auto result = (*it)->predicate_pushdown(current)) {
      auto& [new_expr, replacement] = *result;
      if (replacement) {
        new_operators_.push_back(std::move(replacement));
      }
      current = new_expr;
    } else {
      if (current != trivially_true_expression()) {
        auto where = parse_as_operator(fmt::format("where {}", current));
        VAST_ASSERT(where);
        new_operators_.push_back(std::move(*where));
        current = trivially_true_expression();
      }
      auto copy = (*it)->copy();
      VAST_ASSERT(copy);
      new_operators_.push_back(std::move(copy));
    }
  }
  std::reverse(new_operators_.begin(), new_operators_.end());
  return std::pair{
    std::move(current),
    pipeline{std::move(new_operators_), definition_},
  };
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
    const auto first = &op == &operators_.front();
    if (not first && current.is<void>()) {
      return caf::make_error(
        ec::type_clash,
        fmt::format("pipeline continues with '{}' after sink", op));
    }
    auto next = op->infer_type(current);
    if (!next) {
      return next.error();
    }
    current = *next;
  }
  return current;
}

auto pipeline::make_local_executor() && -> generator<caf::expected<void>> {
  return make_local_executor_impl(std::move(*this));
}

auto pipeline::make_local_executor() const& -> generator<caf::expected<void>> {
  return make_local_executor_impl(*this);
}

auto pipeline::optimize() -> caf::expected<void> {
  VAST_ASSERT(is_closed());
  auto result = predicate_pushdown_pipeline(trivially_true_expression());
  if (not result) {
    return caf::make_error(ec::logic_error, "failed to optimize pipeline");
  }
  if (result->first != trivially_true_expression()) {
    return caf::make_error(
      ec::logic_error, fmt::format("failed to optimize pipeline: first "
                                   "operator pushed unexpected expression {}",
                                   result->first));
  }
  *this = std::move(result->second);
  return {};
}

pipeline::pipeline(pipeline const& other) : definition_{other.definition_} {
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
