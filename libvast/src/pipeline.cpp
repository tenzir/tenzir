//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/pipeline.hpp"

#include "vast/collect.hpp"
#include "vast/diagnostics.hpp"
#include "vast/modules.hpp"
#include "vast/plugin.hpp"
#include "vast/tql/parser.hpp"

#include <caf/detail/stringification_inspector.hpp>
#include <caf/fwd.hpp>

namespace vast {

class local_control_plane final : public operator_control_plane {
public:
  auto get_error() const -> caf::error {
    return error_;
  }

  auto self() noexcept -> exec_node_actor::base& override {
    die("not implemented");
  }

  auto node() noexcept -> node_actor override {
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

  auto diagnostics() noexcept -> diagnostic_handler& override {
    class handler final : public diagnostic_handler {
    public:
      void emit(diagnostic d) override {
        VAST_WARN("got diagnostic: {}", d);
        error_ |= d.severity == severity::error;
      }

      auto has_seen_error() const -> bool override {
        return error_;
      }

    private:
      bool error_ = false;
    };
    static auto diag = handler{};
    return diag;
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

auto pipeline::deserialize_op(inspector& f) -> operator_ptr {
  auto name = std::string{};
  if (!f.apply(name)) {
    return nullptr;
  }
  if (name == pipeline{}.name()) {
    // There is no pipeline plugin (maybe there should be?), so we
    // special-case this here.
    auto pipe = std::make_unique<pipeline>();
    if (!f.apply(*pipe)) {
      return nullptr;
    }
    return pipe;
  }
  auto const* p = plugins::find<operator_serialization_plugin>(name);
  if (!p) {
    f.set_error(
      caf::make_error(ec::serialization_error,
                      fmt::format("could not find plugin `{}`", name)));
    return nullptr;
  }
  auto op = operator_ptr{};
  p->deserialize(f, op);
  if (!op) {
    f.set_error(caf::make_error(ec::serialization_error,
                                fmt::format("plugin `{}` returned nullptr: {}",
                                            p->name(), f.get_error())));
    return nullptr;
  }
  return op;
}

auto pipeline::serialize_op(const operator_base& op, inspector& f) -> bool {
  auto name = op.name();
  auto const* p = plugins::find<operator_serialization_plugin>(name);
  if (!p) {
    f.set_error(
      caf::make_error(ec::serialization_error, "could not find plugin"));
    return false;
  }
  return f.apply(name) && p->serialize(f, op);
}

auto pipeline::internal_parse(std::string_view repr)
  -> caf::expected<pipeline> {
  return tql::parse_internal(std::string{repr});
}

auto pipeline::internal_parse_as_operator(std::string_view repr)
  -> caf::expected<operator_ptr> {
  auto result = internal_parse(repr);
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

auto pipeline::optimize() const -> caf::expected<pipeline> {
  auto optimized = predicate_pushdown_pipeline(trivially_true_expression());
  if (not optimized) {
    return caf::make_error(ec::logic_error, "failed to optimize pipeline '{}'",
                           *this);
  }

  if (optimized->first != trivially_true_expression()) {
    if (this->infer_type<void>()) {
      return caf::make_error(ec::logic_error,
                             "failed to optimizie pipeline '{}': source pushed "
                             "expression {}",
                             *this, optimized->first);
    }
    // Prepend where operator as a hacky way to handle pipelines without source.
    auto where = pipeline::internal_parse_as_operator(
      fmt::format("where {}", optimized->first));
    VAST_ASSERT(where);
    optimized->second.prepend(std::move(*where));
  }

  return std::move(optimized->second);
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
        // TODO: We just want to create a `where {current}` operator. However,
        // we currently only have the interface for parsing this from a string.
        auto pipe = tql::parse_internal(fmt::format("where {}", current));
        VAST_ASSERT(pipe);
        auto ops = std::move(*pipe).unwrap();
        VAST_ASSERT(ops.size() == 1);
        new_rev.push_back(std::move(ops[0]));
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

auto operator_base::copy() const -> operator_ptr {
  // TODO
  auto p = plugins::find<operator_serialization_plugin>(name());
  VAST_ASSERT(p);
  auto buffer = caf::byte_buffer{};
  auto serializer = caf::binary_serializer{nullptr, buffer};
  auto f = inspector{serializer};
  auto success = p->serialize(f, *this);
  VAST_ASSERT(success);
  auto deserializer = caf::binary_deserializer{nullptr, buffer};
  f = inspector{deserializer};
  auto copy = operator_ptr{};
  p->deserialize(f, copy);
  VAST_ASSERT(copy);
  return copy;
}

auto operator_base::to_string() const -> std::string {
  // TODO: Improve this output, perhaps by using JSON and rendering some field
  // type, for instance expressions, as strings.
  auto s = std::string{};
  auto f = caf::detail::stringification_inspector{s};
  auto g = inspector{f};
  auto const* p = plugins::find<operator_serialization_plugin>(name());
  if (!p) {
    return fmt::format("{} <error: plugin not found>", name());
  }
  if (!p->serialize(g, *this)) {
    return fmt::format("{} <error: serialize failed>", name());
  }
  return fmt::format("{} {}", name(), s);
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
      return caf::make_error(ec::type_clash, fmt::format("pipeline continues "
                                                         "with {} after sink",
                                                         op->to_string()));
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

auto inspector::is_loading() -> bool {
  return std::visit(
    [&]<class Inspector>(std::reference_wrapper<Inspector>) {
      return Inspector::is_loading;
    },
    *this);
}

void inspector::set_error(caf::error e) {
  return std::visit(
    [&](auto& f) {
      f.get().set_error(e);
    },
    *this);
}

auto inspector::get_error() const -> const caf::error& {
  return std::visit(
    [](auto&& f) -> const caf::error& {
      return f.get().get_error();
    },
    *this);
}

} // namespace vast
