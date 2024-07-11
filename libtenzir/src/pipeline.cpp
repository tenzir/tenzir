//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pipeline.hpp"

#include "tenzir/diagnostics.hpp"
#include "tenzir/metric_handler.hpp"
#include "tenzir/modules.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tql/parser.hpp"

#include <caf/detail/stringification_inspector.hpp>
#include <caf/fwd.hpp>

namespace tenzir {

class local_control_plane final : public operator_control_plane {
public:
  auto self() noexcept -> exec_node_actor::base& override {
    TENZIR_UNIMPLEMENTED();
  }

  auto node() noexcept -> node_actor override {
    TENZIR_UNIMPLEMENTED();
  }

  auto diagnostics() noexcept -> diagnostic_handler& override {
    struct handler final : public diagnostic_handler {
      handler(local_control_plane& ctrl) : ctrl{ctrl} {
      }
      void emit(diagnostic d) override {
        TENZIR_WARN("got diagnostic: {:?}", d);
        if (d.severity == severity::error) {
          throw std::move(d);
        }
      }
      local_control_plane& ctrl;
    };
    if (not handler_) {
      handler_ = std::make_unique<handler>(*this);
    }
    return *handler_;
  }

  auto metrics(type) noexcept -> metric_handler override {
    TENZIR_UNIMPLEMENTED();
  }

  auto no_location_overrides() const noexcept -> bool override {
    // Location overrides cannot work for the local control plane, as it has no
    // notion of a location.
    return true;
  }

  auto has_terminal() const noexcept -> bool override {
    return false;
  }

  auto set_waiting(bool value) noexcept -> void override {
    (void)value;
    TENZIR_UNIMPLEMENTED();
  }

private:
  caf::error error_{};
  std::unique_ptr<diagnostic_handler> handler_{};
};

auto do_not_optimize(const operator_base& op) -> optimize_result {
  // This default implementation is always correct because it effectively
  // promises `op | where filter | sink <=> op | where filter | sink`, which is
  // trivial. Note that forwarding `order` is not always valid. To see this,
  // assume `op == head` and `order == unordered`. We would have to show that
  // `head | where filter | sink <=> shuffle | head | where filter | sink`, but
  // this is clearly not the case.
  return optimize_result{std::nullopt, event_order::ordered, op.copy()};
}

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

auto pipeline::parse(std::string source, diagnostic_handler& diag)
  -> std::optional<pipeline> {
  auto parsed = tql::parse(std::move(source), diag);
  if (!parsed) {
    return {};
  }
  return tql::to_pipeline(std::move(*parsed));
}

auto pipeline::internal_parse(std::string_view repr)
  -> caf::expected<pipeline> {
  return tql::parse_internal(std::string{repr});
}

auto pipeline::internal_parse_as_operator(std::string_view repr)
  -> caf::expected<operator_ptr> {
  auto result = internal_parse(repr);
  if (not result) {
    return std::move(result.error());
  }
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

auto pipeline::operators() const& -> std::span<const operator_ptr> {
  return operators_;
}

auto pipeline::optimize_if_closed() const -> pipeline {
  if (not is_closed()) {
    return *this;
  }
  auto [filter, pipe] = optimize_into_filter();
  if (filter != trivially_true_expression()) {
    // This could also be an assertion as it always points to an error in the
    // operator implementation, but we try to continue with the original
    // pipeline here.
    TENZIR_ERROR("optimize on closed pipeline `{:?}` returned expression `{}`",
                 *this, filter);
    return *this;
  }
  auto out = pipe.infer_type<void>();
  if (not out) {
    TENZIR_ERROR("closed pipeline was optimized into invalid pipeline: {}",
                 out.error());
    return *this;
  }
  if (not out->is<void>()) {
    TENZIR_ERROR("closed pipeline was optimized into one ending with {}",
                 operator_type_name(*out));
    return *this;
  }
  return std::move(pipe);
}

auto pipeline::optimize_into_filter() const -> std::pair<expression, pipeline> {
  return optimize_into_filter(trivially_true_expression());
}

auto pipeline::optimize_into_filter(const expression& filter) const
  -> std::pair<expression, pipeline> {
  auto opt = optimize(filter, event_order::ordered);
  auto* pipe = dynamic_cast<pipeline*>(opt.replacement.get());
  // We know that `pipeline::optimize` yields a pipeline and a filter.
  TENZIR_ASSERT(pipe);
  TENZIR_ASSERT(opt.filter);
  return {std::move(*opt.filter), std::move(*pipe)};
}

auto pipeline::optimize(expression const& filter, event_order order) const
  -> optimize_result {
  auto current_filter = filter;
  auto current_order = order;
  // Collect the optimized pipeline in reversed order.
  auto result = std::vector<operator_ptr>{};
  for (auto it = operators_.rbegin(); it != operators_.rend(); ++it) {
    TENZIR_ASSERT(*it);
    auto const& op = **it;
    auto opt = op.optimize(current_filter, current_order);
    // TODO: This is a small hack to not propagate a TQLv2 `where` unless the
    // pipeline starts in `export`. By doing this, we make sure that we keep
    // TQLv2 semantics (including warnings), unless performance demands it. This
    // hack will be fixed by upgrading the catalog to the new expressions.
    if (op.name() == "tql2.where") {
      auto qualifies = std::ranges::all_of(it, operators_.rend(), [](auto& op) {
        return op->name() == "tql2.where" || op->name() == "export";
      });
      if (not qualifies) {
        opt = optimize_result::order_invariant(op, current_order);
      }
    }
    if (opt.filter) {
      current_filter = std::move(*opt.filter);
    } else if (current_filter != trivially_true_expression()) {
      // TODO: We just want to create a `where {current}` operator. However,
      // we currently only have the interface for parsing this from a string.
      auto pipe = tql::parse_internal(fmt::format("where {}", current_filter));
      TENZIR_ASSERT(pipe);
      auto ops = std::move(*pipe).unwrap();
      TENZIR_ASSERT(ops.size() == 1);
      result.push_back(std::move(ops[0]));
      current_filter = trivially_true_expression();
    }
    if (opt.replacement) {
      result.push_back(std::move(opt.replacement));
    }
    current_order = opt.order;
  }
  std::reverse(result.begin(), result.end());
  return optimize_result{current_filter, current_order,
                         std::make_unique<pipeline>(std::move(result))};
}

auto pipeline::copy() const -> operator_ptr {
  auto copied = std::make_unique<pipeline>();
  copied->operators_.reserve(operators_.size());
  for (const auto& op : operators_) {
    copied->operators_.push_back(op->copy());
  }
  return copied;
}

auto pipeline::instantiate(operator_input input,
                           operator_control_plane& control) const
  -> caf::expected<operator_output> {
  TENZIR_DEBUG("instantiating '{:?}' for {}", *this, operator_type_name(input));
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

auto operator_base::copy() const -> operator_ptr {
  auto p = plugins::find<operator_serialization_plugin>(name());
  if (not p) {
    TENZIR_ERROR("could not find serialization plugin `{}`", name());
    TENZIR_ASSERT(false);
  }
  auto buffer = caf::byte_buffer{};
  auto f = caf::binary_serializer{nullptr, buffer};
  auto success = p->serialize(f, *this);
  if (not success) {
    TENZIR_ERROR("failed to serialize `{}` operator: {}", name(),
                 f.get_error());
    TENZIR_ASSERT(false);
  }
  auto g = caf::binary_deserializer{nullptr, buffer};
  auto copy = operator_ptr{};
  p->deserialize(g, copy);
  if (not copy) {
    TENZIR_ERROR("failed to deserialize `{}` operator: {}", name(),
                 g.get_error());
    TENZIR_ASSERT(false);
  }
  return copy;
}

auto operator_base::infer_signature() const -> operator_signature {
  const auto void_output = infer_type<void>();
  const auto bytes_output = infer_type<chunk_ptr>();
  const auto events_output = infer_type<table_slice>();
  return {
    .source = static_cast<bool>(void_output),
    .transformation = (bytes_output and not bytes_output->is<void>())
                      or (events_output and not events_output->is<void>()),
    .sink = (void_output and void_output->is<void>())
            or (bytes_output and bytes_output->is<void>())
            or (events_output and events_output->is<void>()),
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
    auto first = &op == &operators_.front();
    if (!first && current.is<void>()) {
      return caf::make_error(ec::type_clash, fmt::format("pipeline continues "
                                                         "with '{}' after sink",
                                                         op->name()));
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
  auto error = std::optional<caf::error>{};
  try {
    auto ctrl = local_control_plane{};
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
      (void)monostate;
      co_yield {};
    }
  } catch (diagnostic& d) {
    error = std::move(d).to_error();
  } catch (std::exception& exc) {
    error = diagnostic::error("unhandled exception: {}", exc.what()).to_error();
  } catch (...) {
    error = diagnostic::error("unhandled exception").to_error();
  }
  if (error) {
    co_yield std::move(*error);
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

auto detail::serialize_op(serializer f, const operator_base& x) -> bool {
  return std::visit(
    [&](auto& f) {
      return plugin_serialize(f.get(), x);
    },
    f);
}

} // namespace tenzir
