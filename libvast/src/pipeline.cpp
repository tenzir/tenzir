//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/concept/parseable/vast/pipeline.hpp"

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

  auto warn(caf::error) noexcept -> void override {
    die("not implemented");
  }

  auto emit(table_slice) noexcept -> void override {
    die("not implemented");
  }

  auto demand(type = {}) const noexcept -> size_t override {
    die("not implemented");
  }

  auto schemas() const noexcept -> const std::vector<type>& override {
    die("not implemented");
  }

  auto concepts() const noexcept -> const concepts_map& override {
    die("not implemented");
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
  auto ops = std::vector<operator_ptr>{};
  // plugin name parser
  using parsers::alnum, parsers::chr, parsers::space,
    parsers::optional_ws_or_comment;
  const auto plugin_name_char_parser = alnum | chr{'-'};
  const auto plugin_name_parser
    = optional_ws_or_comment >> +plugin_name_char_parser;
  // TODO: allow more empty string
  while (!repr.empty()) {
    // 1. parse a single word as operator plugin name
    const auto* f = repr.begin();
    const auto* const l = repr.end();
    auto plugin_name = std::string{};
    if (!plugin_name_parser(f, l, plugin_name)) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("failed to parse pipeline '{}': "
                                         "operator name is invalid",
                                         repr));
    }
    // 2. find plugin using operator name
    const auto* plugin = plugins::find<operator_plugin>(plugin_name);
    if (!plugin) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("failed to parse pipeline '{}': "
                                         "operator '{}' does not exist",
                                         repr, plugin_name));
    }
    // 3. ask the plugin to parse itself from the remainder
    auto [remaining_repr, op] = plugin->make_operator(std::string_view{f, l});
    if (!op)
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to parse pipeline '{}': {}",
                                         repr, op.error()));
    ops.push_back(std::move(*op));
    repr = remaining_repr;
  }
  return pipeline{std::move(ops)};
}

auto pipeline::copy() const -> operator_ptr {
  auto copied = std::make_unique<pipeline>();
  copied->operators_.reserve(operators_.size());
  for (const auto& op : operators_) {
    copied->operators_.push_back(op->copy());
  }
  return copied;
};

auto pipeline::to_string() const -> std::string {
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
    (void)monostate;
    if (auto error = ctrl.get_error()) {
      co_yield std::move(error);
      co_return;
    }
  }
  if (auto error = ctrl.get_error()) {
    co_yield std::move(error);
  }
}

} // namespace vast
