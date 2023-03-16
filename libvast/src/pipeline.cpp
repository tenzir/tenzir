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

auto pipeline::parse(std::string_view repr) -> caf::expected<pipeline> {
  auto ops = std::vector<transformer_ptr>{};
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
    const auto* plugin = plugins::find<transformer_plugin>(plugin_name);
    if (!plugin) {
      return caf::make_error(ec::syntax_error,
                             fmt::format("failed to parse pipeline '{}': "
                                         "operator '{}' does not exist",
                                         repr, plugin_name));
    }
    // 3. ask the plugin to parse itself from the remainder
    auto [remaining_repr, op]
      = plugin->make_transformer(std::string_view{f, l});
    if (!op)
      return caf::make_error(ec::unspecified,
                             fmt::format("failed to parse pipeline '{}': {}",
                                         repr, op.error()));
    ops.push_back(std::move(*op));
    repr = remaining_repr;
  }
  return pipeline{std::move(ops)};
}

class local_transformer_control final : public transformer_control {
public:
  void abort(caf::error error) override {
    VAST_ASSERT(error);
    error_ = error;
  }

  caf::error get_error() const {
    return error_;
  }

private:
  caf::error error_{};
};

auto pipeline::instantiate(dynamic_input input,
                           transformer_control& control) const
  -> caf::expected<dynamic_output> {
  if (transformers_.empty()) {
    auto f = detail::overload{
      [](std::monostate) -> dynamic_output {
        return generator<std::monostate>{};
      },
      []<class Input>(generator<Input> input) -> dynamic_output {
        return input;
      },
    };
    return std::visit(f, std::move(input));
  }
  auto it = transformers_.begin();
  auto end = transformers_.end();
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
      [](generator<std::monostate>) -> dynamic_input {
        return std::monostate{};
      },
      []<class Output>(generator<Output> output) -> dynamic_input {
        return output;
      },
    };
    input = std::visit(f, std::move(*output));
    if (std::holds_alternative<std::monostate>(input)) {
      return caf::make_error(ec::type_clash, "pipeline ended before all "
                                             "transformers were used");
    }
  }
}

auto make_local_executor(pipeline p) -> generator<caf::error> {
  local_transformer_control control;
  auto dynamic_gen = p.instantiate(std::monostate{}, control);
  if (!dynamic_gen) {
    co_yield dynamic_gen.error();
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
    if (auto error = control.get_error()) {
      co_yield error;
      co_return;
    }
  }
}

} // namespace vast
