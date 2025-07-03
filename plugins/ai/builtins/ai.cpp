//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/secret_resolution.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/generator.hpp>
#include <tenzir/operator_control_plane.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>
#include <tenzir/tql2/ast.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/set.hpp>
#include <tenzir/view3.hpp>

#include <ai/anthropic.h>
#include <ai/openai.h>
#include <ai/types/client.h>
#include <ai/types/generate_options.h>
#include <arrow/array/builder_binary.h>

namespace tenzir::plugins::ai {

namespace {

constexpr auto default_prompt = "You are a helpful intelligent assistant.";

constexpr auto models = std::array{
  "gpt-4o",
  "gpt-4o-mini",
  "gpt-4-turbo",
  "gpt-3.5-turbo",
  "gpt-4",
  "claude-3-5-sonnet-20241022",
  "claude-3-5-haiku-20241022",
  "claude-3-opus-20240229",
  "claude-3-sonnet-20240229",
  "claude-3-haiku-20240307",
};

struct ai_args {
  location op;
  located<std::string> user_prompt;
  located<std::string> system_prompt{default_prompt, location::unknown};
  located<std::string> model{"gpt-4o", location::unknown};
  located<secret> api_key;
  ast::field_path response_field;

  auto add_to(argument_parser2& p) -> void {
    p.positional("user_prompt", user_prompt);
    p.named("api_key", api_key);
    p.named("response_field", response_field);
    p.named_optional("system", system_prompt);
    p.named_optional("model", model);
  }

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (user_prompt.inner.empty()) {
      diagnostic::error("`prompt` must not be empty")
        .primary(user_prompt)
        .emit(dh);
      return failure::promise();
    }
    if (model.inner.empty()) {
      diagnostic::error("`model` must not be empty").primary(model).emit(dh);
      return failure::promise();
    }
    if (not std::ranges::contains(models, model.inner)) {
      diagnostic::error("unknown `model`: `{}`", model.inner)
        .primary(model)
        .hint("supported models: ", fmt::join(models, ", "))
        .emit(dh);
      return failure::promise();
    }
    return {};
  }

  friend auto inspect(auto& f, ai_args& x) -> bool {
    return f.object(x).fields(f.field("op", x.op),
                              f.field("user_prompt", x.user_prompt),
                              f.field("system_prompt", x.system_prompt),
                              f.field("model", x.model),
                              f.field("api_key", x.api_key),
                              f.field("response_field", x.response_field));
  }
};

class ai_operator final : public crtp_operator<ai_operator> {
public:
  ai_operator() = default;

  ai_operator(ai_args args) : args_(std::move(args)) {
  }

  auto operator()(generator<table_slice> x, operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto& dh = ctrl.diagnostics();
    auto api_key = std::string{};
    co_yield ctrl.resolve_secrets_must_yield({
      make_secret_request("api_key", args_.api_key, api_key, dh),
    });
    if (api_key.empty()) {
      diagnostic::error("`api_key` must not be empty")
        .primary(args_.api_key)
        .emit(dh);
      co_return;
    }
    auto client = [&] {
      if (args_.model.inner.starts_with("gpt")) {
        return ::ai::openai::create_client(api_key);
      }
      return ::ai::anthropic::create_client(api_key);
    }();
    const auto printer = json_printer{{}};
    for (const auto& slice : x) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      auto b = arrow::StringBuilder{};
      for (auto event : values3(slice)) {
        auto buf = args_.user_prompt.inner + '\n';
        auto it = std::back_inserter(buf);
        TENZIR_ASSERT(printer.print(it, event));
        const auto result = client.generate_text({
          args_.model.inner,
          args_.system_prompt.inner,
          std::move(buf),
        });
        if (not result.is_success()) {
          diagnostic::warning("failed to fetch response: ",
                              result.error_message())
            .primary(args_.op)
            .emit(dh);
          check(b.AppendNull());
          continue;
        }
        check(b.Append(result.text));
      }
      auto s = series{string_type{}, finish(b)};
      auto out = assign(args_.response_field, std::move(s), slice, dh);
      co_yield std::move(out);
    }
  }

  auto name() const -> std::string override {
    return "ai";
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, ai_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  ai_args args_;
};

class ai final : public virtual operator_plugin2<ai_operator> {
public:
  auto name() const -> std::string override {
    return "tql2.ai";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = ai_args{};
    args.op = inv.self.get_location();
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    TRY(p.parse(inv, ctx));
    TRY(args.validate(ctx));
    return std::make_unique<ai_operator>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::ai

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ai::ai)
