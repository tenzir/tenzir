//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/ai.hpp>
#include <tenzir/ai/openai.hpp>
#include <tenzir/async.hpp>
#include <tenzir/data.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/table_slice.hpp>

#include <fmt/format.h>

#include <chrono>
#include <cmath>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::ai_prompt {
namespace {

constexpr auto default_endpoint = std::string_view{"http://127.0.0.1:11434/v1"};

struct PromptArgs {
  located<std::string> model;
  Option<located<secret>> endpoint;
  Option<located<std::string>> system;
  Option<ast::expression> data;
  ast::field_path into = ai::default_into("prompt");
  Option<located<secret>> api_key;
  Option<located<double>> temperature;
  Option<located<uint64_t>> max_tokens;
  located<duration> timeout{std::chrono::seconds{30}, location::unknown};
  located<uint64_t> concurrency{1, location::unknown};
  Option<located<tenzir::data>> tls;
  location operator_location = location::unknown;
};

/// The request and outcome for one event.
struct Row {
  Option<std::string> input = None{};
  Option<ai::openai::ResponsesResult> response = None{};
  Option<std::string> error = None{};
};

auto connect(PromptArgs const& args, OpCtx& ctx)
  -> Task<Option<Box<ai::openai::ResponsesClient>>> {
  auto endpoint = args.endpoint
                    ? *args.endpoint
                    : located<secret>{secret::make_literal(default_endpoint),
                                      args.operator_location};
  auto connection
    = co_await ai::connect(std::move(endpoint), args.api_key, "responses",
                           args.timeout.inner, args.tls, ctx);
  if (not connection) {
    co_return None{};
  }
  co_return Box<ai::openai::ResponsesClient>{
    std::in_place, std::move(connection->pool), std::move(connection->headers)};
}

/// Sends one request per row with an input.
auto prompt_all(ai::openai::ResponsesClient& client, PromptArgs const& args,
                std::vector<Row>& rows) -> Task<void> {
  co_await ai::request_all(
    rows.size(), args.concurrency.inner, [&](size_t i) -> Task<void> {
      auto& row = rows[i];
      if (not row.input) {
        co_return;
      }
      auto response = co_await client.create(ai::openai::ResponsesRequest{
        .model = args.model.inner,
        .instructions = args.system.map([](auto const& x) {
          return x.inner;
        }),
        .input = std::move(*row.input),
        .temperature = args.temperature.map([](auto const& x) {
          return x.inner;
        }),
        .max_output_tokens = args.max_tokens.map([](auto const& x) {
          return x.inner;
        }),
      });
      if (response.is_err()) {
        row.error = std::move(response).unwrap_err();
        co_return;
      }
      row.response = std::move(response).unwrap();
    });
}

auto emit_warnings(std::vector<Row> const& rows, PromptArgs const& args,
                   diagnostic_handler& dh) -> void {
  for (auto const& row : rows) {
    if (not row.error) {
      continue;
    }
    auto diag = ai::request_failed(*row.error, args.operator_location);
    if (not args.endpoint) {
      diag = std::move(diag)
               .note("endpoint: {}/responses", default_endpoint)
               .hint("the default endpoint targets Ollama; check that "
                     "Ollama is running and the model name is valid, or "
                     "set `endpoint=...`");
    }
    std::move(diag).emit(dh);
  }
}

/// Appends the result record of one response. Works with the builders of both
/// event representations.
template <class Record>
auto append_response(Record row, ai::openai::ResponsesResult const& response)
  -> void {
  row.field("text").data(std::string_view{response.text});
  ai::set_optional(row.field("model"), response.model);
  if (not response.usage) {
    row.field("usage").null();
  } else {
    auto usage = row.field("usage").record();
    ai::set_optional(usage.field("input_tokens"), response.usage->input_tokens);
    ai::set_optional(usage.field("output_tokens"),
                     response.usage->output_tokens);
    ai::set_optional(usage.field("total_tokens"), response.usage->total_tokens);
  }
  row.field("latency").data(response.latency);
}

class Prompt final : public Operator<table_slice, table_slice> {
public:
  explicit Prompt(PromptArgs args) : args_{std::move(args)} {
  }

  Prompt(Prompt const&) = delete;
  auto operator=(Prompt const&) -> Prompt& = delete;
  Prompt(Prompt&&) noexcept = default;
  auto operator=(Prompt&&) noexcept -> Prompt& = default;

  auto start(OpCtx& ctx) -> Task<void> override {
    client_ = co_await connect(args_, ctx);
    done_ = not client_;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (done_) {
      co_return;
    }
    TENZIR_ASSERT(client_);
    auto rows = std::vector<Row>{};
    for (auto& json : ai::print_inputs(
           args_.data, args_.into, args_.operator_location, input, ctx.dh())) {
      auto& row = rows.emplace_back();
      if (json) {
        row.input = std::move(json);
      } else {
        row.error = std::string{"failed to serialize data as JSON"};
      }
    }
    co_await prompt_all(**client_, args_, rows);
    emit_warnings(rows, args_, ctx.dh());
    auto results = series_builder{};
    for (auto const& row : rows) {
      if (row.response) {
        append_response(results.record(), *row.response);
      } else {
        results.null();
      }
    }
    for (auto& output :
         ai::assign_results(input, args_.into, std::move(results), ctx.dh())) {
      co_await push(std::move(output));
    }
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::normal;
  }

private:
  PromptArgs args_;
  Option<Box<ai::openai::ResponsesClient>> client_ = None{};
  bool done_ = false;
};

class PromptNova final : public Operator<nova::Events, nova::Events> {
public:
  explicit PromptNova(PromptArgs args) : args_{std::move(args)} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto inputs = co_await ai::InputPrinter::make(args_.data, args_.into, ctx);
    if (not inputs) {
      co_return;
    }
    inputs_.emplace(std::move(*inputs));
    client_ = co_await connect(args_, ctx);
  }

  auto process(nova::Events input, Push<nova::Events>& push, OpCtx& ctx)
    -> Task<void> override {
    if (not client_ or not inputs_ or not input.mask.any()) {
      co_return;
    }
    auto rows = std::vector<Row>{};
    for (auto& json : inputs_->print(input, ctx.dh())) {
      rows.push_back(Row{.input = std::move(json)});
    }
    co_await prompt_all(**client_, args_, rows);
    emit_warnings(rows, args_, ctx.dh());
    auto results = ai::build_results(
      input, [&](nova::ArrayBuilder<nova::Data>& builder, size_t i) {
        if (rows[i].response) {
          append_response(builder.record(), *rows[i].response);
        } else {
          builder.null();
        }
      });
    ai::assign_results(input, args_.into, std::move(results), ctx.dh());
    co_await push(std::move(input));
  }

private:
  PromptArgs args_;
  Option<ai::InputPrinter> inputs_ = None{};
  Option<Box<ai::openai::ResponsesClient>> client_ = None{};
};

class plugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "ai_prompt";
  }

  auto describe() const -> Description override {
    auto d = Describer<PromptArgs, Prompt, PromptNova>{};
    auto model = d.named("model", &PromptArgs::model);
    d.named("endpoint", &PromptArgs::endpoint, "string");
    d.named("system", &PromptArgs::system);
    d.named("data", &PromptArgs::data, "any");
    d.named_optional("into", &PromptArgs::into);
    d.named("api_key", &PromptArgs::api_key, "string");
    auto temperature = d.named("temperature", &PromptArgs::temperature);
    d.named("max_tokens", &PromptArgs::max_tokens);
    auto timeout = d.named_optional("timeout", &PromptArgs::timeout);
    auto concurrency
      = d.named_optional("concurrency", &PromptArgs::concurrency);
    d.named("tls", &PromptArgs::tls, "record");
    d.operator_location(&PromptArgs::operator_location);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      ai::check_arguments(ctx.get(model), ctx.get(concurrency),
                          ctx.get(timeout), ctx);
      if (auto value = ctx.get(temperature);
          value
          and (not std::isfinite(value->inner) or value->inner < 0.0
               or value->inner > 2.0)) {
        diagnostic::error("`temperature` must be between 0 and 2")
          .primary(*value)
          .emit(ctx);
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::ai_prompt

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ai_prompt::plugin)
