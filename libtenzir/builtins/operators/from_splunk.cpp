//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/argument_parser2.hpp"
#include "tenzir/compile_ctx.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/format_utils.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/try.hpp"
#include "tenzir/view3.hpp"

#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

namespace tenzir::plugins::from_splunk {

namespace {

constexpr auto error_field_name = std::string_view{"_from_splunk_error"};

struct SplunkMessage {
  std::string type;
  std::string text;
};

struct Args {
  located<std::string> url;
  ast::expression search;
  ast::expression earliest;
  ast::expression latest;
  located<data> headers;
  Option<located<data>> tls;
  Option<located<duration>> timeout;
  Option<located<duration>> connection_timeout;
  Option<located<uint64_t>> max_retry_count;
  Option<located<duration>> retry_delay;
};

auto make_constant(data value, location source) -> ast::expression {
  auto constant = match(
    std::move(value),
    [](pattern const&) -> ast::constant::kind {
      TENZIR_UNREACHABLE();
    },
    []<class T>(T&& value) -> ast::constant::kind
      requires(not std::same_as<std::decay_t<T>, pattern>)
    {
      return std::forward<T>(value);
    });
  return ast::constant{std::move(constant), source};
}

auto make_named_arg(std::string name, ast::expression value, location source)
  -> ast::expression {
  auto field = ast::field_path::try_from(
    ast::root_field{ast::identifier{std::move(name), source}});
  TENZIR_ASSERT(field);
  return ast::assignment{std::move(field).value().unwrap(), source,
                         std::move(value)};
}

auto append_named_arg(ast::invocation& inv, std::string name, data value,
                      location source) -> void {
  inv.args.push_back(make_named_arg(
    std::move(name), make_constant(std::move(value), source), source));
}

auto check_non_empty_literal(std::string_view name,
                             ast::expression const& expression,
                             diagnostic_handler& dh) -> failure_or<void> {
  auto const* constant = try_as<ast::constant>(expression);
  if (not constant) {
    return {};
  }
  auto const* value = try_as<std::string>(constant->value);
  if (not value) {
    return {};
  }
  return check_non_empty(
    name, located<std::string>{*value, expression.get_location()}, dh);
}

auto extract_messages(record const& envelope) -> std::vector<SplunkMessage> {
  auto result = std::vector<SplunkMessage>{};
  list const* messages = nullptr;
  for (auto const& [name, value] : envelope) {
    if (name == "messages") {
      messages = try_as<list>(value);
      break;
    }
  }
  if (not messages) {
    return result;
  }
  for (auto const& message : *messages) {
    auto const* message_record = try_as<record>(message);
    if (not message_record) {
      continue;
    }
    auto parsed = SplunkMessage{};
    for (auto const& [name, value] : *message_record) {
      auto const* text = try_as<std::string>(value);
      if (not text) {
        continue;
      }
      if (name == "type") {
        parsed.type = *text;
      } else if (name == "text") {
        parsed.text = *text;
      }
    }
    if (not parsed.type.empty() or not parsed.text.empty()) {
      result.push_back(std::move(parsed));
    }
  }
  return result;
}

auto extract_messages(record_view3 envelope) -> std::vector<SplunkMessage> {
  auto result = std::vector<SplunkMessage>{};
  auto messages = Option<list_view3>{};
  for (auto [name, value] : envelope) {
    if (name == "messages") {
      if (auto const* list = try_as<list_view3>(value)) {
        messages = *list;
      }
      break;
    }
  }
  if (not messages) {
    return result;
  }
  for (auto message : *messages) {
    auto const* message_record = try_as<record_view3>(message);
    if (not message_record) {
      continue;
    }
    auto parsed = SplunkMessage{};
    for (auto [name, value] : *message_record) {
      auto const* text = try_as<std::string_view>(value);
      if (not text) {
        continue;
      }
      if (name == "type") {
        parsed.type = *text;
      } else if (name == "text") {
        parsed.text = *text;
      }
    }
    if (not parsed.type.empty() or not parsed.text.empty()) {
      result.push_back(std::move(parsed));
    }
  }
  return result;
}

auto response_body_text(blob_view body) -> std::string {
  auto const* data = reinterpret_cast<char const*>(body.data());
  auto text = std::string_view{data, body.size()};
  if (auto parsed = from_json(text)) {
    if (auto const* envelope = try_as<record>(*parsed)) {
      auto messages = extract_messages(*envelope);
      if (not messages.empty()) {
        auto result = std::string{};
        for (auto const& message : messages) {
          if (not result.empty()) {
            result += "; ";
          }
          if (not message.type.empty()) {
            result += message.type;
            result += ": ";
          }
          result += message.text;
        }
        return result;
      }
    }
  }
  constexpr auto max_body_size = size_t{2048};
  text = detail::trim(text);
  if (text.size() <= max_body_size) {
    return std::string{text};
  }
  return fmt::format("{}...", text.substr(0, max_body_size));
}

class ValidateSplunkResponse final : public Operator<table_slice, table_slice> {
public:
  ValidateSplunkResponse(std::string search, location search_source)
    : search_{std::move(search)}, search_source_{search_source} {
  }

  auto start(OpCtx&) -> Task<void> override {
    TENZIR_DEBUG("from_splunk: submitting streaming search without pagination: "
                 "{}",
                 search_);
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    ++response_batches_;
    TENZIR_TRACE("from_splunk: received response batch with {} records",
                 input.rows());
    auto failed = false;
    for (auto row : values3(input)) {
      for (auto [name, value] : row) {
        if (name == "result" and not is<caf::none_t>(value)) {
          ++results_received_;
        }
        if (name != error_field_name) {
          continue;
        }
        auto const* body = try_as<blob_view>(value);
        auto message = body ? response_body_text(*body)
                            : std::string{"unexpected HTTP error response"};
        if (message.empty()) {
          message = "HTTP request failed without a response body";
        }
        diagnostic::error("Splunk rejected the search: {}", message)
          .primary(search_source_)
          .note("search: {}", search_)
          .emit(ctx);
        co_return;
      }
      for (auto const& message : extract_messages(row)) {
        auto text = message.text.empty() ? std::string{"no details provided"}
                                         : message.text;
        if (detail::ascii_icase_equal(message.type, "ERROR")
            or detail::ascii_icase_equal(message.type, "FATAL")) {
          diagnostic::error("Splunk search failed: {}", text)
            .primary(search_source_)
            .note("search: {}", search_)
            .emit(ctx);
          failed = true;
          continue;
        }
        auto type
          = message.type.empty() ? std::string{"message"} : message.type;
        diagnostic::warning("Splunk search returned {}: {}", type, text)
          .primary(search_source_)
          .note("search: {}", search_)
          .emit(ctx);
      }
    }
    if (failed) {
      co_return;
    }
    co_await push(std::move(input));
  }

  auto finalize(Push<table_slice>&, OpCtx&) -> Task<FinalizeBehavior> override {
    TENZIR_DEBUG("from_splunk: completed streaming search with {} results in "
                 "{} response "
                 "batches",
                 results_received_, response_batches_);
    co_return FinalizeBehavior::done;
  }

private:
  std::string search_;
  location search_source_;
  uint64_t results_received_ = 0;
  uint64_t response_batches_ = 0;
};

class ValidateSplunkResponseIr final : public ir::Operator {
public:
  ValidateSplunkResponseIr() = default;

  ValidateSplunkResponseIr(ast::expression search, location search_source)
    : search_{std::move(search)}, search_source_{search_source} {
  }

  auto name() const -> std::string override {
    return "validate_splunk_response_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TRY(search_.substitute(ctx));
    if (instantiate) {
      TRY(auto value, const_eval(search_, ctx));
      auto* search = try_as<std::string>(value);
      if (not search) {
        diagnostic::error("expected `string` for `search`")
          .primary(search_)
          .emit(ctx);
        return failure::promise();
      }
      search_value_ = std::move(*search);
    }
    return {};
  }

  auto spawn(element_type_tag input) && -> Option<AnyOperator> override {
    TENZIR_ASSERT(input.is<table_slice>());
    TENZIR_ASSERT(search_value_);
    return ValidateSplunkResponse{std::move(*search_value_), search_source_}
      .with_name("from_splunk");
  }

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    if (input.is_not<table_slice>()) {
      diagnostic::error("operator expects events")
        .primary(search_source_)
        .emit(dh);
      return failure::promise();
    }
    return tag_v<table_slice>;
  }

  auto main_location() const -> location override {
    return search_source_;
  }

  friend auto inspect(auto& f, ValidateSplunkResponseIr& x) -> bool {
    return f.object(x).fields(f.field("search", x.search_),
                              f.field("search_value", x.search_value_),
                              f.field("search_source", x.search_source_));
  }

private:
  ast::expression search_;
  Option<std::string> search_value_;
  location search_source_;
};

using validate_splunk_response_ir_plugin
  = inspection_plugin<ir::Operator, ValidateSplunkResponseIr>;

template <class T>
auto append_optional_named_arg(ast::invocation& inv, std::string name,
                               Option<located<T>> value) -> void {
  if (value) {
    append_named_arg(inv, std::move(name), data{std::move(value->inner)},
                     value->source);
  }
}

class Plugin final : public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "from_splunk";
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::CompileResult> override {
    for (auto& arg : inv.args) {
      TRY(arg.bind(ctx));
    }
    auto args = Args{};
    auto provider = session_provider::make(ctx);
    auto session = provider.as_session();
    TRY(argument_parser2::operator_(name())
          .positional("url", args.url)
          .named("search", args.search, "string")
          .named("earliest", args.earliest, "string|time")
          .named("latest", args.latest, "string|time")
          .named("headers", args.headers, "record")
          .named("tls", args.tls)
          .named("timeout", args.timeout)
          .named("connection_timeout", args.connection_timeout)
          .named("max_retry_count", args.max_retry_count)
          .named("retry_delay", args.retry_delay)
          .parse(operator_factory_invocation{std::move(inv.op),
                                             std::move(inv.args)},
                 session));
    TRY(check_non_empty("url", args.url, ctx));
    auto const search_source = args.search.get_location();
    auto const earliest_source = args.earliest.get_location();
    auto const latest_source = args.latest.get_location();
    TRY(check_non_empty_literal("search", args.search, ctx));
    TRY(check_non_empty_literal("earliest", args.earliest, ctx));
    TRY(check_non_empty_literal("latest", args.latest, ctx));
    auto& headers = as<record>(args.headers.inner);
    auto has_authorization = false;
    for (auto const& [name, _] : headers) {
      if (detail::ascii_icase_equal(name, "authorization")) {
        has_authorization = true;
        break;
      }
    }
    if (not has_authorization) {
      diagnostic::error("`headers` must contain an `Authorization` header")
        .primary(args.headers.source)
        .emit(ctx);
      return failure::promise();
    }
    auto endpoint = std::move(args.url.inner);
    if (endpoint.ends_with('/')) {
      endpoint.pop_back();
    }
    endpoint += "/services/search/v2/jobs/export";
    auto const* from_http
      = plugins::find<operator_compiler_plugin>("from_http");
    TENZIR_ASSERT(from_http);
    auto replacement = invocation_for_plugin(*from_http, args.url.source);
    replacement.args.push_back(
      make_constant(data{std::move(endpoint)}, args.url.source));
    append_named_arg(replacement, "method", data{std::string{"post"}},
                     args.url.source);
    auto search_for_diagnostic = args.search;
    auto body = std::vector<ast::record::item>{};
    body.emplace_back(ast::record::field{
      ast::identifier{"search", search_source}, std::move(args.search)});
    body.emplace_back(
      ast::record::field{ast::identifier{"earliest_time", earliest_source},
                         std::move(args.earliest)});
    body.emplace_back(ast::record::field{
      ast::identifier{"latest_time", latest_source}, std::move(args.latest)});
    body.emplace_back(ast::record::field{
      ast::identifier{"output_mode", search_source},
      make_constant(data{std::string{"json"}}, search_source)});
    body.emplace_back(ast::record::field{
      ast::identifier{"preview", search_source},
      make_constant(data{std::string{"false"}}, search_source)});
    replacement.args.push_back(make_named_arg(
      "body", ast::record{search_source, std::move(body), search_source},
      search_source));
    append_named_arg(replacement, "headers", std::move(args.headers.inner),
                     args.headers.source);
    replacement.args.push_back(
      make_named_arg("error_field",
                     ast::root_field{ast::identifier{
                       std::string{error_field_name}, search_source}},
                     search_source));
    append_optional_named_arg(replacement, "tls", std::move(args.tls));
    append_optional_named_arg(replacement, "timeout", std::move(args.timeout));
    append_optional_named_arg(replacement, "connection_timeout",
                              std::move(args.connection_timeout));
    append_optional_named_arg(replacement, "max_retry_count",
                              std::move(args.max_retry_count));
    append_optional_named_arg(replacement, "retry_delay",
                              std::move(args.retry_delay));
    append_named_arg(replacement, "encode", data{std::string{"form"}},
                     search_source);
    TRY(auto parser, parse_pipeline_with_location_override(
                       "read_json", search_source, session));
    TRY(resolve_entities(parser, session));
    replacement.args.emplace_back(
      ast::pipeline_expr{search_source, std::move(parser), search_source});
    TRY(auto result, from_http->compile(std::move(replacement), ctx));
    auto pipeline = std::move(result).unwrap();
    pipeline.operators.push_back(ValidateSplunkResponseIr{
      std::move(search_for_diagnostic), search_source});
    TRY(auto extract_result,
        parse_pipeline_with_location_override(
          "where result? != null | this = result", search_source, session));
    TRY(resolve_entities(extract_result, session));
    TRY(auto extract_result_ir, std::move(extract_result).compile(ctx));
    for (auto& let : extract_result_ir.lets) {
      pipeline.lets.push_back(std::move(let));
    }
    for (auto& op : extract_result_ir.operators) {
      pipeline.operators.push_back(std::move(op));
    }
    return pipeline;
  }
};

} // namespace

} // namespace tenzir::plugins::from_splunk

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_splunk::Plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::plugins::from_splunk::validate_splunk_response_ir_plugin)
