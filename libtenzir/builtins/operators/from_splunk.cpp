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
#include "tenzir/plugin.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/resolve.hpp"
#include "tenzir/try.hpp"

#include <string>
#include <type_traits>
#include <utility>

namespace tenzir::plugins::from_splunk {

namespace {

struct Args {
  located<std::string> url;
  located<std::string> search;
  located<std::string> earliest;
  located<std::string> latest;
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
          .named("search", args.search)
          .named("earliest", args.earliest)
          .named("latest", args.latest)
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
    TRY(check_non_empty("search", args.search, ctx));
    TRY(check_non_empty("earliest", args.earliest, ctx));
    TRY(check_non_empty("latest", args.latest, ctx));
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
    auto body = record{};
    body.emplace("search", std::move(args.search.inner));
    body.emplace("earliest_time", std::move(args.earliest.inner));
    body.emplace("latest_time", std::move(args.latest.inner));
    body.emplace("output_mode", std::string{"json"});
    body.emplace("preview", std::string{"false"});
    append_named_arg(replacement, "body", data{std::move(body)},
                     args.search.source);
    append_named_arg(replacement, "headers", std::move(args.headers.inner),
                     args.headers.source);
    append_optional_named_arg(replacement, "tls", std::move(args.tls));
    append_optional_named_arg(replacement, "timeout", std::move(args.timeout));
    append_optional_named_arg(replacement, "connection_timeout",
                              std::move(args.connection_timeout));
    append_optional_named_arg(replacement, "max_retry_count",
                              std::move(args.max_retry_count));
    append_optional_named_arg(replacement, "retry_delay",
                              std::move(args.retry_delay));
    append_named_arg(replacement, "encode", data{std::string{"form"}},
                     args.search.source);
    TRY(auto parser, parse_pipeline_with_location_override(
                       "read_json | where result? != null | this = result",
                       args.search.source, session));
    TRY(resolve_entities(parser, session));
    replacement.args.emplace_back(ast::pipeline_expr{
      args.search.source, std::move(parser), args.search.source});
    return from_http->compile(std::move(replacement), ctx);
  }
};

} // namespace

} // namespace tenzir::plugins::from_splunk

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_splunk::Plugin)
