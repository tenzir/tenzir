//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/fwd.hpp"

#include "tenzir/async.hpp"
#include "tenzir/async/notify.hpp"
#include "tenzir/concept/printable/tenzir/json.hpp"
#include "tenzir/curl.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/flat_map.hpp"
#include "tenzir/detail/string.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/error.hpp"
#include "tenzir/http.hpp"
#include "tenzir/operator_plugin.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/series_builder.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/plugin.hpp"
#include "tenzir/tql2/set.hpp"

#include <boost/url/parse.hpp>
#include <folly/coro/Sleep.h>

#include <deque>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>

namespace tenzir::plugins {
namespace {

using namespace std::literals;

auto to_chrono(duration d) -> std::chrono::milliseconds {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d);
}

auto field_paths_overlap(ast::field_path const& lhs, ast::field_path const& rhs)
  -> bool {
  auto lhs_path = lhs.path();
  auto rhs_path = rhs.path();
  auto i = size_t{};
  for (; i < lhs_path.size() and i < rhs_path.size(); ++i) {
    if (lhs_path[i].id.name != rhs_path[i].id.name) {
      return false;
    }
  }
  return i == lhs_path.size() or i == rhs_path.size();
}

struct FromHttpArgs {
  location op = location::unknown;
  located<secret> url;
  std::optional<located<std::string>> method;
  std::optional<located<data>> body;
  std::optional<located<std::string>> encode;
  std::optional<located<record>> headers;
  std::optional<ast::field_path> metadata_field;
  std::optional<ast::field_path> error_field;
  std::optional<located<std::string>> paginate;
  located<duration> paginate_delay{0s, location::unknown};
  located<duration> connection_timeout{5s, location::unknown};
  located<uint64_t> max_retry_count{0, location::unknown};
  located<duration> retry_delay{1s, location::unknown};
  std::optional<located<data>> tls;
  std::optional<located<ir::pipeline>> parse;
  let_id response_let;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (metadata_field and error_field
        and field_paths_overlap(*metadata_field, *error_field)) {
      diagnostic::error("`error_field` and `metadata_field` must not "
                        "point to same or nested field")
        .primary(*error_field)
        .primary(*metadata_field)
        .emit(dh);
      return failure::promise();
    }
    if (headers) {
      for (auto const& [_, v] : headers->inner) {
        if (not is<std::string>(v) and not is<secret>(v)) {
          diagnostic::error("header values must be of type `string`")
            .primary(*headers)
            .emit(dh);
          return failure::promise();
        }
      }
    }
    if (body) {
      TRY(match(
        body->inner,
        [](concepts::one_of<blob, std::string, record> auto const&)
          -> failure_or<void> {
          return {};
        },
        [&](auto const&) -> failure_or<void> {
          diagnostic::error("`body` must be `blob`, `record` or `string`")
            .primary(body->source)
            .emit(dh);
          return failure::promise();
        }));
    }
    if (encode) {
      if (not body) {
        diagnostic::error("encoding specified without a `body`")
          .primary(encode->source)
          .emit(dh);
        return failure::promise();
      }
      if (encode->inner != "json" and encode->inner != "form") {
        diagnostic::error("unsupported encoding: `{}`", encode->inner)
          .primary(encode->source)
          .hint("must be `json` or `form`")
          .emit(dh);
        return failure::promise();
      }
    }
    if (method and method->inner.empty()) {
      diagnostic::error("`method` must not be empty").primary(*method).emit(dh);
      return failure::promise();
    }
    if (not make_method()) {
      diagnostic::error("invalid http method: `{}`", method->inner)
        .primary(*method)
        .emit(dh);
      return failure::promise();
    }
    if (retry_delay.inner < duration::zero()) {
      diagnostic::error("`retry_delay` must be a positive duration")
        .primary(retry_delay)
        .emit(dh);
      return failure::promise();
    }
    if (paginate_delay.inner < duration::zero()) {
      diagnostic::error("`paginate_delay` must be a positive duration")
        .primary(paginate_delay)
        .emit(dh);
      return failure::promise();
    }
    if (connection_timeout.inner < duration::zero()) {
      diagnostic::error("`connection_timeout` must be a positive duration")
        .primary(connection_timeout)
        .emit(dh);
      return failure::promise();
    }
    if (paginate and paginate->inner != "link") {
      diagnostic::error("unsupported pagination mode: `{}`", paginate->inner)
        .primary(paginate->source)
        .hint("`paginate` must be `\"link\"`")
        .emit(dh);
      return failure::promise();
    }
    if (parse) {
      auto output = parse->inner.infer_type(tag_v<chunk_ptr>, dh);
      if (not output) {
        return failure::promise();
      }
      if (*output and not(*output)->is_any<void, table_slice>()) {
        diagnostic::error("pipeline must return events or be a sink")
          .primary(*parse)
          .emit(dh);
        return failure::promise();
      }
    }
    auto tls_opts = tls ? tls_options{*tls, {.tls_default = false}}
                        : tls_options{{.tls_default = false}};
    TRY(tls_opts.validate(dh));
    return {};
  }

  auto make_method() const -> std::optional<std::string> {
    auto method_name = std::string{};
    if (not method) {
      method_name = body ? "post" : "get";
    } else {
      method_name = method->inner;
    }
    return http::normalize_http_method(method_name);
  }

  auto make_headers() const
    -> std::pair<std::unordered_map<std::string, std::string>,
                 detail::stable_map<std::string, secret>> {
    auto hdrs = std::unordered_map<std::string, std::string>{};
    auto secrets = detail::stable_map<std::string, secret>{};
    auto insert_accept_header = true;
    auto insert_content_type = body and is<record>(body->inner);
    if (headers) {
      for (auto const& [k, v] : headers->inner) {
        if (detail::ascii_icase_equal(k, "accept")) {
          insert_accept_header = false;
        }
        if (detail::ascii_icase_equal(k, "content-type")) {
          insert_content_type = false;
        }
        match(
          v,
          [&](std::string const& x) {
            hdrs.emplace(k, x);
          },
          [&](secret const& x) {
            secrets.emplace(k, x);
          },
          [](auto const&) {
            TENZIR_UNREACHABLE();
          });
      }
    }
    if (insert_content_type) {
      hdrs.emplace("Content-Type", encode and encode->inner == "form"
                                     ? "application/x-www-form-urlencoded"
                                     : "application/json");
    }
    if (insert_accept_header) {
      hdrs.emplace("Accept", "application/json, */*;q=0.5");
    }
    return std::pair{hdrs, secrets};
  }
};

struct ExecutorHttpRequest {
  std::string url;
  std::unordered_map<std::string, std::string> headers;
  bool is_pagination = false;
};

auto queue_executor_request(
  std::deque<ExecutorHttpRequest>& queue,
  std::unordered_map<std::string, std::string> const& headers,
  std::string next_url, bool tls_enabled, location const& op,
  diagnostic_handler& dh, severity diag_severity, std::string_view note = {})
  -> bool {
  http::normalize_http_url(next_url, tls_enabled);
  auto parsed = boost::urls::parse_uri_reference(next_url);
  if (not parsed) {
    if (diag_severity == severity::warning) {
      if (note.empty()) {
        diagnostic::warning("failed to parse uri: {}", next_url)
          .primary(op)
          .emit(dh);
      } else {
        diagnostic::warning("failed to parse uri: {}", next_url)
          .primary(op)
          .note("{}", note)
          .emit(dh);
      }
    } else {
      if (note.empty()) {
        diagnostic::error("failed to parse uri: {}", next_url)
          .primary(op)
          .emit(dh);
      } else {
        diagnostic::error("failed to parse uri: {}", next_url)
          .primary(op)
          .note("{}", note)
          .emit(dh);
      }
    }
    return false;
  }
  queue.push_back({std::move(next_url), headers, true});
  return true;
}

struct FromHttpTaskEvent {
  std::optional<ExecutorHttpRequest> request;
  std::optional<http::HttpResult<http::ResponseData>> response;
};

class FromHttp final : public Operator<void, table_slice> {
public:
  explicit FromHttp(FromHttpArgs args)
    : args_{std::move(args)}, response_let_id_{args_.response_let} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto& dh = ctx.dh();
    if (args_.metadata_field and args_.error_field
        and field_paths_overlap(*args_.metadata_field, *args_.error_field)) {
      diagnostic::error("`error_field` and `metadata_field` must not "
                        "point to same or nested field")
        .primary(*args_.error_field)
        .primary(*args_.metadata_field)
        .emit(dh);
      done_ = true;
      co_return;
    }
    auto requests = std::vector<secret_request>{};
    auto [headers, secrets] = args_.make_headers();
    requests.emplace_back(
      make_secret_request("url", args_.url, resolved_url_, dh));
    if (not secrets.empty()) {
      auto const& loc = args_.headers->source;
      for (auto& [name, secret] : secrets) {
        auto request = secret_request{
          std::move(secret),
          loc,
          [&, name](resolved_secret_value const& value) -> failure_or<void> {
            TRY(auto str, value.utf8_view(name, loc, dh));
            headers.emplace(name, std::string{str});
            return {};
          },
        };
        requests.emplace_back(std::move(request));
      }
    }
    if (not requests.empty()) {
      auto resolved = co_await ctx.resolve_secrets(std::move(requests));
      if (not resolved) {
        done_ = true;
        co_return;
      }
    }
    if (resolved_url_.empty()) {
      diagnostic::error("`url` must not be empty").primary(args_.url).emit(dh);
      done_ = true;
      co_return;
    }
    auto const tls_default = http::infer_tls_default(resolved_url_);
    tls_options_ = args_.tls
                     ? tls_options{*args_.tls, {.tls_default = tls_default}}
                     : tls_options{{.tls_default = tls_default}};
    auto validate_tls
      = tls_options_.validate(resolved_url_, args_.url.source, dh);
    if (not validate_tls) {
      done_ = true;
      co_return;
    }
    auto ssl_result = tls_options_.make_folly_ssl_context(dh);
    if (not ssl_result) {
      done_ = true;
      co_return;
    }
    ssl_context_ = std::move(*ssl_result);
    tls_enabled_ = tls_options_.get_tls(nullptr).inner;
    http::normalize_http_url(resolved_url_, tls_enabled_);
    pending_.push_back({resolved_url_, std::move(headers), false});
    if (args_.body) {
      match(
        args_.body->inner,
        [&](blob const& x) {
          request_body_.append(reinterpret_cast<char const*>(x.data()),
                               x.size());
        },
        [&](std::string const& x) {
          request_body_ = x;
        },
        [&](record const& x) {
          if (args_.encode and args_.encode->inner == "form") {
            request_body_ = curl::escape(flatten(x));
            return;
          }
          auto printer = json_printer{{}};
          auto it = std::back_inserter(request_body_);
          printer.print(it, x);
        },
        [](auto const&) {
          TENZIR_UNREACHABLE();
        });
    }
  }

  auto await_task(diagnostic_handler&) const -> Task<Any> override {
    if (done_) {
      co_await wait_forever();
      TENZIR_UNREACHABLE();
    }
    if (not active_sub_ and not pending_.empty()) {
      auto request = std::move(pending_.front());
      pending_.pop_front();
      if (request.is_pagination
          and args_.paginate_delay.inner > duration::zero()) {
        co_await folly::coro::sleep(
          std::chrono::duration_cast<folly::HighResDuration>(
            to_chrono(args_.paginate_delay.inner)));
      }
      auto result = co_await perform_request(request);
      co_return FromHttpTaskEvent{
        std::optional<ExecutorHttpRequest>{std::move(request)},
        std::optional<http::HttpResult<http::ResponseData>>{std::move(result)},
      };
    }
    co_await notify_->wait();
    co_return FromHttpTaskEvent{};
  }

  auto process_task(Any result, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    auto event = std::move(result).as<FromHttpTaskEvent>();
    if (not event.response) {
      if (not active_sub_ and pending_.empty()) {
        done_ = true;
      }
      co_return;
    }
    TENZIR_ASSERT(event.request);
    auto request = std::move(*event.request);
    auto response_result = std::move(*event.response);
    if (response_result.is_err()) {
      diagnostic::error("{}", std::move(response_result).unwrap_err())
        .primary(args_.op)
        .emit(ctx);
      if (pending_.empty() and not active_sub_) {
        done_ = true;
      }
      co_return;
    }
    auto response = std::move(response_result).unwrap();
    auto response_metadata = http::make_response_record(response);
    if (auto const code = response.status_code; code < 200 or code > 399) {
      if (not args_.error_field) {
        diagnostic::error("received erroneous http status code: `{}`", code)
          .primary(args_.op)
          .hint("specify `error_field` to keep the event")
          .emit(ctx);
      } else {
        auto sb = series_builder{};
        std::ignore = sb.record();
        auto error = series_builder{};
        error.data(response.body);
        auto slice = assign(*args_.error_field, error.finish_assert_one_array(),
                            sb.finish_assert_one_slice(), ctx.dh());
        if (args_.metadata_field) {
          auto metadata = series_builder{};
          metadata.data(response_metadata);
          slice
            = assign(*args_.metadata_field, metadata.finish_assert_one_array(),
                     std::move(slice), ctx.dh());
        }
        co_await push(std::move(slice));
      }
      if (pending_.empty() and not active_sub_) {
        done_ = true;
      }
      co_return;
    }
    if (args_.paginate and args_.paginate->inner == "link") {
      auto paginate_source = args_.paginate
                               ? std::optional<location>{args_.paginate->source}
                               : std::nullopt;
      if (auto url = http::next_url_from_link_headers(
            response, request.url, paginate_source, ctx.dh())) {
        std::ignore
          = queue_executor_request(pending_, request.headers, std::move(*url),
                                   tls_enabled_, args_.op, ctx.dh(),
                                   severity::error);
      }
    }
    if (response.body.empty()) {
      if (pending_.empty() and not active_sub_) {
        done_ = true;
      } else {
        notify_->notify_one();
      }
      co_return;
    }
    if (not args_.parse) {
      diagnostic::error(
        "`from_http` in the new executor requires an explicit parsing pipeline")
        .primary(args_.op)
        .emit(ctx);
      if (pending_.empty() and not active_sub_) {
        done_ = true;
      }
      co_return;
    }
    auto response_body = std::move(response.body);
    auto chunk_data
      = std::span<std::byte const>{response_body.data(), response_body.size()};
    auto encoding
      = http::find_header_value(response.headers, "content-encoding");
    auto payload = chunk_ptr{};
    if (auto decompressed
        = http::try_decompress_body(encoding, chunk_data, ctx.dh())) {
      payload = chunk::make(std::move(*decompressed));
    } else {
      payload = chunk::make(std::move(response_body));
    }
    auto pipeline = args_.parse->inner;
    auto env = substitute_ctx::env_t{};
    env[response_let_id_] = response_metadata;
    auto reg = global_registry();
    auto b_ctx = base_ctx{ctx, *reg};
    auto sub_result = pipeline.substitute(substitute_ctx{b_ctx, &env}, true);
    if (not sub_result) {
      if (pending_.empty() and not active_sub_) {
        done_ = true;
      }
      notify_->notify_one();
      co_return;
    }
    auto sub_key = next_sub_key_++;
    active_sub_ = ActiveSubContext{
      .key = sub_key,
      .response_metadata = std::move(response_metadata),
    };
    auto sub = co_await ctx.spawn_sub(data{sub_key}, std::move(pipeline),
                                      tag_v<chunk_ptr>);
    auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);
    auto push_result = co_await open_pipeline.push(std::move(payload));
    if (push_result.is_err()) {
      active_sub_ = std::nullopt;
      done_ = true;
      co_return;
    }
    co_await open_pipeline.close();
  }

  auto process_sub(SubKeyView key, table_slice slice, Push<table_slice>& push,
                   OpCtx& ctx) -> Task<void> override {
    if (not active_sub_) {
      co_return;
    }
    if (materialize(key) != data{active_sub_->key}) {
      co_return;
    }
    if (slice.rows() == 0) {
      co_return;
    }
    if (args_.metadata_field) {
      auto metadata = series_builder{};
      for (auto i = size_t{}; i < slice.rows(); ++i) {
        metadata.data(active_sub_->response_metadata);
      }
      slice = assign(*args_.metadata_field, metadata.finish_assert_one_array(),
                     std::move(slice), ctx.dh());
    }
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    if (not active_sub_) {
      co_return;
    }
    if (materialize(key) != data{active_sub_->key}) {
      co_return;
    }
    active_sub_ = std::nullopt;
    if (pending_.empty()) {
      done_ = true;
    } else {
      notify_->notify_one();
    }
    co_return;
  }

  auto state() -> OperatorState override {
    return done_ ? OperatorState::done : OperatorState::unspecified;
  }

private:
  struct ActiveSubContext {
    uint64_t key = 0;
    record response_metadata;
  };

  auto perform_request(ExecutorHttpRequest const& request) const
    -> Task<http::HttpResult<http::ResponseData>> {
    auto request_method = args_.make_method();
    TENZIR_ASSERT(request_method);
    auto config = http::ClientRequestConfig{
      .url = request.url,
      .method = *request_method,
      .body = request_body_,
      .headers = request.headers,
      .connect_timeout = to_chrono(args_.connection_timeout.inner),
      .ssl_context = ssl_context_,
    };
    co_return co_await http::send_request_with_retries(
      std::move(config), args_.max_retry_count.inner,
      to_chrono(args_.retry_delay.inner));
  }

  FromHttpArgs args_;
  mutable std::string resolved_url_;
  mutable std::string request_body_;
  mutable std::shared_ptr<folly::SSLContext> ssl_context_;
  mutable tls_options tls_options_{{.tls_default = false}};
  mutable std::deque<ExecutorHttpRequest> pending_;
  mutable std::optional<ActiveSubContext> active_sub_;
  let_id response_let_id_;
  uint64_t next_sub_key_ = 0;
  mutable std::shared_ptr<Notify> notify_ = std::make_shared<Notify>();
  mutable bool tls_enabled_ = false;
  bool done_ = false;
};

struct HttpExecutorArgs {
  location op = location::unknown;
  ast::expression url;
  std::optional<ast::expression> method;
  std::optional<ast::expression> body;
  std::optional<ast::expression> headers;
  std::optional<located<std::string>> encode;
  std::optional<ast::field_path> metadata_field;
  std::optional<ast::field_path> error_field;
  std::optional<located<std::string>> paginate;
  located<duration> paginate_delay{0s, location::unknown};
  std::optional<located<data>> tls;
  located<duration> connection_timeout{5s, location::unknown};
  located<uint64_t> max_retry_count{0, location::unknown};
  located<duration> retry_delay{1s, location::unknown};
  located<ir::pipeline> parse;
  let_id request_let;
  let_id response_let;

  auto validate(diagnostic_handler& dh) const -> failure_or<void> {
    if (metadata_field and error_field
        and field_paths_overlap(*metadata_field, *error_field)) {
      diagnostic::error("`error_field` and `metadata_field` must not "
                        "point to same or nested field")
        .primary(*error_field)
        .primary(*metadata_field)
        .emit(dh);
      return failure::promise();
    }
    if (encode) {
      if (encode->inner != "json" and encode->inner != "form") {
        diagnostic::error("unsupported encoding: `{}`", encode->inner)
          .primary(encode->source)
          .hint("must be `json` or `form`")
          .emit(dh);
        return failure::promise();
      }
    }
    if (retry_delay.inner < duration::zero()) {
      diagnostic::error("`retry_delay` must be a positive duration")
        .primary(retry_delay)
        .emit(dh);
      return failure::promise();
    }
    if (paginate_delay.inner < duration::zero()) {
      diagnostic::error("`paginate_delay` must be a positive duration")
        .primary(paginate_delay)
        .emit(dh);
      return failure::promise();
    }
    if (connection_timeout.inner < duration::zero()) {
      diagnostic::error("`connection_timeout` must be a positive duration")
        .primary(connection_timeout)
        .emit(dh);
      return failure::promise();
    }
    if (paginate and paginate->inner != "link") {
      diagnostic::error("unsupported pagination mode: `{}`", paginate->inner)
        .primary(paginate->source)
        .hint("`paginate` must be `\"link\"`")
        .emit(dh);
      return failure::promise();
    }
    auto output = parse.inner.infer_type(tag_v<chunk_ptr>, dh);
    if (not output) {
      return failure::promise();
    }
    if (*output and not(*output)->is_any<void, table_slice>()) {
      diagnostic::error("pipeline must return events or be a sink")
        .primary(parse)
        .emit(dh);
      return failure::promise();
    }
    auto tls_opts = tls ? tls_options{*tls, {.tls_default = false}}
                        : tls_options{{.tls_default = false}};
    TRY(tls_opts.validate(dh));
    return {};
  }

  auto make_method(std::string_view method_name) const
    -> std::optional<std::string> {
    if (method_name.empty()) {
      if (not method and body) {
        return std::string{"POST"};
      }
      return std::string{"GET"};
    }
    return http::normalize_http_method(method_name);
  }
};

class HttpExecutorOperator final : public Operator<table_slice, table_slice> {
public:
  explicit HttpExecutorOperator(HttpExecutorArgs args)
    : args_{std::move(args)},
      request_let_id_{args_.request_let},
      response_let_id_{args_.response_let} {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    if (args_.metadata_field and args_.error_field
        and field_paths_overlap(*args_.metadata_field, *args_.error_field)) {
      diagnostic::error("`error_field` and `metadata_field` must not "
                        "point to same or nested field")
        .primary(*args_.error_field)
        .primary(*args_.metadata_field)
        .emit(ctx);
      aborted_ = true;
    }
    co_return;
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    if (aborted_) {
      co_return;
    }
    if (input.rows() == 0) {
      co_return;
    }
    auto& dh = ctx.dh();
    auto urls = std::vector<std::string>{};
    urls.reserve(input.rows());
    auto requests = std::vector<secret_request>{};
    auto url_warned = false;
    auto url_ms = eval(args_.url, input, dh);
    for (auto const& part : url_ms.parts()) {
      if (part.type.kind().is<string_type>()) {
        for (auto value : part.values<string_type>()) {
          if (value) {
            urls.emplace_back(*value);
          } else {
            url_warned = true;
            urls.emplace_back();
          }
        }
        continue;
      }
      if (part.type.kind().is<secret_type>()) {
        for (auto const& value : part.values<secret_type>()) {
          if (value) {
            requests.emplace_back(make_secret_request(
              "url", materialize(*value), args_.url.get_location(),
              urls.emplace_back(), dh));
          } else {
            url_warned = true;
            urls.emplace_back();
          }
        }
        continue;
      }
      diagnostic::warning("expected `string`, got `{}`", part.type.kind())
        .primary(args_.url)
        .note("skipping request")
        .emit(dh);
      urls.insert(urls.end(), part.length(), {});
    }
    if (url_warned) {
      diagnostic::warning("`url` must not be null")
        .primary(args_.url)
        .note("skipping request")
        .emit(dh);
    }
    auto headers = std::vector<HeaderEvaluation>{};
    if (not co_await evaluate_headers(input, requests, headers, ctx)) {
      aborted_ = true;
      co_return;
    }
    if (not requests.empty()) {
      auto resolved = co_await ctx.resolve_secrets(std::move(requests));
      if (not resolved) {
        aborted_ = true;
        co_return;
      }
    }
    TENZIR_ASSERT(urls.size() == input.rows());
    TENZIR_ASSERT(headers.size() == input.rows());
    auto methods = eval_optional_string(args_.method, input, dh);
    auto bodies = eval_body(input, dh);
    auto row_index = size_t{};
    for (auto row : input.values()) {
      auto request_record = materialize(row);
      auto url = std::move(urls[row_index]);
      auto row_headers = std::move(headers[row_index]);
      auto method_name = methods.next().value();
      auto [body_view, insert_content_type] = bodies.next().value();
      auto body = std::string{body_view};
      ++row_index;
      if (url.empty()) {
        diagnostic::warning("`url` must not be empty")
          .primary(args_.url)
          .note("skipping request")
          .emit(dh);
        continue;
      }
      auto method = args_.make_method(method_name);
      if (not method) {
        auto const method_location = args_.method ? args_.method->get_location()
                                                  : args_.url.get_location();
        diagnostic::warning("invalid http method: `{}`", method_name)
          .primary(method_location)
          .note("skipping request")
          .emit(dh);
        continue;
      }
      if (insert_content_type and not row_headers.has_content_type) {
        row_headers.values.emplace(
          "Content-Type", args_.encode and args_.encode->inner == "form"
                            ? "application/x-www-form-urlencoded"
                            : "application/json");
      }
      if (not row_headers.has_accept_header) {
        row_headers.values.emplace("Accept", "application/json, */*;q=0.5");
      }
      auto tls_default = http::infer_tls_default(url);
      auto tls_opts = args_.tls
                        ? tls_options{*args_.tls, {.tls_default = tls_default}}
                        : tls_options{{.tls_default = tls_default}};
      auto tls_enabled = tls_opts.get_tls(nullptr).inner;
      http::normalize_http_url(url, tls_enabled);
      if (auto valid = tls_opts.validate(url, args_.url.get_location(), dh);
          not valid) {
        continue;
      }
      auto ssl_result = tls_opts.make_folly_ssl_context(dh);
      if (not ssl_result) {
        continue;
      }
      auto ssl_context = std::move(*ssl_result);
      auto pending = std::deque<ExecutorHttpRequest>{
        ExecutorHttpRequest{
          .url = std::move(url),
          .headers = std::move(row_headers.values),
          .is_pagination = false,
        },
      };
      while (not pending.empty()) {
        auto request = std::move(pending.front());
        pending.pop_front();
        if (request.is_pagination
            and args_.paginate_delay.inner > duration::zero()) {
          co_await folly::coro::sleep(
            std::chrono::duration_cast<folly::HighResDuration>(
              to_chrono(args_.paginate_delay.inner)));
        }
        auto response_result
          = co_await perform_request(request, *method, body, ssl_context);
        if (response_result.is_err()) {
          diagnostic::warning("{}", std::move(response_result).unwrap_err())
            .primary(args_.op)
            .emit(dh);
          break;
        }
        auto response = std::move(response_result).unwrap();
        auto response_metadata = http::make_response_record(response);
        if (auto const code = response.status_code; code < 200 or code > 399) {
          if (not args_.error_field) {
            diagnostic::warning("received erroneous http status code: `{}`",
                                code)
              .primary(args_.op)
              .note("skipping response handling")
              .hint("specify `error_field` to keep the event")
              .emit(dh);
            break;
          }
          auto base = series_builder{};
          base.data(request_record);
          auto error = series_builder{};
          error.data(response.body);
          auto slice
            = assign(*args_.error_field, error.finish_assert_one_array(),
                     base.finish_assert_one_slice(), dh);
          if (args_.metadata_field) {
            auto metadata = series_builder{};
            metadata.data(response_metadata);
            slice = assign(*args_.metadata_field,
                           metadata.finish_assert_one_array(), std::move(slice),
                           dh);
          }
          co_await push(std::move(slice));
          break;
        }
        if (args_.paginate and args_.paginate->inner == "link") {
          auto paginate_source
            = std::optional<location>{args_.paginate->source};
          if (auto next_url = http::next_url_from_link_headers(
                response, request.url, paginate_source, dh)) {
            std::ignore = queue_executor_request(
              pending, request.headers, std::move(*next_url), tls_enabled,
              args_.op, dh, severity::warning, "skipping request");
          }
        }
        if (response.body.empty()) {
          continue;
        }
        auto response_body = std::move(response.body);
        auto response_span = std::span<std::byte const>{response_body.data(),
                                                        response_body.size()};
        auto encoding
          = http::find_header_value(response.headers, "content-encoding");
        auto payload = chunk_ptr{};
        if (auto decompressed
            = http::try_decompress_body(encoding, response_span, dh)) {
          payload = chunk::make(std::move(*decompressed));
        } else {
          payload = chunk::make(std::move(response_body));
        }
        auto pipeline = args_.parse.inner;
        auto env = substitute_ctx::env_t{};
        env[request_let_id_] = request_record;
        env[response_let_id_] = response_metadata;
        auto reg = global_registry();
        auto b_ctx = base_ctx{ctx, *reg};
        auto sub_result
          = pipeline.substitute(substitute_ctx{b_ctx, &env}, true);
        if (not sub_result) {
          continue;
        }
        auto sub_key = data{next_sub_key_++};
        active_sub_keys_.insert(sub_key);
        if (args_.metadata_field) {
          response_metadata_by_sub_key_.emplace(sub_key,
                                                std::move(response_metadata));
        }
        auto sub = co_await ctx.spawn_sub(sub_key, std::move(pipeline),
                                          tag_v<chunk_ptr>);
        auto open_pipeline = as<OpenPipeline<chunk_ptr>>(sub);
        auto push_result = co_await open_pipeline.push(std::move(payload));
        if (push_result.is_err()) {
          active_sub_keys_.erase(sub_key);
          response_metadata_by_sub_key_.erase(sub_key);
          continue;
        }
        co_await open_pipeline.close();
      }
    }
  }

  auto process_sub(SubKeyView key, table_slice slice, Push<table_slice>& push,
                   OpCtx& ctx) -> Task<void> override {
    auto sub_key = materialize(key);
    if (active_sub_keys_.find(sub_key) == active_sub_keys_.end()) {
      co_return;
    }
    if (slice.rows() == 0) {
      co_return;
    }
    if (args_.metadata_field) {
      auto metadata_it = response_metadata_by_sub_key_.find(sub_key);
      TENZIR_ASSERT(metadata_it != response_metadata_by_sub_key_.end());
      auto metadata = series_builder{};
      for (auto i = size_t{}; i < slice.rows(); ++i) {
        metadata.data(metadata_it->second);
      }
      slice = assign(*args_.metadata_field, metadata.finish_assert_one_array(),
                     std::move(slice), ctx.dh());
    }
    co_await push(std::move(slice));
  }

  auto finish_sub(SubKeyView key, Push<table_slice>&, OpCtx&)
    -> Task<void> override {
    auto sub_key = materialize(key);
    active_sub_keys_.erase(sub_key);
    response_metadata_by_sub_key_.erase(sub_key);
    co_return;
  }

  auto finalize(Push<table_slice>&, OpCtx&) -> Task<void> override {
    upstream_done_ = true;
    co_return;
  }

  auto state() -> OperatorState override {
    if ((aborted_ or upstream_done_) and active_sub_keys_.empty()) {
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

private:
  struct HeaderEvaluation {
    std::unordered_map<std::string, std::string> values;
    bool has_content_type = false;
    bool has_accept_header = false;
  };

  auto
  perform_request(ExecutorHttpRequest const& request, std::string_view method,
                  std::string const& body,
                  std::shared_ptr<folly::SSLContext> const& ssl_context) const
    -> Task<http::HttpResult<http::ResponseData>> {
    auto config = http::ClientRequestConfig{
      .url = request.url,
      .method = std::string{method},
      .body = body,
      .headers = request.headers,
      .connect_timeout = to_chrono(args_.connection_timeout.inner),
      .ssl_context = ssl_context,
    };
    co_return co_await http::send_request_with_retries(
      std::move(config), args_.max_retry_count.inner,
      to_chrono(args_.retry_delay.inner));
  }

  auto evaluate_headers(table_slice const& input,
                        std::vector<secret_request>& requests,
                        std::vector<HeaderEvaluation>& headers, OpCtx& ctx)
    -> Task<bool> {
    auto& dh = ctx.dh();
    headers.clear();
    headers.reserve(input.rows());
    if (not args_.headers) {
      headers.resize(input.rows());
      co_return true;
    }
    auto header_warned = false;
    auto location = args_.headers->get_location();
    auto header_ms = eval(*args_.headers, input, dh);
    for (auto const& part : header_ms.parts()) {
      if (part.type.kind().is_not<record_type>()) {
        headers.insert(headers.end(), part.length(), HeaderEvaluation{});
        diagnostic::warning("expected `record`, got `{}`", part.type.kind())
          .primary(*args_.headers)
          .note("skipping headers")
          .emit(dh);
        continue;
      }
      for (auto const& value : part.values<record_type>()) {
        auto& row_headers = headers.emplace_back();
        if (not value) {
          diagnostic::warning("expected `record`, got `null`")
            .primary(*args_.headers)
            .note("skipping headers")
            .emit(dh);
          continue;
        }
        for (auto const& [name, header_value] : *value) {
          row_headers.has_content_type
            |= args_.body.has_value()
               and detail::ascii_icase_equal(name, "content-type");
          row_headers.has_accept_header
            |= detail::ascii_icase_equal(name, "accept");
          match(
            header_value,
            [&](std::string_view const x) {
              row_headers.values.emplace(name, x);
            },
            [&](secret_view const x) {
              auto key = std::string{name};
              requests.emplace_back(make_secret_request(
                key, materialize(x), location, row_headers.values[key], dh));
            },
            [&](auto const&) {
              if (not header_warned) {
                header_warned = true;
                diagnostic::warning(
                  "`headers` must be `{{ string: string|secret }}`")
                  .primary(*args_.headers)
                  .note("skipping invalid header values")
                  .emit(dh);
              }
            });
        }
      }
    }
    co_return true;
  }

  auto eval_body(table_slice const& slice, diagnostic_handler& dh) const
    -> generator<std::pair<std::string_view, bool>> {
    if (not args_.body) {
      for (auto i = size_t{}; i < slice.rows(); ++i) {
        co_yield {};
      }
      co_return;
    }
    auto ms = eval(args_.body.value(), slice, dh);
    for (auto const& part : ms.parts()) {
      if (part.type.kind().is<null_type>()) {
        for (auto i = int64_t{}; i < part.length(); ++i) {
          co_yield {};
        }
        continue;
      }
      if (part.type.kind().is<blob_type>()) {
        for (auto value : part.values<blob_type>()) {
          if (not value) {
            co_yield {};
            continue;
          }
          co_yield {
            {reinterpret_cast<char const*>(value->data()), value->size()},
            false,
          };
        }
        continue;
      }
      if (part.type.kind().is<string_type>()) {
        for (auto value : part.values<string_type>()) {
          if (not value) {
            co_yield {};
            continue;
          }
          co_yield {value.value(), false};
        }
        continue;
      }
      if (part.type.kind().is<record_type>()) {
        auto buffer = std::string{};
        auto const form = args_.encode and args_.encode->inner == "form";
        for (auto value : part.values<record_type>()) {
          if (not value) {
            co_yield {};
            continue;
          }
          if (form) {
            co_yield {curl::escape(flatten(materialize(value.value()))), true};
            continue;
          }
          auto printer = json_printer{{}};
          auto it = std::back_inserter(buffer);
          printer.print(it, value.value());
          co_yield {buffer, true};
          buffer.clear();
        }
        continue;
      }
      diagnostic::warning("expected `blob`, `record` or `string`, got `{}`",
                          part.type.kind())
        .primary(args_.body.value())
        .emit(dh);
      for (auto i = int64_t{}; i < part.length(); ++i) {
        co_yield {};
      }
    }
  }

  static auto
  eval_optional_string(std::optional<ast::expression> const& expr,
                       table_slice const& slice, diagnostic_handler& dh)
    -> generator<std::string_view> {
    if (not expr) {
      for (auto i = size_t{}; i < slice.rows(); ++i) {
        co_yield {};
      }
      co_return;
    }
    auto ms = eval(*expr, slice, dh);
    for (auto const& part : ms.parts()) {
      if (part.type.kind().is<null_type>()) {
        for (auto i = int64_t{}; i < part.length(); ++i) {
          co_yield {};
        }
        continue;
      }
      if (part.type.kind().is<string_type>()) {
        for (auto value : part.values<string_type>()) {
          co_yield value.value_or("");
        }
        continue;
      }
      diagnostic::warning("expected `string`, got `{}`", part.type.kind())
        .primary(*expr)
        .emit(dh);
      for (auto i = int64_t{}; i < part.length(); ++i) {
        co_yield {};
      }
    }
  }

  HttpExecutorArgs args_;
  let_id request_let_id_;
  let_id response_let_id_;
  std::unordered_set<data> active_sub_keys_;
  std::unordered_map<data, record> response_metadata_by_sub_key_;
  uint64_t next_sub_key_ = 0;
  bool upstream_done_ = false;
  bool aborted_ = false;
};

auto make_http_executor_description() -> Description {
  auto d = Describer<HttpExecutorArgs, HttpExecutorOperator>{};
  d.operator_location(&HttpExecutorArgs::op);
  auto url = d.positional("url", &HttpExecutorArgs::url, "string");
  auto method = d.named("method", &HttpExecutorArgs::method, "string");
  auto body = d.named("body", &HttpExecutorArgs::body, "record|string|blob");
  auto headers = d.named("headers", &HttpExecutorArgs::headers, "record");
  auto encode = d.named("encode", &HttpExecutorArgs::encode);
  auto metadata_field
    = d.named("metadata_field", &HttpExecutorArgs::metadata_field);
  auto error_field = d.named("error_field", &HttpExecutorArgs::error_field);
  auto paginate = d.named("paginate", &HttpExecutorArgs::paginate);
  auto paginate_delay
    = d.named_optional("paginate_delay", &HttpExecutorArgs::paginate_delay);
  auto tls = d.named("tls", &HttpExecutorArgs::tls);
  auto connection_timeout = d.named_optional(
    "connection_timeout", &HttpExecutorArgs::connection_timeout);
  auto max_retry_count
    = d.named_optional("max_retry_count", &HttpExecutorArgs::max_retry_count);
  auto retry_delay
    = d.named_optional("retry_delay", &HttpExecutorArgs::retry_delay);
  auto parse = d.pipeline(&HttpExecutorArgs::parse,
                          {{"request", &HttpExecutorArgs::request_let},
                           {"response", &HttpExecutorArgs::response_let}});
  d.validate([=](ValidateCtx& ctx) -> Empty {
    if (ctx.get(encode) and not ctx.get_location(body)) {
      diagnostic::error("encoding specified without a `body`")
        .primary(ctx.get_location(encode).value())
        .emit(ctx);
    }
    auto args = HttpExecutorArgs{};
    args.op = ctx.get_location(url).value_or(location::unknown);
    if (auto x = ctx.get(url)) {
      args.url = *x;
    }
    if (auto x = ctx.get(method)) {
      args.method = *x;
    }
    if (auto x = ctx.get(body)) {
      args.body = *x;
    }
    if (auto x = ctx.get(headers)) {
      args.headers = *x;
    }
    if (auto x = ctx.get(encode)) {
      args.encode = *x;
    }
    if (auto x = ctx.get(metadata_field)) {
      args.metadata_field = *x;
    }
    if (auto x = ctx.get(error_field)) {
      args.error_field = *x;
    }
    if (auto x = ctx.get(paginate)) {
      args.paginate = *x;
    }
    if (auto x = ctx.get(paginate_delay)) {
      args.paginate_delay = *x;
    }
    if (auto x = ctx.get(tls)) {
      args.tls = *x;
    }
    if (auto x = ctx.get(connection_timeout)) {
      args.connection_timeout = *x;
    }
    if (auto x = ctx.get(max_retry_count)) {
      args.max_retry_count = *x;
    }
    if (auto x = ctx.get(retry_delay)) {
      args.retry_delay = *x;
    }
    if (auto x = ctx.get(parse)) {
      args.parse = *x;
    }
    std::ignore = args.validate(ctx);
    return {};
  });
  return d.without_optimize();
}

} // namespace

struct FromHttpPlugin final : public virtual OperatorPlugin {
  auto name() const -> std::string override {
    return "tql2.from_http";
  }

  auto describe() const -> Description override {
    auto d = Describer<FromHttpArgs, FromHttp>{};
    d.operator_location(&FromHttpArgs::op);
    auto url = d.positional("url", &FromHttpArgs::url);
    auto method = d.named("method", &FromHttpArgs::method);
    auto body = d.named("body", &FromHttpArgs::body);
    auto encode = d.named("encode", &FromHttpArgs::encode);
    auto headers = d.named("headers", &FromHttpArgs::headers);
    auto metadata_field
      = d.named("metadata_field", &FromHttpArgs::metadata_field);
    auto error_field = d.named("error_field", &FromHttpArgs::error_field);
    auto paginate = d.named("paginate", &FromHttpArgs::paginate);
    auto paginate_delay
      = d.named_optional("paginate_delay", &FromHttpArgs::paginate_delay);
    auto connection_timeout = d.named_optional(
      "connection_timeout", &FromHttpArgs::connection_timeout);
    auto max_retry_count
      = d.named_optional("max_retry_count", &FromHttpArgs::max_retry_count);
    auto retry_delay
      = d.named_optional("retry_delay", &FromHttpArgs::retry_delay);
    auto tls = d.named("tls", &FromHttpArgs::tls);
    auto parse = d.pipeline(&FromHttpArgs::parse,
                            {{"response", &FromHttpArgs::response_let}});
    d.validate([=](ValidateCtx& ctx) -> Empty {
      auto args = FromHttpArgs{};
      args.op = ctx.get_location(url).value_or(location::unknown);
      if (auto x = ctx.get(url)) {
        args.url = *x;
      }
      if (auto x = ctx.get(method)) {
        args.method = *x;
      }
      if (auto x = ctx.get(body)) {
        args.body = *x;
      }
      if (auto x = ctx.get(encode)) {
        args.encode = *x;
      }
      if (auto x = ctx.get(headers)) {
        args.headers = *x;
      }
      if (auto x = ctx.get(metadata_field)) {
        args.metadata_field = *x;
      }
      if (auto x = ctx.get(error_field)) {
        args.error_field = *x;
      }
      if (auto x = ctx.get(paginate)) {
        args.paginate = *x;
      }
      if (auto x = ctx.get(paginate_delay)) {
        args.paginate_delay = *x;
      }
      if (auto x = ctx.get(connection_timeout)) {
        args.connection_timeout = *x;
      }
      if (auto x = ctx.get(max_retry_count)) {
        args.max_retry_count = *x;
      }
      if (auto x = ctx.get(retry_delay)) {
        args.retry_delay = *x;
      }
      if (auto x = ctx.get(tls)) {
        args.tls = *x;
      }
      if (auto x = ctx.get(parse)) {
        args.parse = *x;
      }
      std::ignore = args.validate(ctx);
      return {};
    });
    return d.without_optimize();
  }
};

auto describe_http_executor() -> Description {
  return make_http_executor_description();
}

} // namespace tenzir::plugins

TENZIR_REGISTER_PLUGIN(tenzir::plugins::FromHttpPlugin)
