//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/async/notify.hpp>
#include <tenzir/box.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/http.hpp>
#include <tenzir/http_pool.hpp>
#include <tenzir/operator_plugin.hpp>
#include <tenzir/pipeline_metrics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/secret_resolution.hpp>
#include <tenzir/tls_options.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/try.hpp>

#include <arrow/util/compression.h>
#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <fmt/core.h>

#include <map>

namespace tenzir::plugins::opensearch2 {
namespace {

struct ToOpenSearchArgs {
  located<secret> url;
  ast::expression action;
  Option<ast::expression> index;
  Option<ast::expression> doc;
  Option<ast::expression> id;
  Option<located<secret>> user;
  Option<located<secret>> passwd;
  Option<located<data>> tls;
  Option<location> include_nulls;
  located<uint64_t> max_content_length{5'000'000, location::unknown};
  located<duration> buffer_timeout{std::chrono::seconds{5}, location::unknown};
  Option<location> compress;
  location operator_location = location::unknown;
};

class json_builder {
public:
  json_builder(json_printer_options printer_opts, uint64_t max_size,
               bool compress)
    : printer_{printer_opts}, max_size_{max_size} {
    if (compress) {
      auto codec = arrow::util::Codec::Create(
        arrow::Compression::type::GZIP,
        arrow::util::Codec::UseDefaultCompressionLevel());
      TENZIR_ASSERT(codec.ok());
      codec_ = codec.MoveValueUnsafe();
    }
  }

  enum class state {
    ok,
    full,
    event_too_large,
  };

  auto create_metadata(std::string_view action,
                       std::optional<std::optional<std::string_view>> idx,
                       std::optional<std::optional<std::string_view>> id,
                       const ToOpenSearchArgs& args)
    -> std::optional<diagnostic> {
    constexpr auto supported_actions
      = std::array{"create", "delete", "index", "update", "upsert"};
    const auto valid_action
      = std::ranges::find(supported_actions, action) != end(supported_actions);
    if (not valid_action) {
      return diagnostic::warning("unsupported action `{}`", action)
        .primary(args.action)
        .note("skipping event")
        .hint("supported actions: {}", fmt::join(supported_actions, ","))
        .done();
    }
    const auto has_idx = idx and idx.value() and not idx.value()->empty();
    const auto has_id = id and id.value() and not id.value()->empty();
    const auto needs_id = action == "delete" or action == "update";
    if (needs_id and not has_id) {
      return diagnostic::warning("action `{}` requires `id`, but got `null`",
                                 action)
        .primary(args.action)
        .note("skipping event")
        .done();
    }
    element_text_ += "{";
    auto it = std::back_inserter(element_text_);
    printer_.print(it, action == "upsert" ? "update" : action);
    element_text_ += ":{";
    if (has_idx) {
      element_text_ += "\"_index\":";
      printer_.print(it, idx.value().value());
    }
    if (has_id) {
      if (has_idx) {
        element_text_ += ',';
      }
      element_text_ += "\"_id\":";
      printer_.print(it, id.value().value());
    }
    element_text_ += "}}\n";
    return std::nullopt;
  }

  auto create_doc(std::string_view action, view3<record> doc) {
    if (action == "delete") {
      return;
    }
    if (action == "update" or action == "upsert") {
      element_text_ += R"({"doc":)";
    }
    auto it = std::back_inserter(element_text_);
    printer_.print(it, doc);
    if (action == "update") {
      element_text_ += "}";
    } else if (action == "upsert") {
      element_text_ += R"(,"doc_as_upsert":true})";
    }
    element_text_ += '\n';
  }

  auto finish_event() -> state {
    last_element_size_ = std::ssize(element_text_);
    if (last_element_size_ > max_size_) {
      element_text_.clear();
      return state::event_too_large;
    }
    if (std::ssize(body_) + last_element_size_ <= max_size_) {
      if (body_.empty()) {
        std::swap(body_, element_text_);
      } else {
        body_ += element_text_;
        element_text_.clear();
      }
      return state::ok;
    }
    return state::full;
  }

  auto has_contents() const -> bool {
    return not body_.empty();
  }

  auto last_element_size() const -> uint64_t {
    return last_element_size_;
  }

  auto yield(diagnostic_handler& dh) -> std::string_view {
    TENZIR_ASSERT(not body_.empty());
    if (not codec_) {
      std::swap(body_, result_);
    } else {
      auto in_size = detail::narrow_cast<int64_t>(body_.size());
      auto* in_ptr = reinterpret_cast<uint8_t*>(body_.data());
      auto max_size = codec_->MaxCompressedLen(in_size, in_ptr);
      result_.resize(max_size);
      auto res = codec_->Compress(in_size, in_ptr,
                                  detail::narrow_cast<int64_t>(result_.size()),
                                  reinterpret_cast<uint8_t*>(result_.data()));
      if (not res.ok()) {
        diagnostic::error("compression failure: {}", res.status().ToString())
          .emit(dh);
        return {};
      }
      TENZIR_ASSERT(*res < max_size);
      result_.resize(*res);
    }
    std::swap(body_, element_text_);
    element_text_.clear();
    return result_;
  }

private:
  json_printer printer_;
  uint64_t max_size_{};
  std::string element_text_;
  std::string body_;
  std::string result_;
  std::unique_ptr<arrow::util::Codec> codec_;
  uint64_t last_element_size_{};
};

auto resolve_str(std::string_view option_name,
                 const std::optional<ast::expression>& expr,
                 const table_slice& slice, diagnostic_handler& dh)
  -> std::optional<series> {
  if (not expr) {
    return std::nullopt;
  }
  auto res = eval(*expr, slice, dh);
  auto b = arrow::StringBuilder{};
  for (auto& part : res.parts()) {
    if (auto* str = try_as<arrow::StringArray>(*part.array)) {
      if (res.parts().size() == 1) {
        return std::move(part);
      }
      check(append_array(b, string_type{}, *str));
    } else {
      diagnostic::warning("`{}` did not evaluate to a `{}`", option_name,
                          string_type{})
        .primary(*expr)
        .emit(dh);
      if (res.parts().size() == 1) {
        return std::nullopt;
      }
      check(b.AppendNulls(part.length()));
    }
  }
  return series{string_type{}, finish(b)};
}

class ToOpenSearch final : public Operator<table_slice, void> {
public:
  explicit ToOpenSearch(ToOpenSearchArgs args)
    : args_{std::move(args)},
      builder_{
        {
          .style = no_style(),
          .oneline = true,
          .omit_null_fields = not args_.include_nulls,
          .omit_empty_records = false,
          .omit_empty_lists = false,
        },
        args_.max_content_length.inner,
        args_.compress.is_some(),
      } {
  }

  auto start(OpCtx& ctx) -> Task<void> override {
    auto resolved_url = std::string{};
    auto user = std::string{};
    auto password = std::string{};
    auto requests = std::vector<secret_request>{
      make_secret_request("url", args_.url, resolved_url, ctx.dh()),
    };
    if (args_.user) {
      requests.emplace_back(
        make_secret_request("user", *args_.user, user, ctx.dh()));
    }
    if (args_.passwd) {
      requests.emplace_back(
        make_secret_request("passwd", *args_.passwd, password, ctx.dh()));
    }
    if (auto result = co_await ctx.resolve_secrets(std::move(requests));
        result.is_error()) {
      co_return;
    }
    auto parsed_url = boost::urls::parse_uri_reference(resolved_url);
    if (not parsed_url) {
      diagnostic::error("failed to parse url").primary(args_.url).emit(ctx);
      co_return;
    }
    auto final_url = boost::urls::url{*parsed_url};
    if (final_url.segments().empty()
        or final_url.segments().back() != "_bulk") {
      final_url.segments().push_back("_bulk");
    }
    url_ = std::string{final_url.buffer()};
    auto tls_needed
      = http::normalize_url_and_tls(args_.tls, url_, args_.url.source, ctx);
    if (tls_needed.is_error()) {
      co_return;
    }
    auto config = HttpPoolConfig{
      .tls = *tls_needed,
      .ssl_context = nullptr,
    };
    if (*tls_needed) {
      auto tls_opts = tls_options::from_optional(args_.tls);
      auto ssl_context = tls_opts.make_folly_ssl_context(
        ctx, std::addressof(ctx.actor_system().config()));
      if (ssl_context.is_error()) {
        co_return;
      }
      config.ssl_context = std::move(*ssl_context);
    }
    try {
      pool_ = HttpPool::make(ctx.io_executor(), url_, config);
    } catch (std::exception const& e) {
      diagnostic::error("failed to initialize HTTP client: {}", e.what())
        .primary(args_.url)
        .emit(ctx);
      co_return;
    }
    if (args_.user or args_.passwd) {
      auto token = detail::base64::encode(fmt::format("{}:{}", user, password));
      headers_["Authorization"] = fmt::format("Basic {}", token);
    }
    headers_["Content-Type"] = "application/json";
    if (args_.compress) {
      headers_["Content-Encoding"] = "gzip";
    }
    bytes_write_counter_
      = ctx.make_counter(MetricsLabel{"operator", "to_opensearch"},
                         MetricsDirection::write, MetricsVisibility::external_);
  }

  auto process(table_slice input, OpCtx& ctx) -> Task<void> override {
    input = resolve_enumerations(std::move(input));
    constexpr auto null_values
      = []() -> generator<std::optional<std::string_view>> {
      co_yield std::nullopt;
    };
    auto ids = resolve_str(
      "id", args_.id ? std::optional<ast::expression>{*args_.id} : std::nullopt,
      input, ctx.dh());
    auto idxs = resolve_str(
      "index",
      args_.index ? std::optional<ast::expression>{*args_.index} : std::nullopt,
      input, ctx.dh());
    auto acts = resolve_str(
      "action", std::optional<ast::expression>{args_.action}, input, ctx.dh());
    auto docs
      = eval(args_.doc ? *args_.doc
                       : ast::expression{ast::this_{args_.operator_location}},
             input, ctx.dh());
    constexpr auto ty = string_type{};
    auto id
      = ids ? values(ty, as<arrow::StringArray>(*ids->array)) : null_values();
    auto idx
      = idxs ? values(ty, as<arrow::StringArray>(*idxs->array)) : null_values();
    auto act
      = acts ? values(ty, as<arrow::StringArray>(*acts->array)) : null_values();
    for (auto&& doc : docs.values3()) {
      auto* ptr = try_as<view3<record>>(doc);
      auto action = act.next();
      auto actual_id = id.next();
      auto actual_idx = idx.next();
      if (not ptr) {
        diagnostic::warning("`doc` evaluated to non-record, skipping event")
          .primary(args_.doc
                     ? *args_.doc
                     : ast::expression{ast::this_{args_.operator_location}})
          .emit(ctx);
        continue;
      }
      if (not action or not action.value()) {
        diagnostic::warning("`action` evaluated to `null`, skipping event")
          .primary(args_.action)
          .emit(ctx);
        continue;
      }
      if (auto diag
          = builder_.create_metadata(**action, actual_idx, actual_id, args_)) {
        ctx.dh().emit(std::move(*diag));
        continue;
      }
      builder_.create_doc(**action, *ptr);
      switch (builder_.finish_event()) {
        using enum json_builder::state;
        case ok: {
          if (next_timeout_.is_none()) {
            next_timeout_
              = std::chrono::steady_clock::now() + args_.buffer_timeout.inner;
            buffer_ready_->notify_one();
          }
          break;
        }
        case full: {
          co_await send_request(ctx);
          if (builder_.has_contents()) {
            next_timeout_
              = std::chrono::steady_clock::now() + args_.buffer_timeout.inner;
          } else {
            next_timeout_ = None{};
          }
          break;
        }
        case event_too_large: {
          diagnostic::warning("event too large for given `max_content_length`")
            .note("serialized event size was `{}`",
                  builder_.last_element_size())
            .primary(args_.max_content_length)
            .emit(ctx);
          break;
        }
      }
    }
  }

  auto await_task(diagnostic_handler& dh) const -> Task<Any> override {
    TENZIR_UNUSED(dh);
    if (not next_timeout_) {
      co_await buffer_ready_->wait();
    }
    if (next_timeout_) {
      co_await sleep_until(*next_timeout_);
    }
    co_return {};
  }

  auto process_task(Any result, OpCtx& ctx) -> Task<void> override {
    TENZIR_UNUSED(result);
    if (std::chrono::steady_clock::now() < *next_timeout_) {
      co_return;
    }
    if (builder_.has_contents()) {
      co_await send_request(ctx);
    }
    next_timeout_ = None{};
  }

  auto finalize(OpCtx& ctx) -> Task<FinalizeBehavior> override {
    if (builder_.has_contents()) {
      co_await send_request(ctx);
    }
    co_return FinalizeBehavior::done;
  }

  auto prepare_snapshot(OpCtx& ctx) -> Task<void> override {
    if (builder_.has_contents()) {
      co_await send_request(ctx);
    }
    next_timeout_ = None{};
  }

private:
  enum class Lifecycle {
    running,
    done,
  };

  friend auto inspect(auto& f, Lifecycle& x) {
    return tenzir::detail::inspect_enum_str(f, x, {"running", "done"});
  }

  auto send_request(OpCtx& ctx) -> Task<void> {
    TENZIR_ASSERT(pool_);
    auto body = builder_.yield(ctx.dh());
    if (body.empty()) {
      co_return;
    }
    bytes_write_counter_.add(body.size());
    auto headers = headers_;
    headers["Content-Length"] = fmt::to_string(body.size());
    auto result
      = co_await (*pool_)->post(std::string{body}, std::move(headers));
    if (result.is_err()) {
      diagnostic::error("HTTP request failed: {}",
                        std::move(result).unwrap_err())
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    auto response = std::move(result).unwrap();
    if (response.status_code < 200 or response.status_code > 299) {
      diagnostic::warning("issue sending data. HTTP response code `{}`",
                          response.status_code)
        .note("response body: {}", response.body)
        .primary(args_.operator_location)
        .emit(ctx);
      co_return;
    }
    auto json = from_json(response.body);
    if (not json.has_value()) {
      co_return;
    }
    auto const* r = try_as<record>(&json.value());
    if (not r) {
      co_return;
    }
    auto it = r->find("errors");
    if (it == r->end()) {
      co_return;
    }
    if (as<bool>(it->second)) {
      diagnostic::warning("issue sending data")
        .note("response body: {}", response.body)
        .primary(args_.operator_location)
        .emit(ctx);
    }
  }

  ToOpenSearchArgs args_;
  json_builder builder_;
  Option<Box<HttpPool>> pool_ = None{};
  std::string url_;
  std::map<std::string, std::string> headers_;
  MetricsCounter bytes_write_counter_;
  mutable Option<std::chrono::steady_clock::time_point> next_timeout_;
  mutable Box<Notify> buffer_ready_{std::in_place};
};

class ToOpenSearchPlugin final : public virtual OperatorPlugin {
public:
  auto name() const -> std::string override {
    return "tql2.to_opensearch";
  }

  auto describe() const -> Description override {
    auto d = Describer<ToOpenSearchArgs, ToOpenSearch>{};
    d.positional("url", &ToOpenSearchArgs::url);
    d.named("action", &ToOpenSearchArgs::action, "string");
    d.named("index", &ToOpenSearchArgs::index, "string");
    d.named("id", &ToOpenSearchArgs::id, "string");
    d.named("doc", &ToOpenSearchArgs::doc, "record");
    d.named("user", &ToOpenSearchArgs::user);
    d.named("passwd", &ToOpenSearchArgs::passwd);
    d.named("include_nulls", &ToOpenSearchArgs::include_nulls);
    auto max_content_length = d.named_optional(
      "max_content_length", &ToOpenSearchArgs::max_content_length);
    auto buffer_timeout
      = d.named_optional("buffer_timeout", &ToOpenSearchArgs::buffer_timeout);
    d.named("compress", &ToOpenSearchArgs::compress);
    d.operator_location(&ToOpenSearchArgs::operator_location);
    auto tls_validator = tls_options{
      {.is_server = false}}.add_to_describer(d, &ToOpenSearchArgs::tls);
    d.validate([=](DescribeCtx& ctx) -> Empty {
      tls_validator(ctx);
      if (auto value = ctx.get(max_content_length)) {
        if (value->inner == 0) {
          diagnostic::error("`max_content_length` must be positive")
            .primary(value->source)
            .emit(ctx);
        }
      }
      if (auto value = ctx.get(buffer_timeout)) {
        if (value->inner <= duration::zero()) {
          diagnostic::error("`buffer_timeout` must be positive")
            .primary(value->source)
            .emit(ctx);
        }
      }
      return {};
    });
    return d.without_optimize();
  }
};

} // namespace
} // namespace tenzir::plugins::opensearch2

TENZIR_REGISTER_PLUGIN(tenzir::plugins::opensearch2::ToOpenSearchPlugin)
