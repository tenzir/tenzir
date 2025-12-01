//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser2.hpp>
#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/printable/tenzir/json.hpp>
#include <tenzir/curl.hpp>
#include <tenzir/detail/url.hpp>
#include <tenzir/location.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/ssl_options.hpp>
#include <tenzir/tql2/eval.hpp>

#include <arrow/util/compression.h>
#include <boost/url/parse.hpp>
#include <fmt/core.h>

namespace tenzir::plugins::opensearch {
namespace {

struct opensearch_args {
  located<secret> url;
  ast::expression action;
  std::optional<ast::expression> index;
  std::optional<ast::expression> doc;
  std::optional<ast::expression> id;
  std::optional<located<secret>> user;
  std::optional<located<secret>> passwd;
  ssl_options ssl;
  std::optional<location> include_nulls;
  std::optional<located<uint64_t>> max_content_length
    = located{5'000'000, location::unknown};
  std::optional<located<duration>> buffer_timeout
    = located{std::chrono::seconds{5}, location::unknown};
  std::optional<location> compress = location::unknown;
  std::optional<location> _debug_curl;
  location operator_location = location::unknown;

  auto add_to(argument_parser2& parser) -> void {
    parser.positional("url", url)
      .named("action", action, "string")
      .named("index", index, "string")
      .named("id", id, "string")
      .named("doc", doc, "record")
      .named("user", user)
      .named("passwd", passwd)
      .named("include_nulls", include_nulls)
      .named("max_content_length", max_content_length)
      .named("buffer_timeout", buffer_timeout)
      .named("compress", compress)
      .named("_debug_curl", _debug_curl);
    ssl.add_tls_options(parser);
  }

  auto validate(diagnostic_handler& dh) -> failure_or<void> {
    if (max_content_length->inner <= 0) {
      diagnostic::error("`max_content_length` must be positive")
        .primary(*max_content_length)
        .emit(dh);
      return failure::promise();
    }
    if (buffer_timeout->inner <= duration::zero()) {
      diagnostic::error("`buffer_timeout` must be positive")
        .primary(*buffer_timeout)
        .emit(dh);
      return failure::promise();
    }

    if (_debug_curl) {
      if (not url.inner.is_all_literal()) {
        diagnostic::error(
          "cannot use `_debug_curl` when an argument is a secret")
          .primary(*_debug_curl)
          .primary(url.source)
          .emit(dh);
        return failure::promise();
      }
#define X(NAME)                                                                \
  if (NAME and not NAME->inner.is_all_literal()) {                             \
    diagnostic::error("cannot use `_debug_curl` when an argument is a secret") \
      .primary(*_debug_curl)                                                   \
      .primary(NAME->source)                                                   \
      .emit(dh);                                                               \
    return failure::promise();                                                 \
  }
      X(user)
      X(passwd)
#undef X
    }
    return {};
  }

  friend auto inspect(auto& f, opensearch_args& x) -> bool {
    return f.object(x).fields(
      f.field("url", x.url), f.field("index", x.index),
      f.field("action", x.action), f.field("doc", x.doc), f.field("id", x.id),
      f.field("user", x.user), f.field("passwd", x.passwd),
      f.field("ssl", x.ssl), f.field("include_nulls", x.include_nulls),
      f.field("max_content_length", x.max_content_length),
      f.field("buffer_timeout", x.buffer_timeout),
      f.field("compress", x.compress), f.field("_debug_curl", x._debug_curl),
      f.field("operator_location", x.operator_location));
  }
};

class json_builder {
public:
  json_builder(json_printer_options printer_opts, uint64_t max_size,
               bool compress)
    : printer_{std::move(printer_opts)}, max_size_{max_size} {
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
                       const opensearch_args& args)
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
    if (std::ssize(body_) + last_element_size_ < max_size_) {
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

  auto last_element_size() const -> int64_t {
    return last_element_size_;
  }

  auto yield(diagnostic_handler& dh) -> std::string_view {
    TENZIR_ASSERT(not body_.empty());
    if (not codec_) {
      // make the result body available
      std::swap(body_, result_);
    } else {
      auto in_size = body_.size();
      auto in_ptr = reinterpret_cast<uint8_t*>(body_.data());
      auto max_size = codec_->MaxCompressedLen(in_size, in_ptr);
      result_.resize(max_size);
      auto res = codec_->Compress(in_size, in_ptr, result_.size(),
                                  reinterpret_cast<uint8_t*>(result_.data()));
      if (not res.ok()) {
        diagnostic::error("compression failure: {}", res.status().ToString())
          .emit(dh);
        return {};
      }
      TENZIR_ASSERT(*res < max_size);
      result_.resize(*res);
    }
    // swap potentially remaining event text into the result body
    std::swap(body_, element_text_);
    // clear the remaining event text
    element_text_.clear();
    return result_;
  }

private:
  json_printer printer_;
  uint64_t max_size_{};
  std::string element_text_{};
  std::string body_{};
  std::string result_{};
  std::unique_ptr<arrow::util::Codec> codec_{};
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
    if (auto str = try_as<arrow::StringArray>(*part.array)) {
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

class opensearch_operator final : public crtp_operator<opensearch_operator> {
public:
  opensearch_operator() = default;

  opensearch_operator(opensearch_args args) : args_{std::move(args)} {
  }

  auto send_req(curl::easy& req, const std::string_view body,
                diagnostic_handler& dh) const {
    auto response = std::string{};
    const auto write_callback = [&](std::span<const std::byte> data) {
      response.append(reinterpret_cast<const char*>(data.data()),
                      reinterpret_cast<const char*>(data.data() + data.size()));
    };
    check(req.set(write_callback));
    check(req.set(CURLOPT_POSTFIELDS, body));
    check(req.set(CURLOPT_POSTFIELDSIZE, detail::narrow<long>(body.size())));
    req.set_http_header("Content-Length", fmt::to_string(body.size()));
    if (const auto ec = req.perform(); ec != curl::easy::code::ok) {
      diagnostic::error("{}", to_string(ec))
        .primary(args_.operator_location)
        .emit(dh);
      return;
    }
    const auto [ec, http_code] = req.get<curl::easy::info::response_code>();
    check(ec);
    if (http_code < 200 or http_code > 299) {
      diagnostic::warning("issue sending data. HTTP response code `{}`",
                          http_code)
        .note("response body: {}", response)
        .primary(args_.operator_location)
        .emit(dh);
      return;
    }
    auto json = from_json(response);
    if (not json.has_value()) {
      return;
    }
    const auto r = try_as<record>(json.value());
    if (not r) {
      return;
    }
    const auto it = r->find("errors");
    if (it == r->end()) {
      return;
    }
    const auto errors = as<bool>(it->second);
    if (errors) {
      diagnostic::warning("issue sending data")
        .note("response body: {}", response)
        .primary(args_.operator_location)
        .emit(dh);
    }
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    auto& dh = ctrl.diagnostics();
    auto url = resolved_secret_value{};
    auto user = resolved_secret_value{};
    auto password = resolved_secret_value{};
    {
      auto requests = std::vector<secret_request>{
        {args_.url, url},
      };
      if (args_.user) {
        requests.emplace_back(*args_.user, user);
      }
      if (args_.passwd) {
        requests.emplace_back(*args_.passwd, password);
      }
      co_yield ctrl.resolve_secrets_must_yield(std::move(requests));
    }
    auto final_url = std::string{};
    {
      const auto url_utf8_result = url.utf8_view("url", args_.url.source, dh);
      if (! url_utf8_result) {
        co_return;
      }
      const auto parsed_url
        = boost::urls::parse_uri_reference(*url_utf8_result);
      if (not parsed_url) {
        diagnostic::error("failed to parse url").primary(args_.url).emit(dh);
        co_return;
      }
      if (parsed_url->segments().empty()
          or parsed_url->segments().back() != "_bulk") {
        auto u = boost::urls::url{*parsed_url};
        u.segments().push_back("_bulk");
        final_url = fmt::to_string(u);
      }
    }
    if (not args_.ssl.validate(final_url, args_.url.source, dh)) {
      co_return;
    }
    auto req = curl::easy{};
    if (args_.user or args_.passwd) {
      const auto user_utf8
        = args_.user ? user.utf8_view("user", args_.user->source, dh).unwrap()
                     : std::string_view{};
      const auto password_utf8
        = args_.passwd
            ? password.utf8_view("password", args_.passwd->source, dh).unwrap()
            : std::string_view{};
      const auto token = detail::base64::encode(
        fmt::format("{}:{}", user_utf8, password_utf8));
      req.set_http_header("Authorization", fmt::format("Basic {}", token));
      user.clear();
      password.clear();
    }
    req.set_http_header("Content-Type", "application/json");
    if (args_.compress) {
      req.set_http_header("Content-Encoding", "gzip");
    }
    if (auto e = args_.ssl.apply_to(req, final_url, &ctrl)) {
      diagnostic::error(e).emit(dh);
      co_return;
    }
    check(req.set(CURLOPT_POST, 1));
    check(req.set(CURLOPT_URL, final_url));
    check(req.set(CURLOPT_VERBOSE, args_._debug_curl ? 1 : 0));
    auto b = json_builder{
      {
        .style = no_style(),
        .oneline = true,
        .omit_null_fields = not args_.include_nulls,
        .omit_empty_records = false,
        .omit_empty_lists = false,
      },
      args_.max_content_length->inner,
      args_.compress.has_value(),
    };
    for (auto last = time::clock::now(); auto&& slice : input) {
      const auto now = time::clock::now();
      if (now - last > args_.buffer_timeout->inner and b.has_contents()) {
        send_req(req, b.yield(dh), dh);
        last = now;
      }
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      slice = resolve_enumerations(std::move(slice));
      constexpr auto ng = []() -> generator<std::optional<std::string_view>> {
        co_yield {std::nullopt};
      };
      const auto ids = resolve_str("id", args_.id, slice, dh);
      const auto idxs = resolve_str("index", args_.index, slice, dh);
      const auto acts = resolve_str("action", args_.action, slice, dh);
      const auto docs = eval(
        args_.doc.value_or(ast::this_{args_.operator_location}), slice, dh);
      constexpr auto ty = string_type{};
      auto id = ids ? values(ty, as<arrow::StringArray>(*ids->array)) : ng();
      auto idx = idxs ? values(ty, as<arrow::StringArray>(*idxs->array)) : ng();
      auto act = acts ? values(ty, as<arrow::StringArray>(*acts->array)) : ng();
      for (auto&& doc : docs.values3()) {
        const auto ptr = try_as<view3<record>>(doc);
        const auto action = act.next();
        const auto actual_id = id.next();
        const auto actual_idx = idx.next();
        if (not ptr) {
          diagnostic::warning("`doc` evaluated to non-record, skipping event")
            .primary(*args_.doc)
            .emit(dh);
          continue;
        }
        if (not action or not action.value()) {
          diagnostic::warning("`action` evaluated to `null`, skipping event")
            .primary(args_.action)
            .emit(dh);
          continue;
        }
        if (auto diag
            = b.create_metadata(**action, actual_idx, actual_id, args_)) {
          dh.emit(std::move(diag).value());
          continue;
        }
        b.create_doc(**action, *ptr);
        switch (b.finish_event()) {
          using enum json_builder::state;
          case ok: {
            break;
          }
          case full: {
            send_req(req, b.yield(dh), dh);
            break;
          }
          case event_too_large: {
            diagnostic::warning(
              "event too large for given `max_content_length`")
              .note("serialized event size was `{}`", b.last_element_size())
              .primary(*args_.max_content_length)
              .emit(dh);
            break;
          }
        }
      }
    }
    if (b.has_contents()) {
      send_req(req, b.yield(dh), dh);
    }
  }

  auto optimize(expression const&, event_order) const
    -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "to_opensearch";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  friend auto inspect(auto& f, opensearch_operator& x) -> bool {
    return f.apply(x.args_);
  }

  auto detached() const -> bool override {
    return true;
  }

private:
  opensearch_args args_;
};

struct plugin : public virtual operator_plugin2<opensearch_operator> {
  auto name() const -> std::string override {
    return "to_opensearch";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = opensearch_args{};
    auto p = argument_parser2::operator_(name());
    args.add_to(p);
    args.operator_location = inv.self.get_location();
    TRY(p.parse(inv, ctx));
    if (not args.validate(ctx)) {
      return failure::promise();
    }
    return std::make_unique<opensearch_operator>(std::move(args));
  }

  auto save_properties() const -> save_properties_t override {
    return {.schemes = {"elasticsearch", "opensearch"},
            .strip_scheme = true,
            .events = true};
  }
};

} // namespace
} // namespace tenzir::plugins::opensearch

TENZIR_REGISTER_PLUGIN(tenzir::plugins::opensearch::plugin)
