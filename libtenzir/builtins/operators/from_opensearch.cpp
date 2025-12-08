//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/actors.hpp"
#include "tenzir/argument_parser2.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/json_parser.hpp"
#include "tenzir/multi_series_builder.hpp"
#include "tenzir/plugin.hpp"
#include "tenzir/tls_options.hpp"
#include "tenzir/view3.hpp"

#include <arrow/util/compression.h>
#include <caf/actor_from_state.hpp>
#include <caf/actor_system_config.hpp>
#include <caf/async/spsc_buffer.hpp>
#include <caf/event_based_actor.hpp>
#include <caf/expected.hpp>
#include <caf/flow/observable_builder.hpp>
#include <caf/net/http/method.hpp>
#include <caf/net/http/server.hpp>
#include <caf/net/http/with.hpp>
#include <caf/net/ssl/context.hpp>
#include <caf/scheduled_actor/flow.hpp>
#include <caf/timespan.hpp>

#include <charconv>

namespace tenzir::plugins::from_opensearch {
namespace {

namespace http = caf::net::http;
namespace ssl = caf::net::ssl;

constexpr auto inner(const located<std::string>& x) -> std::string {
  return x.inner;
}

auto split_at_newline(const chunk_ptr& chunk)
  -> std::vector<std::vector<std::byte>> {
  if (not chunk || chunk->size() == 0) {
    return {};
  }
  auto svs = std::vector<std::vector<std::byte>>{};
  const auto end = chunk->end();
  auto start = chunk->begin();
  auto newline = std::find(start, end, std::byte{'\n'});
  while (newline != end) {
    ++newline;
    auto& vec = svs.emplace_back();
    vec.reserve(std::distance(start, newline) + simdjson::SIMDJSON_PADDING);
    vec.insert(vec.begin(), start, newline);
    start = newline;
    newline = std::find(start, end, std::byte{'\n'});
  }
  auto& vec = svs.emplace_back();
  vec.reserve(std::distance(start, newline) + simdjson::SIMDJSON_PADDING);
  vec.insert(vec.begin(), start, newline);
  return svs;
}

struct opensearch_args {
  location op;
  located<std::string> url{"0.0.0.0:9200", location::unknown};
  bool keep_actions{false};
  std::optional<location> tls;
  std::optional<located<std::string>> keyfile;
  std::optional<located<std::string>> certfile;
  std::optional<located<std::string>> password;
  uint16_t port{9200};
  located<uint64_t> max_request_size{10 * 1024 * 1024, location::unknown};

  auto
  add_to(argument_parser2& p, std::optional<located<std::string>>& url_op) {
    p.positional("url", url_op);
    p.named_optional("keep_actions", keep_actions);
    p.named_optional("max_request_size", max_request_size);
    p.named("tls", tls);
    p.named("certfile", certfile);
    p.named("keyfile", keyfile);
    p.named("password", password);
  }

  auto validate(std::optional<located<std::string>> url_op,
                diagnostic_handler& dh) -> failure_or<void> {
    if (url_op) {
      url = std::move(*url_op);
    }
    if (url.inner.empty()) {
      diagnostic::error("`url` must not be empty").primary(url).emit(dh);
    }
    if (const auto col = url.inner.rfind(':'); col != std::string::npos) {
      const auto* end = url.inner.data() + url.inner.size();
      const auto [ptr, err]
        = std::from_chars(url.inner.data() + col + 1, end, port);
      if (err != std::errc{}) {
        diagnostic::error("failed to parse port").primary(url).emit(dh);
      }
      if (col == 0 or ptr != end) {
        diagnostic::error("`url` must have the form `host[:port]`")
          .primary(url)
          .emit(dh);
      }
      url.inner.resize(col);
    }
    if (max_request_size.inner == 0) {
      diagnostic::error("request size must not be zero")
        .primary(max_request_size)
        .emit(dh);
      return failure::promise();
    }
    const auto tls_logic
      = [&](const std::optional<located<std::string>>& opt,
            std::string_view name, bool required = false) -> failure_or<void> {
      if (not tls) {
        if (opt) {
          diagnostic::error("`{}` is unused when `tls` is disabled", name)
            .primary(*opt)
            .emit(dh);
          return failure::promise();
        }
        return {};
      }
      if (not opt and required) {
        diagnostic::error("`{}` must be set when enabling `tls`", name)
          .secondary(*tls)
          .emit(dh);
        return failure::promise();
      }
      if (opt and opt->inner.empty()) {
        diagnostic::error("`{}` must not be empty", name).primary(*opt).emit(dh);
        return failure::promise();
      }
      return {};
    };
    TRY(tls_logic(certfile, "certfile", true));
    TRY(tls_logic(keyfile, "keyfile", true));
    TRY(tls_logic(password, "password"));
    return {};
  }

  friend auto inspect(auto& f, opensearch_args& x) -> bool {
    return f.object(x).fields(
      f.field("op", x.op), f.field("port", x.port), f.field("url", x.url),
      f.field("max_request_size", x.max_request_size),
      f.field("keep_actions", x.keep_actions), f.field("tls", x.tls),
      f.field("certfile", x.certfile), f.field("keyfile", x.keyfile),
      f.field("password", x.password));
  }
};

auto decompress_payload(const http::request& r,
                        diagnostic_handler& dh) -> std::optional<chunk_ptr> {
  if (not r.header().has_field("Content-Encoding")) {
    // TODO: Can we take ownership?
    return chunk::copy(r.payload());
  }
  const auto encoding = r.header().field("Content-Encoding");
  const auto compression_type
    = arrow::util::Codec::GetCompressionType(std::string{encoding});
  if (not compression_type.ok()) {
    diagnostic::warning("invalid compression type: {}", encoding)
      .note("must be one of `brotli`, `bz2`, `gzip`, `lz4`, `zstd`")
      .emit(dh);
    return std::nullopt;
  }
  auto out = std::vector<uint8_t>{};
  out.resize(r.payload().size_bytes() * 2);
  const auto codec = arrow::util::Codec::Create(
    compression_type.ValueUnsafe(), arrow::util::kUseDefaultCompressionLevel);
  TENZIR_ASSERT(codec.ok());
  if (not codec.ValueUnsafe()) {
    return chunk::copy(r.payload());
  }
  const auto decompressor = check(codec.ValueUnsafe()->MakeDecompressor());
  auto written = size_t{};
  auto read = size_t{};
  while (read != r.payload().size_bytes()) {
    const auto result = decompressor->Decompress(
      detail::narrow<long>(r.payload().size_bytes() - read),
      reinterpret_cast<const uint8_t*>(r.payload().data() + read),
      detail::narrow<long>(out.capacity() - written), out.data() + written);
    if (not result.ok()) {
      diagnostic::warning("failed to decompress: {}",
                          result.status().ToString())
        .emit(dh);
      return std::nullopt;
    }
    TENZIR_ASSERT(std::cmp_less_equal(result->bytes_written, out.size()));
    written += result->bytes_written;
    read += result->bytes_read;
    if (result->need_more_output) {
      if (out.size() == out.max_size()) [[unlikely]] {
        diagnostic::error("failed to resize buffer").emit(dh);
        return std::nullopt;
      }
      if (out.size() < out.max_size() / 2) {
        out.resize(out.size() * 2);
      } else [[unlikely]] {
        out.resize(out.max_size());
      }
    }
    // In case the input contains multiple concatenated compressed streams,
    // we gracefully reset the decompressor.
    if (decompressor->IsFinished()) {
      const auto result = decompressor->Reset();
      if (not result.ok()) {
        diagnostic::warning("failed to reset decompressor: {}",
                            result.ToString())
          .emit(dh);
        return std::nullopt;
      }
    }
  }
  TENZIR_ASSERT(written != 0);
  out.resize(written);
  return chunk::make(std::move(out));
}

auto handle_slice(bool& is_action, const table_slice& slice) -> table_slice {
  if (slice.rows() == 0) {
    return {};
  }
  const auto ty = as<record_type>(slice.schema());
  auto fields = std::vector<record_type::field_view>{};
  for (const auto& field : ty.fields()) {
    fields.push_back(field);
  }
  const auto delete_ = std::ranges::any_of(
    fields,
    [](auto&& x) {
      return x == "delete";
    },
    &record_type::field_view::name);

  const auto other_actions = std::ranges::any_of(
    fields,
    [](auto&& x) {
      return x == "create" or x == "index" or x == "update";
    },
    &record_type::field_view::name);

  if (delete_) {
    return is_action ? table_slice{} : subslice(slice, 0, 1);
  }
  if (other_actions) {
    auto filtered = std::vector<table_slice>{};
    if (is_action) {
      is_action = slice.rows() % 2 == 0;
      for (auto i = size_t{1}; i < slice.rows(); i += 2) {
        filtered.push_back(subslice(slice, i, i + 1));
      }
      return concatenate(filtered);
    }
    is_action = slice.rows() % 2 != 0;
    for (auto i = size_t{}; i < slice.rows(); i += 2) {
      filtered.push_back(subslice(slice, i, i + 1));
    }
    return concatenate(filtered);
  }
  if (is_action) {
    is_action = slice.rows() % 2 == 0;
    return {};
  }
  // TODO: Diagnostic?
  is_action = slice.rows() % 2 != 0;
  return subslice(slice, 0, 1);
}

class from_opensearch_operator final
  : public crtp_operator<from_opensearch_operator> {
public:
  from_opensearch_operator() = default;

  from_opensearch_operator(opensearch_args args) : args_{std::move(args)} {
  }

  auto
  operator()(operator_control_plane& ctrl) const -> generator<table_slice> {
    co_yield {};
    auto slices = std::vector<table_slice>{};
    auto stream = std::optional<caf::typed_stream<std::vector<table_slice>>>{};
    auto [ptr, launch] = ctrl.self().system().spawn_inactive();

    // Query TLS min version from config
    auto tls_min_version = ssl::tls::any;
    if (args_.tls.has_value()) {
      auto& config = ctrl.self().system().config();
      if (auto* v = caf::get_if<std::string>(&config.content,
                                             "tenzir.tls.min-version")) {
        auto version = parse_caf_tls_version(*v);
        if (version) {
          tls_min_version = *version;
        } else {
          diagnostic::warning(version.error())
            .note("while parsing TLS configuration for from_opensearch")
            .emit(ctrl.diagnostics());
        }
      }
    }

    auto context
      = ssl::context::enable(args_.tls.has_value())
          .and_then(ssl::emplace_server(tls_min_version))
          .and_then(ssl::use_private_key_file_if(args_.keyfile.transform(inner),
                                                 ssl::format::pem))
          .and_then(ssl::use_certificate_file_if(
            args_.certfile.transform(inner), ssl::format::pem))
          .and_then(ssl::use_password_if(args_.password.transform(inner)))
          .and_then(ssl::enable_default_verify_paths());
    auto server
      = http::with(ctrl.self().system())
          .context(std::move(context))
          .accept(args_.port, args_.url.inner)
          .monitor(static_cast<exec_node_actor>(&ctrl.self()))
          .max_request_size(args_.max_request_size.inner)
          .route(
            "/", http::method::get,
            [](http::responder& r) {
              r.respond(
                http::status::ok, "application/x-ndjson",
                R"({"name":"hostname","cluster_name":"opensearch","cluster_uuid":"rTLctDY8SoqcaEkfmuyGFA","version":{"distribution":"opensearch","number":"8.17.0","build_flavor":"default","build_type":"tar","build_hash":"unknown","build_date":"2025-02-21T09:34:11Z","build_snapshot":false,"lucene_version":"9.12.1","minimum_wire_compatibility_version":"7.10.0","minimum_index_compatibility_version":"7.0.0"},"tagline":"Tenzir from_opensearch"})");
            })
          .start([&](const caf::async::consumer_resource<http::request>& c) {
            stream
              = c.observe_on(ptr)
                  .flat_map([keep_actions = args_.keep_actions,
                             dh = ctrl.shared_diagnostics()](
                              const http::request& r) mutable
                            -> std::optional<std::vector<table_slice>> {
                    if (r.header().path() != "/_bulk") {
                      TENZIR_VERBOSE("unhandled {} {}",
                                     to_string(r.header().method()),
                                     r.header().path());
                      if (r.header().method() == http::method::head) {
                        r.respond(http::status::ok, "", "");
                      } else {
                        r.respond(http::status::ok, "application/x-ndjson",
                                  "{}");
                      }
                      return std::nullopt;
                    }
                    r.respond(
                      http::status::ok, "application/x-ndjson",
                      R"({"errors":false,"items":[{"create":{"status":201,"result":"created"}}]})");
                    auto ptr = decompress_payload(r, dh);
                    if (not ptr) {
                      return std::nullopt;
                    }
                    auto parser
                      = json::ndjson_parser{"from_opensearch", dh, {}};
                    for (const auto& chunk : split_at_newline(*ptr)) {
                      if (chunk.empty()) {
                        continue;
                      }
                      auto view = simdjson::padded_string_view{
                        reinterpret_cast<const char*>(chunk.data()),
                        chunk.size(),
                        chunk.capacity(),
                      };
                      parser.parse(view);
                    }
                    TENZIR_ASSERT(not parser.abort_requested);
                    auto result = parser.builder.finalize_as_table_slice();
                    if (keep_actions) {
                      return result;
                    }
                    auto slices = std::vector<table_slice>{};
                    auto is_action = true;
                    for (const auto& slice : result) {
                      slices.push_back(handle_slice(is_action, slice));
                    }
                    return slices;
                  })
                  .to_typed_stream<std::vector<table_slice>>(
                    "from_opensearch", std::chrono::seconds{1}, 1);
          });
    if (not server) {
      diagnostic::error("failed to setup http server: {}", server.error())
        .primary(args_.op)
        .emit(ctrl.diagnostics());
      co_return;
    }
    auto _guard
      = detail::scope_guard{[server = std::move(*server)]() mutable noexcept {
          // NOTE: This could very well throw but scope_guard needs to be
          // noexcept.
          server.dispose();
        }};
    TENZIR_ASSERT(stream);
    ctrl.self()
      .observe(*stream, 30, 10)
      .for_each([&](std::vector<table_slice> result) {
        ctrl.set_waiting(false);
        std::ranges::move(result, std::back_inserter(slices));
      });
    launch();
    while (true) {
      ctrl.set_waiting(true);
      co_yield {};
      // NOTE: Must be an index-based loop. The thread can go back to the
      // observe loop after yielding here, causing the vector's iterator to be
      // invalidated.
      for (auto i = size_t{}; i < slices.size(); ++i) {
        co_yield slices[i];
      }
      slices.clear();
    }
  }

  auto
  optimize(expression const&, event_order) const -> optimize_result override {
    return do_not_optimize(*this);
  }

  auto name() const -> std::string override {
    return "from_opensearch";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  friend auto inspect(auto& f, from_opensearch_operator& x) -> bool {
    return f.apply(x.args_);
  }

private:
  opensearch_args args_;
};

struct plugin final
  : public virtual operator_plugin2<from_opensearch_operator> {
  auto name() const -> std::string override {
    return "from_opensearch";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto args = opensearch_args{};
    args.op = inv.self.get_location();
    auto url_op = std::optional<located<std::string>>{};
    auto p = argument_parser2::operator_(name());
    args.add_to(p, url_op);
    TRY(p.parse(inv, ctx));
    TRY(args.validate(std::move(url_op), ctx));
    return std::make_unique<from_opensearch_operator>(std::move(args));
  }

  auto load_properties() const -> load_properties_t override {
    return {
      .schemes = {"elasticsearch", "opensearch"},
      .strip_scheme = true,
      .events = true,
    };
  }
};

} // namespace
} // namespace tenzir::plugins::from_opensearch

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from_opensearch::plugin)
