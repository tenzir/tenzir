//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/diagnostics.hpp>
#include <tenzir/file.hpp>
#include <tenzir/glob.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/pipeline_executor.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/prepend_token.hpp>
#include <tenzir/scope_linked.hpp>
#include <tenzir/tql/fwd.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/eval_impl.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/filesystem/api.h>
#include <arrow/util/future.h>
#include <arrow/util/uri.h>
#include <boost/unordered_set.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <caf/actor_from_state.hpp>
#include <caf/mail_cache.hpp>

#include <ranges>

namespace tenzir::plugins::from {

namespace {

/// How long to wait between file queries when using `from_file watch=true`.
constexpr auto watch_pause = std::chrono::seconds{10};

/// The maximum number of concurrent pipelines for `from_file`.
constexpr auto max_jobs = 10;

class from_events final : public crtp_operator<from_events> {
public:
  from_events() = default;

  explicit from_events(std::vector<ast::expression> events)
    : events_{std::move(events)} {
  }

  auto name() const -> std::string override {
    return "tql2.from_events";
  }

  auto operator()() const -> generator<table_slice> {
    // We suppress diagnostics here as we already evaluated the expression once
    // as part of the `from` operator. This avoids `from {x: 3 * null}` emitting
    // the same warning twice.
    auto null_dh = null_diagnostic_handler{};
    auto null_sp = session_provider::make(null_dh);
    const auto non_const_eval = [&](const ast::expression& expr) {
      auto value = evaluator{nullptr, null_sp.as_session()}.eval(expr);
      TENZIR_ASSERT(value.length() == 1);
      TENZIR_ASSERT(value.parts().size() == 1);
      return value.part(0);
    };
    for (auto& expr : events_) {
      auto slice = non_const_eval(expr);
      auto cast = slice.as<record_type>();
      TENZIR_ASSERT(cast);
      auto schema = tenzir::type{"tenzir.from", cast->type};
      co_yield table_slice{arrow::RecordBatch::Make(schema.to_arrow_schema(),
                                                    cast->length(),
                                                    cast->array->fields()),
                           schema};
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, from_events& x) -> bool {
    return f.apply(x.events_);
  }

private:
  std::vector<ast::expression> events_;
};

using from_events_plugin = operator_inspection_plugin<from_events>;

template <bool is_loading>
struct from_to_trait;

template <>
struct from_to_trait<true> {
  constexpr static std::string_view operator_name = "from";
  constexpr static std::string_view default_io_operator = "tql2.load_file";
  using io_properties_t = operator_factory_plugin::load_properties_t;
  constexpr static auto io_properties_getter
    = &operator_factory_plugin::load_properties;
  constexpr static auto io_properties_range_member
    = &operator_factory_plugin::load_properties_t::schemes;
  using compression_properties_t
    = operator_factory_plugin::decompress_properties_t;
  constexpr static auto compression_properties_getter
    = &operator_factory_plugin::decompress_properties;
  constexpr static auto compression_properties_range_member
    = &operator_factory_plugin::decompress_properties_t::extensions;
  using rw_properties_t = operator_factory_plugin::read_properties_t;
  constexpr static auto rw_properties_getter
    = &operator_factory_plugin::read_properties;
  constexpr static auto rw_properties_range_member
    = &operator_factory_plugin::read_properties_t::extensions;
};
template <>
struct from_to_trait<false> {
  constexpr static std::string_view operator_name = "to";
  constexpr static std::string_view default_io_operator = "tql2.save_file";
  using io_properties_t = operator_factory_plugin::save_properties_t;
  constexpr static auto io_properties_getter
    = &operator_factory_plugin::save_properties;
  constexpr static auto io_properties_range_member
    = &operator_factory_plugin::save_properties_t::schemes;
  using compression_properties_t
    = operator_factory_plugin::compress_properties_t;
  constexpr static auto compression_properties_getter
    = &operator_factory_plugin::compress_properties;
  constexpr static auto compression_properties_range_member
    = &operator_factory_plugin::compress_properties_t::extensions;
  using rw_properties_t = operator_factory_plugin::write_properties_t;
  constexpr static auto rw_properties_range_member
    = &operator_factory_plugin::write_properties_t::extensions;
  constexpr static auto rw_properties_getter
    = &operator_factory_plugin::write_properties;
};

auto find_given(std::string_view what, auto func, auto member,
                std::vector<std::string>& possibilities)
  -> std::pair<const operator_factory_plugin*,
               decltype((std::declval<operator_factory_plugin>().*func)())> {
  for (const auto& p : plugins::get<operator_factory_plugin>()) {
    auto properties = (p->*func)();
    for (auto possibility : properties.*member) {
      if (what.ends_with(possibility)) {
        return {p, std::move(properties)};
      }
      possibilities.push_back(std::move(possibility));
    }
  }
  return {};
}

template <bool is_loading>
auto find_connector_given(std::string_view what, std::string_view path,
                          location loc, const char* docs, session ctx)
  -> std::pair<const operator_factory_plugin*,
               typename from_to_trait<is_loading>::io_properties_t> {
  using traits = from_to_trait<is_loading>;
  auto possibilities = std::vector<std::string>{};
  auto res = find_given(what, traits::io_properties_getter,
                        traits::io_properties_range_member, possibilities);
  if (res.first) {
    return res;
  }
  std::ranges::sort(possibilities);
  if (loc.end - loc.begin == path.size() + 2) {
    loc.begin += 1;
    loc.end = loc.begin + what.size();
  }
  diagnostic::error("unsupported scheme `{}`", what)
    .primary(loc)
    .note("supported schemes for deduction: `{}`",
          fmt::join(possibilities, "`, `"))
    .docs(docs)
    .emit(ctx);
  return {};
}

template <auto getter, auto member>
auto find_plugin(std::string_view extension)
  -> std::tuple<const operator_factory_plugin*, std::string,
                std::vector<std::string>> {
  const operator_factory_plugin* found_plugin = nullptr;
  auto found_extension = std::string{};
  auto all_extensions = std::vector<std::string>{};
  for (const auto& plugin : plugins::get<operator_factory_plugin>()) {
    const auto props = (plugin->*getter)();
    for (const auto& possibility : props.*member) {
      TENZIR_ASSERT(not possibility.starts_with('.'));
      TENZIR_ASSERT(not possibility.empty());
      const auto matches
        = extension.size() > possibility.size()
          and extension[extension.size() - possibility.size() - 1] == '.'
          and extension.ends_with(possibility);
      if (matches) {
        TENZIR_ASSERT(not found_plugin);
        found_plugin = plugin;
        found_extension = possibility;
      }
      all_extensions.push_back(std::move(possibility));
    }
  }
  return {found_plugin, found_extension, all_extensions};
}

struct compression_and_format {
  compression_and_format(
    const operator_factory_plugin* compression,
    std::reference_wrapper<const operator_factory_plugin> format)
    : compression{compression}, format{format} {
  }

  /// Compression is optional.
  const operator_factory_plugin* compression;
  /// Format is required.
  std::reference_wrapper<const operator_factory_plugin> format;
};

template <bool is_loading>
auto find_compression_and_format(std::string_view extension,
                                 located<std::string_view> url,
                                 std::string_view docs,
                                 const operator_factory_plugin* fallback_format,
                                 diagnostic_handler& dh)
  -> failure_or<compression_and_format> {
  using traits = from_to_trait<is_loading>;
  auto [found_compression_plugin, found_compression_extensions,
        all_compression_extensions]
    = find_plugin<traits::compression_properties_getter,
                  traits::compression_properties_range_member>(extension);
  if (found_compression_plugin) {
    extension.remove_suffix(found_compression_extensions.size() + 1);
  }
  auto [found_rw_plugin, found_rw_extension, all_rw_extensions]
    = find_plugin<traits::rw_properties_getter,
                  traits::rw_properties_range_member>(extension);
  if (found_rw_plugin) {
    return compression_and_format{found_compression_plugin, *found_rw_plugin};
  }
  if (fallback_format) {
    return compression_and_format{nullptr, *fallback_format};
  }
  std::ranges::sort(all_rw_extensions);
  auto loc = url.source;
  auto path = url.inner;
  if (loc.end - loc.begin == path.size() + 2) {
    // FIXME: This reads like it doesn't work reliably.
    const auto extension_start = path.find(extension);
    loc.begin += extension_start + 1;
    loc.end -= 1;
    if (found_compression_plugin) {
      loc.end -= found_compression_extensions.size() + 1;
    }
  }
  std::ranges::sort(all_compression_extensions);
  auto diag = diagnostic::error("no known format for extension `{}`", extension)
                .primary(loc)
                .note("supported extensions for format deduction: `{}`",
                      fmt::join(all_rw_extensions, "`, `"));
  if (found_compression_extensions.empty()) {
    diag = std::move(diag).note("supported extensions for compression "
                                "deduction: `{}`",
                                fmt::join(all_compression_extensions, "`, `"));
  }
  std::move(diag)
    .hint("you can pass a pipeline to handle compression and format")
    .docs(std::string{docs})
    .emit(dh);
  return failure::promise();
}

auto strip_scheme(ast::expression& expr, std::string_view scheme) -> void {
  auto arg = try_as<ast::constant>(expr);
  TENZIR_ASSERT(arg);
  auto loc = arg->get_location();
  auto strip_size = scheme.size() + 3;
  match(
    arg->value,
    [strip_size, arg, loc](std::string& s) {
      if (s.size() == loc.end - loc.begin) {
        // remove the quotes and scheme from the location
        arg->source.begin += 1 + strip_size;
        // remove the quotes from the location
        arg->source.end -= 1;
      }
      s.erase(0, strip_size);
    },
    [](const auto&) {
      TENZIR_UNREACHABLE();
    });
}

auto get_as_located_string(const ast::expression& expr)
  -> located<std::string> {
  auto arg = try_as<ast::constant>(expr);
  TENZIR_ASSERT(arg);
  auto loc = arg->get_location();
  return match(
    arg->value,
    [loc](const std::string& s) {
      return located{s, loc};
    },
    [](const auto&) -> located<std::string> {
      TENZIR_UNREACHABLE();
    });
}

auto strip_prefix(std::string name) -> std::string {
  constexpr auto prefix = std::string_view{"tql2."};
  if (name.starts_with(prefix)) {
    return std::move(name).substr(prefix.size());
  }
  return name;
}

auto entity_for_plugin(const plugin& plugin,
                       location location = location::unknown) -> ast::entity {
  auto name = strip_prefix(plugin.name());
  auto segments = detail::split(name, "::");
  auto identifiers = std::vector<ast::identifier>{};
  for (auto& segment : segments) {
    identifiers.emplace_back(
      segment, &segment == &segments.back() ? location : location::unknown);
  }
  return ast::entity{std::move(identifiers)};
}

auto invocation_for_plugin(const plugin& plugin, location location
                                                 = location::unknown)
  -> ast::invocation {
  return ast::invocation{entity_for_plugin(plugin, location), {}};
}

auto make_operator(const operator_factory_plugin& plugin, location location,
                   session ctx) -> failure_or<operator_ptr> {
  auto inv = invocation_for_plugin(plugin, location);
  return plugin.make({std::move(inv.op), std::move(inv.args)}, ctx);
}

auto get_file(const boost::urls::url_view& url) -> std::string {
  if (not url.segments().empty()) {
    return url.segments().back();
  }
  if (url.host_type() == boost::urls::host_type::name) {
    return url.host();
  }
  return {};
}

template <bool is_loading>
auto get_compression_and_format(located<std::string_view> url,
                                const operator_factory_plugin* default_format,
                                std::string_view docs, diagnostic_handler& dh)
  -> failure_or<compression_and_format> {
  auto file = get_file(url.inner);
  if (file.empty()) {
    if (default_format) {
      return compression_and_format{nullptr, *default_format};
    }
    diagnostic::error("URL has no segments to deduce a format")
      .primary(url)
      .hint("you can pass a pipeline to handle compression and format")
      .emit(dh);
    return failure::promise();
  }
  auto filename_loc = url.source;
  if (filename_loc.end - filename_loc.begin == url.inner.size() + 2) {
    // FIXME: This reads like it doesn't work reliably.
    auto file_start = url.inner.find(file);
    filename_loc.begin += file_start + 1;
    filename_loc.end -= 1;
  }
  auto first_dot = file.find('.');
  if (first_dot == std::string::npos) {
    if (default_format) {
      return compression_and_format{nullptr, *default_format};
    }
    diagnostic::error("did not find extension in `{}`", file)
      .primary(filename_loc)
      .hint("you can pass a pipeline to handle compression and format")
      .emit(dh);
    return failure::promise();
  }
  auto file_ending = std::string_view{file}.substr(first_dot);
  return find_compression_and_format<is_loading>(file_ending, url, docs,
                                                 default_format, dh);
};

template <bool is_loading>
auto create_pipeline_from_uri(std::string path,
                              operator_factory_plugin::invocation inv,
                              session ctx, const char* docs)
  -> failure_or<operator_ptr> {
  using traits = from_to_trait<is_loading>;
  /// We do this to make our lives easier in the code below
  inv.args.front() = ast::constant{path, inv.args.front().get_location()};
  const operator_factory_plugin* io_plugin = nullptr;
  const operator_factory_plugin* compression_plugin = nullptr;
  const operator_factory_plugin* rw_plugin = nullptr;
  auto io_properties = typename traits::io_properties_t{};
  auto rw_properties = typename traits::rw_properties_t{};
  const auto pipeline_count
    = std::ranges::count_if(inv.args, [](const ast::expression& expr) {
        return is<ast::pipeline_expr>(expr);
      });
  if (pipeline_count > 1) {
    diagnostic::error("`{}` accepts at most one pipeline",
                      traits::operator_name)
      .primary(inv.self)
      .emit(ctx);
    return failure::promise();
  }
  auto pipeline_argument = try_as<ast::pipeline_expr>(inv.args.back());
  if (pipeline_count > 0) {
    auto it = std::ranges::find_if(inv.args, [](const ast::expression& expr) {
      return is<ast::pipeline_expr>(expr);
    });
    if (it != std::prev(inv.args.end())) {
      diagnostic::error("pipeline must be the last argument")
        .primary(it->get_location())
        .secondary(inv.args.back().get_location())
        .emit(ctx);
      return failure::promise();
    }
  }
  auto url = boost::urls::parse_uri_reference(path);
  if (not url) {
    diagnostic::error("invalid URI `{}`", path)
      .primary(inv.args.front().get_location(), url.error().message())
      .emit(ctx);
    return failure::promise();
  }
  // determine loader based on schema
  if (url->has_scheme()) {
    std::tie(io_plugin, io_properties) = find_connector_given<is_loading>(
      url->scheme(), path, inv.args.front().get_location(), docs, ctx);
    if (io_plugin) {
      if (io_properties.strip_scheme) {
        strip_scheme(inv.args.front(), url->scheme());
      }
      if (io_properties.transform_uri) {
        TRY(auto uri_replacement,
            io_properties.transform_uri(get_as_located_string(inv.args.front()),
                                        ctx));
        TENZIR_TRACE("{} operator: URI replacement size  : {}",
                     traits::operator_name, uri_replacement.size());
        TENZIR_ASSERT(not uri_replacement.empty());
        inv.args.erase(inv.args.begin());
        inv.args.insert(inv.args.begin(), uri_replacement.begin(),
                        uri_replacement.end());
        if (pipeline_argument) {
          pipeline_argument = try_as<ast::pipeline_expr>(inv.args.back());
        }
      }
    } else {
      return failure::promise();
    }
  } else {
    io_plugin
      = plugins::find<operator_factory_plugin>(traits::default_io_operator);
  }
  const bool has_pipeline_or_events = pipeline_argument or io_properties.events;
  if (not has_pipeline_or_events) {
    TRY(auto compression_and_format,
        get_compression_and_format<is_loading>(
          {path, inv.args.front().get_location()}, io_properties.default_format,
          docs, ctx));
    compression_plugin = compression_and_format.compression;
    rw_plugin = &compression_and_format.format.get();
  }
  TENZIR_TRACE("{} operator: given pipeline size   : {}", traits::operator_name,
               pipeline_argument
                 ? static_cast<int>(pipeline_argument->inner.body.size())
                 : -1);
  TENZIR_TRACE("{} operator: determined loader     : {}", traits::operator_name,
               io_plugin ? io_plugin->name() : "none");
  TENZIR_TRACE("{} operator: loader accepts pipe   : {}", traits::operator_name,
               io_plugin ? io_properties.accepts_pipeline : false);
  TENZIR_TRACE("{} operator: loader produces events: {}", traits::operator_name,
               io_plugin ? io_properties.events : false);
  TENZIR_TRACE("{} operator: determined decompress : {}", traits::operator_name,
               compression_plugin ? compression_plugin->name() : "none");
  TENZIR_TRACE("{} operator: determined read       : {}", traits::operator_name,
               rw_plugin ? rw_plugin->name() : "none");
  if (not io_plugin) {
    return failure::promise();
  }
  if (not rw_plugin and not has_pipeline_or_events) {
    if (io_properties.default_format) {
      rw_plugin = io_properties.default_format;
      TENZIR_TRACE("{} operator: fallback read         : {}",
                   traits::operator_name, rw_plugin->name());
    } else {
      return failure::promise();
    }
  }
  if (not has_pipeline_or_events) {
    inv.args.emplace_back(ast::pipeline_expr{});
    pipeline_argument = try_as<ast::pipeline_expr>(inv.args.back());
    if constexpr (not is_loading) {
      pipeline_argument->inner.body.emplace_back(
        invocation_for_plugin(*rw_plugin));
    }
    if (compression_plugin) {
      pipeline_argument->inner.body.emplace_back(
        invocation_for_plugin(*compression_plugin));
    }
    if constexpr (is_loading) {
      pipeline_argument->inner.body.emplace_back(
        invocation_for_plugin(*rw_plugin));
    }
    TENZIR_ASSERT(resolve_entities(pipeline_argument->inner, ctx));
  }
  TENZIR_TRACE("{} operator: final pipeline        :", traits::operator_name);
  for (auto& arg : inv.args) {
    TENZIR_TRACE("    {:?}", arg);
  }
  if (io_properties.accepts_pipeline) {
    return io_plugin->make(std::move(inv), std::move(ctx));
  } else {
    auto compiled_pipeline = pipeline{};
    if (pipeline_argument) {
      TRY(compiled_pipeline, compile(std::move(pipeline_argument->inner), ctx));
      TENZIR_TRACE("{} operator: compiled pipeline ops : {}",
                   traits::operator_name, compiled_pipeline.operators().size());
      inv.args.pop_back();
    }
    TRY(auto io_op, io_plugin->make(std::move(inv), ctx));
    if constexpr (is_loading) {
      compiled_pipeline.prepend(std::move(io_op));
    } else {
      compiled_pipeline.append(std::move(io_op));
    }
    return std::make_unique<pipeline>(std::move(compiled_pipeline));
  }
}

class from_plugin2 final : public virtual operator_factory_plugin {
public:
  constexpr static auto docs
    = "https://docs.tenzir.com/reference/operators/from";
  auto name() const -> std::string override {
    return "tql2.from";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    if (inv.args.empty()) {
      diagnostic::error("expected positional argument `uri|events`")
        .primary(inv.self)
        .docs(docs)
        .emit(ctx);
      return failure::promise();
    }
    auto& expr = inv.args[0];
    auto events = std::vector<ast::expression>{};
    TRY(auto value, const_eval(expr, ctx));
    using ret = std::variant<bool, failure_or<operator_ptr>>;
    auto result = match(
      value,
      [&](const record&) -> ret {
        events.push_back(expr);
        return true;
      },
      [&](const std::string& path) -> ret {
        return create_pipeline_from_uri<true>(path, std::move(inv), ctx, docs);
      },
      [&](const auto&) -> ret {
        const auto t = type::infer(value);
        diagnostic::error("expected `string`, or `record`")
          .primary(expr, "got `{}`", t ? t->kind() : type_kind{})
          .docs(docs)
          .emit(ctx);
        return failure::promise();
      });
    if (auto* op = try_as<failure_or<operator_ptr>>(result)) {
      return std::move(*op);
    }
    if (not as<bool>(result)) {
      return failure::promise();
    }
    for (auto& expr : inv.args | std::views::drop(1)) {
      TRY(value, const_eval(expr, ctx));
      result = match(
        value,
        [&](const record&) -> ret {
          events.push_back(expr);
          return true;
        },
        [&](const auto&) -> ret {
          const auto t = type::infer(value);
          diagnostic::error("expected `string`, or `record`")
            .primary(expr, "got `{}`", t ? t->kind() : type_kind{})
            .docs(docs)
            .emit(ctx);
          return failure::promise();
        });
      if (auto* op = try_as<failure_or<operator_ptr>>(result)) {
        return std::move(*op);
      }
    }
    return std::make_unique<from_events>(std::move(events));
  }
};

class to_plugin2 final : public virtual operator_factory_plugin {
public:
  constexpr static auto docs = "https://docs.tenzir.com/reference/operators/to";
  auto name() const -> std::string override {
    return "tql2.to";
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    if (inv.args.empty()) {
      diagnostic::error("expected positional argument `uri`")
        .primary(inv.self)
        .docs(docs)
        .emit(ctx);
      return failure::promise();
    }
    auto& expr = inv.args[0];
    TRY(auto value, const_eval(expr, ctx));
    return match(
      value,
      [&](std::string& path) -> failure_or<operator_ptr> {
        return create_pipeline_from_uri<false>(path, std::move(inv),
                                               std::move(ctx), docs);
      },
      [&](auto&) -> failure_or<operator_ptr> {
        auto t = type::infer(value);
        diagnostic::error("expected `string`")
          .primary(inv.args[0], "got `{}`", t ? t->kind() : type_kind{})
          .docs(docs)
          .emit(ctx);
        return failure::promise();
      });
  }
};

struct chunk_source_traits {
  using signatures = caf::type_list<auto(atom::get)->caf::result<chunk_ptr>>;
};

using chunk_source_actor = caf::typed_actor<chunk_source_traits>;

class arrow_chunk_source {
public:
  explicit arrow_chunk_source(std::shared_ptr<arrow::io::InputStream> stream)
    : stream_{std::move(stream)} {
  }

  auto make_behavior() -> chunk_source_actor::behavior_type {
    return {
      [this](atom::get) -> caf::result<chunk_ptr> {
        auto buffer = stream_->Read(1 << 20);
        if (not buffer.ok()) {
          return diagnostic::error(
                   "{}", buffer.status().ToStringWithoutContextLines())
            .to_error();
        }
        return chunk::make(buffer.MoveValueUnsafe());
      },
    };
  }

private:
  std::shared_ptr<arrow::io::InputStream> stream_;
};

class from_file_source final : public crtp_operator<from_file_source> {
public:
  from_file_source() = default;

  explicit from_file_source(chunk_source_actor source)
    : source_{std::move(source)} {
    TENZIR_ASSERT(source_);
  }

  auto name() const -> std::string override {
    return "from_file_source";
  }

  auto operator()(operator_control_plane& ctrl) const -> generator<chunk_ptr> {
    while (true) {
      auto result = chunk_ptr{};
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::get_v)
        .request(source_, caf::infinite)
        .then(
          [&](chunk_ptr chunk) {
            result = std::move(chunk);
            ctrl.set_waiting(false);
          },
          [&](caf::error error) {
            diagnostic::error(std::move(error)).emit(ctrl.diagnostics());
          });
      co_yield {};
      if (not result or result->size() == 0) {
        break;
      }
      co_yield std::move(result);
    }
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, from_file_source& x) -> bool {
    return f.apply(x.source_);
  }

private:
  chunk_source_actor source_;
};

struct from_file_actor_traits {
  using signatures = caf::type_list<
    // Fetch the result of one of the subpipelines.
    auto(atom::get)->caf::result<table_slice>,
    // Provide a result from one of the subpipelines.
    auto(atom::put, table_slice)->caf::result<void>
    // Derive the metric and diagnostic handler signatures.
    >::append_from<receiver_actor<diagnostic>::signatures,
                   metrics_receiver_actor::signatures>;
};

using from_file_actor = caf::typed_actor<from_file_actor_traits>;

class from_file_sink final : public crtp_operator<from_file_sink> {
public:
  from_file_sink() = default;

  explicit from_file_sink(
    from_file_actor parent, event_order order,
    std::optional<std::pair<ast::field_path, std::string>> path_field)
    : parent_{std::move(parent)},
      order_{order},
      path_field_{std::move(path_field)} {
  }

  auto name() const -> std::string override {
    return "from_file_sink";
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    for (auto slice : input) {
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      if (path_field_) {
        slice = assign(path_field_->first,
                       data_to_series(path_field_->second, slice.rows()), slice,
                       ctrl.diagnostics());
      }
      // We wait for a response in order to get backpressure.
      ctrl.set_waiting(true);
      ctrl.self()
        .mail(atom::put_v, std::move(slice))
        .request(parent_, caf::infinite)
        .then(
          [&]() {
            ctrl.set_waiting(false);
          },
          [&](caf::error error) {
            diagnostic::error(std::move(error)).emit(ctrl.diagnostics());
          });
      co_yield {};
    }
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return optimize_result{std::nullopt, order_, copy()};
  }

  friend auto inspect(auto& f, from_file_sink& x) -> bool {
    return f.object(x).fields(f.field("parent", x.parent_),
                              f.field("order", x.order_),
                              f.field("path_field", x.path_field_));
  }

private:
  from_file_actor parent_;
  event_order order_;
  std::optional<std::pair<ast::field_path, std::string>> path_field_;
};

/// Add a callback to an `arrow::Future` that shall run inside an actor context.
template <class T, class F>
void add_actor_callback(caf::scheduled_actor* self,
                        const arrow::Future<T>& future, F&& f) {
  using result_type
    = std::conditional_t<std::same_as<T, arrow::internal::Empty>, arrow::Status,
                         arrow::Result<T>>;
  future.AddCallback(
    [self, weak = caf::weak_actor_ptr{self->ctrl()},
     f = std::forward<F>(f)](const result_type& result) mutable {
      if (auto strong = weak.lock()) {
        self->schedule_fn([f = std::move(f), result]() mutable -> void {
          return std::invoke(std::move(f), std::move(result));
        });
      }
    });
}

/// Iterate asynchronously over an `arrow::fs::FileInfoGenerator`.
template <class F>
void iterate_files(caf::scheduled_actor* self, arrow::fs::FileInfoGenerator gen,
                   F&& f) {
  auto future = gen();
  add_actor_callback(self, future,
                     [self, gen = std::move(gen), f = std::forward<F>(f)](
                       arrow::Result<arrow::fs::FileInfoVector> infos) {
                       auto more = infos.ok() and not infos->empty();
                       f(std::move(infos));
                       if (more) {
                         iterate_files(self, std::move(gen), std::move(f));
                       }
                     });
}

struct file_hasher {
  auto operator()(const arrow::fs::FileInfo& file) const -> size_t {
    return hash(file.path(), file.type(), file.size(), file.mtime());
  }
};

using file_set = boost::unordered_set<arrow::fs::FileInfo, file_hasher>;

struct from_file_args {
  located<std::string> url;
  bool watch{false};
  bool remove{false};
  std::optional<ast::field_path> path_field;
  std::optional<located<pipeline>> pipe;

  friend auto inspect(auto& f, from_file_args& x) -> bool {
    return f.object(x).fields(f.field("url", x.url), f.field("watch", x.watch),
                              f.field("remove", x.remove),
                              f.field("path_field", x.path_field),
                              f.field("pipe", x.pipe));
  }
};

class from_file_impl {
public:
  from_file_impl(from_file_actor::pointer self, from_file_args args,
                 event_order order, std::unique_ptr<diagnostic_handler> dh,
                 std::string definition, node_actor node, bool is_hidden,
                 metrics_receiver_actor metrics_receiver,
                 uint64_t operator_index)
    : self_{self},
      dh_{std::move(dh)},
      args_{std::move(args)},
      order_{order},
      definition_{std::move(definition)},
      node_{std::move(node)},
      is_hidden_{is_hidden},
      operator_index_{operator_index},
      metrics_receiver_{std::move(metrics_receiver)} {
    TENZIR_ASSERT(dh_);
    auto expanded = expand_home(args_.url.inner);
    if (not expanded.contains("://")) {
      // Arrow doesn't allow relative paths, so we make it absolute.
      expanded = std::filesystem::weakly_canonical(expanded);
    }
    // TODO: Arrow removes trailing slashes here. Do we need them?
    // TODO: Once we allow `?` in globs (which is currently not supported), we
    // run into trouble here because of `s3://bucket/a?.b?endpoint_override=`.
    auto path = std::string{};
    auto fs = arrow::fs::FileSystemFromUriOrPath(expanded, &path);
    if (not fs.ok()) {
      diagnostic::error("{}", fs.status().ToStringWithoutContextLines())
        .primary(args_.url)
        .emit(*dh_);
      self->quit(ec::silent);
      return;
    }
    fs_ = fs.MoveValueUnsafe();
    glob_ = parse_glob(path);
    root_path_ = std::invoke([&]() -> std::string {
      if (not glob_.empty()) {
        if (auto prefix = try_as<std::string>(glob_[0])) {
          // Use the whole path if we don't do any actual globbing.
          if (glob_.size() == 1) {
            return *prefix;
          }
          // Otherwise use the last directory before the globbing starts.
          auto slash = prefix->rfind("/");
          if (slash != std::string::npos) {
            // The slash itself should be included.
            return prefix->substr(0, slash + 1);
          }
        }
      }
      return "/";
    });
    query_files();
  }

  auto make_behavior() -> from_file_actor::behavior_type {
    return {
      [this](atom::get) -> caf::result<table_slice> {
        return get();
      },
      [this](atom::put, table_slice slice) -> caf::result<void> {
        return put(std::move(slice));
      },
      [this](diagnostic diag) {
        dh_->emit(std::move(diag));
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             type schema) {
        return register_metrics(nested_operator_index, nested_metrics_id,
                                std::move(schema));
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             record metrics) {
        return handle_metrics(nested_operator_index, nested_metrics_id,
                              std::move(metrics));
      },
      [](const operator_metric& metrics) {
        // Cannot forward operator metrics from nested pipelines.
        TENZIR_UNUSED(metrics);
      },
    };
  }

private:
  auto get() -> caf::result<table_slice> {
    if (puts_.empty()) {
      auto rp = self_->make_response_promise<table_slice>();
      gets_.emplace_back(rp);
      check_termination();
      return rp;
    }
    auto slice = std::move(puts_.front().first);
    puts_.front().second.deliver();
    puts_.pop_front();
    return slice;
  }

  auto put(table_slice slice) -> caf::result<void> {
    if (gets_.empty()) {
      auto rp = self_->make_response_promise<void>();
      puts_.emplace_back(std::move(slice), rp);
      return rp;
    }
    gets_.front().deliver(std::move(slice));
    gets_.pop_front();
    return {};
  }

  void query_files() {
    add_actor_callback(
      self_, fs_->GetFileInfoAsync(std::vector{root_path_}),
      [this](arrow::Result<std::vector<arrow::fs::FileInfo>> infos) {
        if (not infos.ok()) {
          diagnostic::error("{}", infos.status().ToStringWithoutContextLines())
            .primary(args_.url)
            .emit(*dh_);
          return;
        }
        TENZIR_ASSERT(infos->size() == 1);
        auto root_info = std::move((*infos)[0]);
        switch (root_info.type()) {
          case arrow::fs::FileType::NotFound:
            // We only want to allow this if `watch=true`.
            if (args_.watch) {
              got_all_files();
            } else {
              diagnostic::error("`{}` does not exist", root_path_)
                .primary(args_.url)
                .emit(*dh_);
            }
            return;
          case arrow::fs::FileType::Unknown:
            diagnostic::error("`{}` is unknown", root_path_)
              .primary(args_.url)
              .emit(*dh_);
            return;
          case arrow::fs::FileType::File:
            if (matches(root_info.path(), glob_)) {
              add_job(std::move(root_info));
              got_all_files();
            } else if (not args_.watch) {
              diagnostic::error("`{}` is a file, not a directory", root_path_)
                .primary(args_.url)
                .emit(*dh_);
            }
            return;
          case arrow::fs::FileType::Directory:
            auto sel = arrow::fs::FileSelector{};
            sel.base_dir = root_path_;
            sel.recursive = true;
            auto gen = fs_->GetFileInfoGenerator(sel);
            iterate_files(
              self_, std::move(gen),
              [this](arrow::Result<arrow::fs::FileInfoVector> files) {
                if (not files.ok()) {
                  diagnostic::error(
                    "{}", files.status().ToStringWithoutContextLines())
                    .primary(args_.url)
                    .emit(*dh_);
                  return;
                }
                if (files->empty()) {
                  got_all_files();
                  return;
                }
                for (auto& file : *files) {
                  process_file(std::move(file));
                }
              });
            return;
        }
        TENZIR_UNREACHABLE();
      });
  }

  void process_file(arrow::fs::FileInfo file) {
    if (file.IsFile() and matches(file.path(), glob_)) {
      add_job(std::move(file));
    }
  }

  void got_all_files() {
    if (args_.watch) {
      std::swap(previous_, current_);
      current_.clear();
      self_->run_delayed_weak(watch_pause, [this] {
        query_files();
      });
    } else {
      added_all_jobs_ = true;
      check_termination();
    }
  }

  void check_termination() {
    if (added_all_jobs_ and jobs_.empty() and active_jobs_ == 0) {
      for (auto& get : gets_) {
        // If there are any unmatched gets, we know that there are no puts.
        get.deliver(table_slice{});
      }
    }
  }

  void check_jobs() {
    while (not jobs_.empty() and active_jobs_ < max_jobs) {
      start_job(jobs_.front());
      jobs_.pop_front();
    }
  }

  void check_jobs_and_termination() {
    check_jobs();
    check_termination();
  }

  void add_job(arrow::fs::FileInfo file) {
    auto inserted = current_.emplace(file).second;
    TENZIR_ASSERT(inserted);
    if (previous_.contains(file)) {
      return;
    }
    jobs_.push_back(std::move(file));
    check_jobs();
  }

  auto make_pipeline(std::string_view path) -> failure_or<pipeline> {
    if (args_.pipe) {
      return args_.pipe->inner;
    }
    auto parse_dh = transforming_diagnostic_handler{
      *dh_, [this, path](diagnostic d) {
        if (is_globbing()) {
          d.severity = severity::warning;
        }
        return std::move(d)
          .modify()
          .note(fmt::format("coming from `{}`", path))
          .done();
      }};
    TRY(auto compression_and_format,
        get_compression_and_format<true>(
          located<std::string_view>{path, args_.url.source}, nullptr,
          "https://docs.tenzir.com/reference/operators/from_file", parse_dh));
    auto& format = compression_and_format.format.get();
    auto compression = compression_and_format.compression;
    auto provider = session_provider::make(parse_dh);
    auto ctx = provider.as_session();
    auto pipe = pipeline{};
    if (compression) {
      TRY(auto decompress, make_operator(*compression, args_.url.source, ctx));
      pipe.append(std::move(decompress));
    }
    TRY(auto read, make_operator(format, args_.url.source, ctx));
    pipe.append(std::move(read));
    return pipe;
  }

  void start_job(const arrow::fs::FileInfo& file) {
    TENZIR_ASSERT(active_jobs_ < max_jobs);
    active_jobs_ += 1;
    auto pipe = make_pipeline(file.path());
    if (pipe.is_error()) {
      active_jobs_ -= 1;
      return;
    }
    // We already checked the output type after parsing.
    auto output_type = pipe->infer_type<chunk_ptr>();
    TENZIR_ASSERT(output_type);
    TENZIR_ASSERT(output_type->is<table_slice>());
    add_actor_callback(
      self_, fs_->OpenInputStreamAsync(file),
      [this, pipe = std::move(*pipe), path = file.path()](
        arrow::Result<std::shared_ptr<arrow::io::InputStream>> stream) mutable {
        start_stream(std::move(stream), std::move(pipe), std::move(path));
      });
  }

  void
  start_stream(arrow::Result<std::shared_ptr<arrow::io::InputStream>> stream,
               pipeline pipe, std::string path) {
    if (not stream.ok()) {
      pipeline_failed("failed to open `{}`", path)
        .primary(args_.url)
        .note(stream.status().ToStringWithoutContextLines())
        .emit(*dh_);
      active_jobs_ -= 1;
      return;
    }
    auto source = self_->spawn(caf::actor_from_state<arrow_chunk_source>,
                               std::move(*stream));
    auto weak = caf::weak_actor_ptr{source->ctrl()};
    pipe.prepend(std::make_unique<from_file_source>(std::move(source)));
    pipe.append(std::make_unique<from_file_sink>(
      self_, order_,
      args_.path_field ? std::optional{std::pair{*args_.path_field, path}}
                       : std::nullopt));
    pipe = pipe.optimize_if_closed();
    auto executor
      = self_->spawn(pipeline_executor, std::move(pipe), definition_, self_,
                     self_, node_, false, is_hidden_);
    self_->attach_functor([this, weak]() {
      if (auto strong = weak.lock()) {
        // FIXME: This should not be necessary to ensure that the actor is
        // destroyed when the executor is done. This problem could also
        // apply to other operators.
        self_->send_exit(strong, caf::exit_reason::user_shutdown);
      }
    });
    self_->monitor(executor, [this, executor, path,
                              weak = std::move(weak)](caf::error error) {
      if (auto strong = weak.lock()) {
        // FIXME: This should not be necessary to ensure that the actor is
        // destroyed when the executor is done. This problem could also
        // apply to other operators.
        self_->send_exit(strong, caf::exit_reason::user_shutdown);
      }
      active_jobs_ -= 1;
      if (error) {
        pipeline_failed(std::move(error))
          .note("coming from `{}`", path)
          .emit(*dh_);
        return;
      }
      if (args_.remove) {
        // There is no async call available.
        auto status = fs_->DeleteFile(path);
        if (not status.ok()) {
          diagnostic::warning("failed to remove `{}`", path)
            .primary(args_.url)
            .note(status.ToStringWithoutContextLines())
            .emit(*dh_);
        }
      }
      check_jobs_and_termination();
    });
    self_->mail(atom::start_v)
      .request(executor, caf::infinite)
      .then([] {},
            [this, path = std::move(path)](caf::error error) {
              pipeline_failed(std::move(error))
                .note("coming from `{}`", path)
                .emit(*dh_);
            });
  }

  auto register_metrics(uint64_t nested_operator_index, uuid nested_metrics_id,
                        type schema) -> caf::result<void> {
    (void)nested_operator_index;
    return self_->mail(operator_index_, nested_metrics_id, std::move(schema))
      .delegate(metrics_receiver_);
  }

  auto handle_metrics(uint64_t nested_operator_index, uuid nested_metrics_id,
                      record metrics) -> caf::result<void> {
    (void)nested_operator_index;
    return self_->mail(operator_index_, nested_metrics_id, std::move(metrics))
      .delegate(metrics_receiver_);
  }

  auto pipeline_failed(caf::error error) const -> diagnostic_builder {
    if (is_globbing()) {
      return diagnostic::warning(std::move(error));
    }
    return diagnostic::error(std::move(error));
  }

  template <class... Ts>
  auto pipeline_failed(fmt::format_string<Ts...> str, Ts&&... xs) const
    -> diagnostic_builder {
    if (is_globbing()) {
      return diagnostic::warning(std::move(str), std::forward<Ts>(xs)...);
    }
    return diagnostic::error(std::move(str), std::forward<Ts>(xs)...);
  }

  auto is_globbing() const -> bool {
    return glob_.size() != 1 or not is<std::string>(glob_[0]);
  }

  from_file_actor::pointer self_;
  std::unique_ptr<diagnostic_handler> dh_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;

  // The configuration and things derived from it.
  from_file_args args_;
  event_order order_;
  glob glob_;
  std::string root_path_;

  // Watching is implemented by checking against the files seen previously.
  file_set previous_;
  file_set current_;

  // Communication with the operator bridges.
  std::deque<caf::typed_response_promise<table_slice>> gets_;
  std::deque<std::pair<table_slice, caf::typed_response_promise<void>>> puts_;

  // Information needed for spawning subpipelines.
  std::string definition_;
  node_actor node_;
  bool is_hidden_;

  // Job management.
  size_t active_jobs_ = 0;
  std::deque<arrow::fs::FileInfo> jobs_;
  bool added_all_jobs_ = false;

  // Forwarding metrics.
  uint64_t operator_index_ = 0;
  metrics_receiver_actor metrics_receiver_;
};

class from_file final : public crtp_operator<from_file> {
public:
  from_file() = default;

  explicit from_file(from_file_args args) : args_{std::move(args)} {
  }

  auto name() const -> std::string override {
    return "from_file";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    // Spawning the actor detached because some parts of the Arrow filesystem
    // API are blocking.
    auto impl = scope_linked{ctrl.self().spawn<caf::linked + caf::detached>(
      caf::actor_from_state<from_file_impl>, args_, order_,
      std::make_unique<shared_diagnostic_handler>(ctrl.shared_diagnostics()),
      std::string{ctrl.definition()}, ctrl.node(), ctrl.is_hidden(),
      ctrl.metrics_receiver(), ctrl.operator_index())};
    while (true) {
      auto result = table_slice{};
      ctrl.self()
        .mail(atom::get_v)
        .request(impl.get(), caf::infinite)
        .then(
          [&](table_slice slice) {
            result = std::move(slice);
            ctrl.set_waiting(false);
          },
          [&](caf::error error) {
            diagnostic::error(std::move(error)).emit(ctrl.diagnostics());
          });
      ctrl.set_waiting(true);
      co_yield {};
      if (result.rows() == 0) {
        break;
      }
      co_yield std::move(result);
    }
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    auto copy = std::make_unique<from_file>(*this);
    copy->order_ = order;
    return optimize_result{std::nullopt, event_order::ordered, std::move(copy)};
  }

  friend auto inspect(auto& f, from_file& x) -> bool {
    return f.object(x).fields(f.field("args", x.args_),
                              f.field("order", x.order_));
  }

private:
  from_file_args args_;
  event_order order_{event_order::ordered};
};

class from_file_plugin : public virtual operator_plugin2<from_file> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto args = from_file_args{};
    auto parser = argument_parser2::operator_(name());
    parser.positional("url", args.url)
      .named_optional("watch", args.watch)
      .named_optional("remove", args.remove)
      .named("path_field", args.path_field)
      .positional("{ … }", args.pipe);
    TRY(parser.parse(inv, ctx));
    if (args.pipe) {
      auto output_type = args.pipe->inner.infer_type<chunk_ptr>();
      if (not output_type) {
        diagnostic::error("pipeline must accept bytes")
          .primary(*args.pipe)
          .docs(parser.docs())
          .emit(ctx);
        return failure::promise();
      }
      if (output_type->is_not<table_slice>()) {
        diagnostic::error("pipeline must return events")
          .primary(*args.pipe)
          .docs(parser.docs())
          .emit(ctx);
        return failure::promise();
      }
    }
    return std::make_unique<from_file>(std::move(args));
  }
};

} // namespace

} // namespace tenzir::plugins::from

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_events_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::to_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_file_plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::operator_inspection_plugin<tenzir::plugins::from::from_file_source>);
TENZIR_REGISTER_PLUGIN(
  tenzir::operator_inspection_plugin<tenzir::plugins::from::from_file_sink>);
