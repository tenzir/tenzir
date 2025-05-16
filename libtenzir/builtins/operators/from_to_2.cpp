//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_caf.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/glob.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/pipeline_executor.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/prepend_token.hpp>
#include <tenzir/tql/fwd.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/eval_impl.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>
#include <tenzir/tql2/set.hpp>

#include <arrow/filesystem/api.h>
#include <arrow/util/uri.h>
#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <caf/actor_from_state.hpp>
#include <caf/mail_cache.hpp>

#include <ranges>

namespace tenzir::plugins::from {
namespace {

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
    // TODO: Doesn't reads like it doesn't work reliably.
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
  // TODO: Figure out what to do here.
  auto filename_loc = url;
  // auto filename_loc = inv.args.front().get_location();
  // if (filename_loc.end - filename_loc.begin == path.size() + 2) {
  //   auto file_start = path.find(file);
  //   filename_loc.begin += file_start + 1;
  //   filename_loc.end -= 1;
  // }
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
    auto io_ent = ast::entity{std::vector{
      ast::identifier{strip_prefix(rw_plugin->name()), location::unknown},
    }};
    if constexpr (not is_loading) {
      pipeline_argument->inner.body.emplace_back(
        ast::invocation{std::move(io_ent), {}});
    }
    if (compression_plugin) {
      auto compression_ent = ast::entity{std::vector{
        ast::identifier{strip_prefix(compression_plugin->name()),
                        location::unknown},
      }};
      pipeline_argument->inner.body.emplace_back(
        ast::invocation{std::move(compression_ent), {}});
    }
    if constexpr (is_loading) {
      pipeline_argument->inner.body.emplace_back(
        ast::invocation{std::move(io_ent), {}});
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
  constexpr static auto docs = "https://docs.tenzir.com/tql2/operators/from";
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
  constexpr static auto docs = "https://docs.tenzir.com/tql2/operators/to";
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

using source_actor = caf::typed_actor<auto(atom::get)->caf::result<chunk_ptr>>;

class arrow_fs_source {
public:
  explicit arrow_fs_source(std::shared_ptr<arrow::io::InputStream> stream)
    : stream_{std::move(stream)} {
  }

  auto make_behavior() -> source_actor::behavior_type {
    return {
      [this](atom::get) -> caf::result<chunk_ptr> {
        auto buffer = stream_->Read(1 << 20);
        if (not buffer.ok()) {
          return diagnostic::error("TODO").to_error();
        }
        // TENZIR_WARN("sending chunk");
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

  explicit from_file_source(source_actor source) : source_{std::move(source)} {
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
          [&](caf::error) {
            diagnostic::error("TODO").emit(ctrl.diagnostics());
          });
      co_yield {};
      if (not result or result->size() == 0) {
        TENZIR_WARN("from_file_source is done");
        break;
      }
      // TENZIR_WARN("got chunk: {}", result->size());
      co_yield std::move(result);
    }
    // arrow::fs::GcsFileSystem::Make();
    // auto s = std::shared_ptr<arrow::io::InputStream>{};
    // // TODO: This is blocking!
    // auto buffer = s->Read(1 << 20);
    // if (not buffer.ok()) {
    //   diagnostic::error("{}", buffer.status().ToStringWithoutContextLines())
    //     .emit(ctrl.diagnostics());
    //   co_return;
    // }
    // co_yield chunk::make(buffer.MoveValueUnsafe());
    // co_return;
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
  source_actor source_;
};

struct from_file_actor_traits {
  using signatures = caf::type_list<
    //
    auto(atom::get)->caf::result<table_slice>,
    //
    auto(atom::put, table_slice)->caf::result<void>
    // Derive the other signatures.
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
          [](caf::error error) {
            TENZIR_TODO();
          });
      co_yield {};
    }
    TENZIR_WARN("from_file_sink is done");
  }

  auto optimize(const expression& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return optimize_result{std::nullopt, order_, copy()};
  }

  friend auto inspect(auto& f, from_file_sink& x) -> bool {
    return f.object(x).fields(f.field("parent", x.parent_),
                              f.field("order", x.order_));
  }

private:
  from_file_actor parent_;
  event_order order_;
  std::optional<std::pair<ast::field_path, std::string>> path_field_;
};

class from_file_impl {
public:
  from_file_impl(from_file_actor::pointer self,
                 std::unique_ptr<diagnostic_handler> dh,
                 located<std::string> url,
                 std::optional<ast::field_path> path_field,
                 std::string definition, node_actor node, bool is_hidden,
                 event_order order, std::optional<located<pipeline>> pipe)
    : self_{self},
      url_{std::move(url)},
      path_field_{std::move(path_field)},
      dh_{std::move(dh)},
      definition_{std::move(definition)},
      node_{std::move(node)},
      is_hidden_{is_hidden},
      io_executor_{self},
      io_ctx_{arrow::default_memory_pool(), &io_executor_},
      order_{order},
      pipe_{std::move(pipe)} {
    TENZIR_ASSERT(dh_);
    auto path = std::string{};
    // TODO: Relative local-filesystem paths.
    // TODO: Arrow removes trailing slashes here.
    auto fs = arrow::fs::FileSystemFromUriOrPath(url_.inner, io_ctx_, &path);
    if (not fs.ok()) {
      diagnostic::error("{}", fs.status().ToStringWithoutContextLines())
        .emit(*dh_);
      self->quit(ec::silent);
      return;
    }
    fs_ = fs.MoveValueUnsafe();
    auto glob = parse_glob(path);
    // TODO: Figure out the proper logic here.
    if (auto star = path.find('*'); star != std::string::npos) {
      auto slash = path.rfind('/', star);
      TENZIR_ASSERT(slash != std::string::npos);
      path = path.substr(0, slash + 1);
    }
    // We intentionally define the lambda in the scope of the generator to make
    // sure that we do not capture anything that doesn't survive.
    auto process
      = [this, glob = std::move(glob)](arrow::fs::FileInfoVector infos) {
          if (infos.empty()) {
            // TODO
            TENZIR_WARN("got all file infos");
            return;
          }
          for (auto& info : infos) {
            if (not matches(info.path(), glob)) {
              continue;
            }
            TENZIR_WARN("{}", info.path());
            add_job(std::move(info));
          }
        };
    TENZIR_WARN("hello?");
    fs_->GetFileInfoAsync(std::vector{path})
      .AddCallback([this, process, path = std::move(path)](
                     arrow::Result<std::vector<arrow::fs::FileInfo>> infos) {
        // TODO: Improve diagnostics.
        if (not infos.ok()) {
          diagnostic::error("{}", infos.status().ToStringWithoutContextLines())
            .emit(*dh_);
          return;
        }
        TENZIR_ASSERT(infos->size() == 1);
        auto root_info = std::move((*infos)[0]);
        TENZIR_WARN("got root info: {}", root_info.path());
        switch (root_info.type()) {
          case arrow::fs::FileType::NotFound:
            diagnostic::error("`{}` does not exist", url_.inner).emit(*dh_);
            return;
          case arrow::fs::FileType::Unknown:
            diagnostic::error("`{}` is unknown", url_.inner).emit(*dh_);
            return;
          case arrow::fs::FileType::File:
            // TODO: What do we do?
            diagnostic::error("`{}` is file", url_.inner).emit(*dh_);
            return;
          case arrow::fs::FileType::Directory:
            auto sel = arrow::fs::FileSelector{};
            sel.base_dir = path;
            sel.recursive = true;
            auto gen = fs_->GetFileInfoGenerator(sel);
            async_iter(std::move(gen), std::move(process));
            return;
        }
        TENZIR_UNREACHABLE();
      });
  }

  auto make_behavior() -> from_file_actor::behavior_type {
    return {
      [this](atom::get) -> caf::result<table_slice> {
        if (puts_.empty()) {
          auto rp = self_->make_response_promise<table_slice>();
          gets_.emplace_back(rp);
          return rp;
        }
        auto slice = std::move(puts_.front().first);
        puts_.front().second.deliver();
        puts_.pop_front();
        return slice;
      },
      [this](atom::put, table_slice slice) -> caf::result<void> {
        if (gets_.empty()) {
          auto rp = self_->make_response_promise<void>();
          puts_.emplace_back(std::move(slice), rp);
          return rp;
        }
        gets_.front().deliver(std::move(slice));
        gets_.pop_front();
        return {};
      },
      [this](diagnostic diag) {
        TENZIR_WARN("{:#?}", diag);
      },
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             type schema) {},
      [this](uint64_t nested_operator_index, uuid nested_metrics_id,
             record metrics) {},
      [](const operator_metric& metrics) {
        // Cannot forward operator metrics from nested pipelines.
        TENZIR_UNUSED(metrics);
      },
    };
  }

private:
  void add_job(arrow::fs::FileInfo file) {
    jobs_.push_back(std::move(file));
    check_jobs();
  }

  void check_jobs() {
    if (remaining_jobs_ == 0) {
      return;
    }
    spawn_job(std::move(jobs_.front()));
    jobs_.pop_front();
  }

  auto make_pipeline(std::string_view path) -> failure_or<pipeline> {
    if (pipe_) {
      return pipe_->inner;
    }
    auto dh = collecting_diagnostic_handler{};
    TRY(auto compression_and_format,
        get_compression_and_format<true>(
          located<std::string_view>{path, url_.source}, nullptr,
          "https://docs.tenzir.com/operators/from_file", dh));
    // TODO
    TENZIR_ASSERT(dh.empty());
    auto& format = compression_and_format.format.get();
    auto compression = compression_and_format.compression;
    auto provider = session_provider::make(dh);
    auto ctx = provider.as_session();
    // TODO: This is not great.
    auto inv = operator_factory_plugin::invocation{
      ast::entity{{ast::identifier{format.name(), url_.source}}}, {}};
    // TODO: No unwrap.
    auto pipe = pipeline{};
    if (compression) {
      pipe.append(compression->make(inv, ctx).unwrap());
    }
    pipe.append(format.make(inv, ctx).unwrap());
    // TODO
    TENZIR_ASSERT(dh.empty());
    return pipe;
  }

  void spawn_job(arrow::fs::FileInfo file) {
    TENZIR_ASSERT(remaining_jobs_ > 0);
    remaining_jobs_ -= 1;
    auto pipe = make_pipeline(file.path());
    if (pipe.is_error()) {
      TENZIR_TODO();
    }
    auto output_type = pipe->infer_type<chunk_ptr>();
    TENZIR_ASSERT(output_type);
    TENZIR_ASSERT(output_type->is<table_slice>());
    // TODO: Wait for this?
    fs_->OpenInputStreamAsync(file).AddCallback(
      [this, pipe = std::move(*pipe), path = file.path()](
        arrow::Result<std::shared_ptr<arrow::io::InputStream>> stream) mutable {
        auto source = self_->spawn(caf::actor_from_state<arrow_fs_source>,
                                   std::move(*stream));
        pipe.prepend(std::make_unique<from_file_source>(std::move(source)));
        pipe.append(std::make_unique<from_file_sink>(
          self_, order_,
          path_field_ ? std::optional{std::pair{*path_field_, path}}
                      : std::nullopt));
        // TODO: Make sure it quits when we quit.
        pipe = pipe.optimize_if_closed();
        TENZIR_WARN("pipe = {:#?}", pipe);
        auto executor
          = self_->spawn(pipeline_executor, std::move(pipe), definition_, self_,
                         self_, node_, false, is_hidden_);
        self_->monitor(executor, [executor](caf::error yo) {
          // TODO: Do we know here that we got all data from our sink?
          // Probably not!
          TENZIR_WARN("EXIT");
        });
        self_->mail(atom::start_v)
          .request(executor, caf::infinite)
          .then(
            [] {
              TENZIR_WARN("oh yes");
            },
            [](caf::error error) {
              TENZIR_WARN("oh no: {}", error);
            });
        // TODO: Get rid of this?
        // executors_.push_back(std::move(executor));
      });
  }

  from_file_actor::pointer self_;

  // TODO
  std::unique_ptr<diagnostic_handler> dh_;
  event_order order_;
  std::optional<located<pipeline>> pipe_;

  std::deque<caf::typed_response_promise<table_slice>> gets_;
  std::deque<std::pair<table_slice, caf::typed_response_promise<void>>> puts_;

  // configuration
  located<std::string> url_;
  std::optional<ast::field_path> path_field_;

  // stuff for spawning subpipelines
  std::string definition_;
  node_actor node_;
  bool is_hidden_;

  // stuff for running subpipelines
  size_t remaining_jobs_ = 10;
  std::deque<arrow::fs::FileInfo> jobs_;
  // std::vector<pipeline_executor_actor> executors_;

  // arrow stuff
  caf_executor io_executor_;
  arrow::io::IOContext io_ctx_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;
};

class from_file final : public crtp_operator<from_file> {
public:
  from_file() = default;

  explicit from_file(located<std::string> url,
                     std::optional<ast::field_path> path_field,
                     std::optional<located<pipeline>> pipe)
    : url_{std::move(url)},
      path_field_{std::move(path_field)},
      pipe_{std::move(pipe)} {
  }

  auto name() const -> std::string override {
    return "from_file";
  }

  auto operator()(operator_control_plane& ctrl) const
    -> generator<table_slice> {
    auto impl = ctrl.self().spawn(
      caf::actor_from_state<from_file_impl>,
      std::make_unique<shared_diagnostic_handler>(ctrl.shared_diagnostics()),
      url_, path_field_, std::string{ctrl.definition()}, ctrl.node(),
      ctrl.is_hidden(), order_, pipe_);
    while (true) {
      auto result = table_slice{};
      ctrl.self()
        .mail(atom::get_v)
        .request(impl, caf::infinite)
        .then(
          [&](table_slice slice) {
            result = std::move(slice);
            ctrl.set_waiting(false);
          },
          [&](caf::error error) {
            TENZIR_TODO();
          });
      ctrl.set_waiting(true);
      co_yield {};
      if (result.rows() == 0) {
        TENZIR_WARN("ending from_file because empty slice");
        break;
      }
      TENZIR_WARN("got slice: {}", result.rows());
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
    return f.object(x).fields(f.field("url", x.url_), f.field("pipe", x.pipe_),
                              f.field("order", x.order_));
  }

private:
  located<std::string> url_;
  std::optional<ast::field_path> path_field_;
  std::optional<located<pipeline>> pipe_;
  event_order order_ = event_order::ordered;
};

class from_file_plugin : public virtual operator_plugin2<from_file> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    auto url = located<std::string>{};
    auto pipe = std::optional<located<pipeline>>{};
    auto watch = false;
    auto path_field = std::optional<ast::field_path>{};
    // from_file "<url>"
    // ->
    // from_file "<url>", watch=true
    // -> from "<resolved>"
    // from_file "<url>" { read_json schema="yo" }
    // -> from "<resolved>" { read_json schema="yo" }
    auto parser = argument_parser2::operator_(name())
                    .positional("url", url)
                    .named_optional("watch", watch)
                    .named("path_field", path_field)
                    .positional("{ … }", pipe);
    TRY(parser.parse(inv, ctx));
    if (pipe) {
      auto output_type = pipe->inner.infer_type<chunk_ptr>();
      if (not output_type) {
        diagnostic::error("pipeline must accept bytes")
          .primary(*pipe)
          .docs(parser.docs())
          .emit(ctx);
        return failure::promise();
      }
      if (output_type->is_not<table_slice>()) {
        diagnostic::error("pipeline must return events")
          .primary(*pipe)
          .docs(parser.docs())
          .emit(ctx);
        return failure::promise();
      }
      // TODO
    }
    return std::make_unique<from_file>(std::move(url), std::move(path_field),
                                       std::move(pipe));
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
