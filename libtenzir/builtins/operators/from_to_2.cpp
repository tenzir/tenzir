//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/diagnostics.hpp>
#include <tenzir/multi_series_builder.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/prepend_token.hpp>
#include <tenzir/tql/fwd.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/eval_impl.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <arrow/util/uri.h>
#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>

#include <ranges>

namespace tenzir::plugins::from {
namespace {

class from_events final : public crtp_operator<from_events> {
public:
  from_events() = default;

  series s;
  explicit from_events(std::vector<ast::expression> events)
    : events_{std::move(events)} {
  }

  auto name() const -> std::string override {
    return "tql2.from_events";
  }

  auto
  operator()(operator_control_plane& ctrl) const -> generator<table_slice> {
    auto sp = session_provider::make(ctrl.diagnostics());
    const auto non_const_eval = [&](const ast::expression& expr) {
      auto value = evaluator{nullptr, sp.as_session()}.eval(expr);
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

  auto optimize(expression const& filter,
                event_order order) const -> optimize_result override {
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

template <bool is_loading>
auto find_compression_and_format(std::string_view extension,
                                 std::string_view path, location loc,
                                 const char* docs, bool emit, session ctx)
  -> std::tuple<const operator_factory_plugin*, const operator_factory_plugin*> {
  using traits = from_to_trait<is_loading>;
  auto format_extensions = std::vector<std::string>{};
  auto compression_extensions = std::vector<std::string>{};

  const operator_factory_plugin* found_compression_plugin = nullptr;
  const operator_factory_plugin* found_rw_plugin = nullptr;
  auto compression_extension = std::string{};
  for (const auto& p : plugins::get<operator_factory_plugin>()) {
    auto comp_properties = (p->*traits::compression_properties_getter)();
    auto rw_properties = (p->*traits::rw_properties_getter)();
    for (auto& possibility :
         comp_properties.*traits::compression_properties_range_member) {
      if (extension.ends_with(possibility)) {
        found_rw_plugin = p;
        break;
      }
      format_extensions.push_back(std::move(possibility));
    }
    if (found_rw_plugin) {
      break;
    }
    for (auto& possibility :
         rw_properties.*traits::rw_properties_range_member) {
      if (extension.ends_with(possibility)) {
        TENZIR_ASSERT(compression_extension.empty());
        compression_extension = possibility;
        extension.remove_suffix(possibility.size() + 1);
        found_compression_plugin = p;
      }
      compression_extensions.push_back(std::move(possibility));
    }
  }
  if (found_rw_plugin) {
    return {found_compression_plugin, found_rw_plugin};
  }
  std::ranges::sort(format_extensions);
  if (loc.end - loc.begin == path.size() + 2) {
    auto file_start = path.find(compression_extension);
    loc.begin += file_start + 1;
    loc.end -= 1;
    loc.end -= compression_extension.size();
  }
  if (emit) {
    auto diag
      = diagnostic::error("no known format for extension `{}`", extension)
          .primary(loc)
          .note("supported extensions for format deduction: `{}`",
                fmt::join(format_extensions, "`, `"));
    if (compression_extension.empty()) {
      diag = std::move(diag).note("supported extensions for compression "
                                  "deduction: `{}`",
                                  fmt::join(compression_extensions, "`, `"));
    }
    diag = std::move(diag)
             .hint("you can pass a pipeline to handle compression and format")
             .docs(docs);
    std::move(diag).emit(ctx);
  }
  return {};
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

auto get_as_located_string(const ast::expression& expr) -> located<std::string> {
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
auto create_pipeline_from_uri(std::string path,
                              operator_factory_plugin::invocation inv,
                              session ctx,
                              const char* docs) -> failure_or<operator_ptr> {
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
  auto compression_name = std::string_view{};
  if (not has_pipeline_or_events) {
    auto file = get_file(*url);
    if (file.empty()) {
      diagnostic::error("URL has no segments to deduce the format from")
        .primary(inv.args.front().get_location())
        .hint("you can pass a pipeline to handle compression and format")
        .docs(docs)
        .emit(ctx);
      goto post_deduction_reporting;
    }
    auto filename_loc = inv.args.front().get_location();
    if (filename_loc.end - filename_loc.begin == path.size() + 2) {
      auto file_start = path.find(file);
      filename_loc.begin += file_start + 1;
      filename_loc.end -= 1;
    }
    auto first_dot = file.find('.');
    if (first_dot == file.npos) {
      if (io_properties.default_format == nullptr) {
        diagnostic::error("did not find extension in `{}`", file)
          .primary(filename_loc)
          .hint("you can pass a pipeline to handle compression and format")
          .emit(ctx);
      }
      goto post_deduction_reporting;
    }
    auto file_ending = std::string_view{file}.substr(first_dot);
    std::tie(compression_plugin, rw_plugin)
      = find_compression_and_format<is_loading>(
        file_ending, path, inv.args.front().get_location(), docs,
        io_properties.default_format == nullptr, ctx);
  }
post_deduction_reporting:
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
  /// TODO: Decide on whether/where we actually want this
  if (not rw_plugin and not has_pipeline_or_events
      and io_properties.default_format) {
    rw_plugin = io_properties.default_format;
    TENZIR_TRACE("{} operator: fallback read         : {}",
                 traits::operator_name, rw_plugin->name());
  }
  if (not io_plugin) {
    return failure::promise();
  }
  if (not rw_plugin and not has_pipeline_or_events) {
    return failure::promise();
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
      pipeline_argument->inner.body.emplace_back(ast::invocation{
        std::move(compression_ent),
        {ast::constant{std::string{compression_name}, location::unknown}}});
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

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
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

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
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

} // namespace
} // namespace tenzir::plugins::from

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_events_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::to_plugin2)
