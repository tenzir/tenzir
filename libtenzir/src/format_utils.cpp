//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2025 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/curl.hpp"

#include <tenzir/format_utils.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>
#include <tenzir/tql2/resolve.hpp>

#include <boost/url/parse.hpp>
#include <boost/url/url_view.hpp>

namespace tenzir {

namespace {

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
  auto* arg = try_as<ast::constant>(expr);
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
  const auto* arg = try_as<ast::constant>(expr);
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

auto get_file(const std::string_view& path) -> std::string {
  const auto escaped = curl::escape(path);
  const auto url = boost::urls::url_view{escaped};
  if (not url.segments().empty()) {
    return url.segments().back();
  }
  if (url.host_type() == boost::urls::host_type::name) {
    return url.host();
  }
  return {};
}

} // namespace

auto invocation_for_plugin(const plugin& plugin, location location)
  -> ast::invocation {
  return ast::invocation{entity_for_plugin(plugin, location), {}};
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
  auto* pipeline_argument = try_as<ast::pipeline_expr>(inv.args.back());
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
    return io_plugin->make(std::move(inv), ctx);
  }
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

template auto
create_pipeline_from_uri<false>(std::string path,
                                operator_factory_plugin::invocation inv,
                                session ctx, const char* docs)
  -> failure_or<operator_ptr>;

template auto
create_pipeline_from_uri<true>(std::string path,
                               operator_factory_plugin::invocation inv,
                               session ctx, const char* docs)
  -> failure_or<operator_ptr>;

} // namespace tenzir
