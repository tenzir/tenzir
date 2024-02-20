//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/identifier.hpp>
#include <tenzir/detail/file_path_to_plugin_name.hpp>
#include <tenzir/detail/loader_saver_resolver.hpp>
#include <tenzir/prepend_token.hpp>
#include <tenzir/tql/parser.hpp>

namespace tenzir::detail {

namespace {

auto make_located_string_view(located<std::string_view> src, size_t pos = 0,
                              size_t count = std::string_view::npos)
  -> located<std::string_view> {
  auto sv = src.inner.substr(pos, count);
  auto loc = src.source.subloc(pos, count);
  return {sv, loc};
}

template <typename Plugin>
struct try_plugin_by_url_result {
  const Plugin* plugin{nullptr};
  located<std::string_view> scheme{};
  located<std::string_view> non_scheme{};
  located<std::string_view> path{};

  [[nodiscard]] bool plugin_found() const {
    return plugin != nullptr;
  }
  [[nodiscard]] bool valid_url_parsed() const {
    return !scheme.inner.empty();
  }
};

template <typename Plugin>
auto find_plugin_by_scheme(std::string_view scheme) -> const Plugin* {
  for (auto&& plugin : plugins::get<Plugin>())
    if (plugin->supported_uri_scheme() == scheme)
      return plugin;
  return nullptr;
}

template <typename Plugin>
auto try_plugin_by_url(located<std::string_view> src)
  -> try_plugin_by_url_result<Plugin> {
  // Not using caf::uri for anything else but checking for URL validity
  // This is because it makes the interaction with located<...> very difficult
  // TODO: We don't want to allow just any URIs, only URLs
  if (!caf::uri::can_parse(src.inner))
    return {};
  // In a valid URL, the first ':' is guaranteed to separate the scheme from
  // the rest
  try_plugin_by_url_result<Plugin> result{};
  auto scheme_len = src.inner.find(':');
  TENZIR_ASSERT(scheme_len != std::string::npos);
  result.scheme = make_located_string_view(src, 0, scheme_len);
  auto non_scheme_offset = scheme_len + 1;
  if (src.inner.substr(non_scheme_offset).starts_with("//"))
    // If the URI has an `authority` component, it starts with "//"
    // We need to skip that before forwarding it to the loader
    non_scheme_offset += 2;
  result.plugin = find_plugin_by_scheme<Plugin>(result.scheme.inner);
  if (!result.plugin)
    return result;
  result.non_scheme = make_located_string_view(src, non_scheme_offset);
  auto non_scheme_without_locator = make_located_string_view(
    result.non_scheme, 0, result.non_scheme.inner.rfind('#'));
  result.path = make_located_string_view(
    non_scheme_without_locator, 0, non_scheme_without_locator.inner.rfind('?'));
  return result;
}

template <typename ParserPlugin, typename Plugin, typename Parse>
auto resolve_loader_saver_impl(parser_interface& parser,
                               located<std::string_view> name, Parse&& parse)
  -> resolve_loader_saver_result<Plugin> {
  if (auto url = try_plugin_by_url<ParserPlugin>(name); url.plugin_found()) {
    // Valid URL, with a matching plugin name
    if (url.scheme.inner == url.plugin->name()) {
      // URL scheme is identical to the plugin name:
      // Pass the URL along _without_ the scheme
      // This is to support URLs like file://foo,
      // which will be transformed to `file foo`
      auto r = prepend_token{url.non_scheme, parser};
      auto parsed = parse(url.plugin, r);
      TENZIR_DIAG_ASSERT(r.at_end());
      return {std::move(parsed), url.scheme, located<std::string>{url.path},
              true};
    }
    // URL scheme is different from the plugin name:
    // Assume that the URL scheme is special, and needs to be passed along
    // This is to support the `gcs`-connector, which needs
    // the `gs://` scheme
    auto r = prepend_token{name, parser};
    auto parsed = parse(url.plugin, r);
    TENZIR_DIAG_ASSERT(r.at_end());
    return {std::move(parsed), url.scheme, located<std::string>{url.path},
            true};
  } else if (url.valid_url_parsed()) {
    // Valid URL, but no matching plugin name -> error
    return {nullptr, url.scheme, {}, true};
  }
  // Check if `name` could be a valid plugin name
  if (parsers::plugin_name(name.inner)) {
    // It could be a plugin name
    if (auto plugin = plugins::find<ParserPlugin>(name.inner)) {
      // Matches a plugin name, use that
      return {parse(plugin, parser), name, {}, false};
    }
    // Not a loaded plugin, but it could've been ->
    // assume that it was a typo, or a plugin that's not loaded -> error
    return {nullptr, name, {}, false};
  }
  // Try `file` loader, may be a path,
  // since it's not a URL or a valid plugin name
  auto plugin = plugins::find<ParserPlugin>("file");
  TENZIR_DIAG_ASSERT(plugin);
  auto r = prepend_token{name, parser};
  auto parsed = parse(plugin, r);
  TENZIR_DIAG_ASSERT(r.at_end());
  return {std::move(parsed),
          located<std::string_view>{"file", location::unknown},
          located<std::string>{name}, false};
}
} // namespace

auto resolve_loader(parser_interface& parser, located<std::string_view> name)
  -> resolve_loader_saver_result<plugin_loader> {
  return resolve_loader_saver_impl<loader_parser_plugin, plugin_loader>(
    parser, name, [](const loader_parser_plugin* plugin, parser_interface& p) {
      return plugin->parse_loader(p);
    });
}

auto resolve_saver(parser_interface& parser, located<std::string_view> name)
  -> resolve_loader_saver_result<plugin_saver> {
  return resolve_loader_saver_impl<saver_parser_plugin, plugin_saver>(
    parser, name, [](const saver_parser_plugin* plugin, parser_interface& p) {
      return plugin->parse_saver(p);
    });
}

namespace {
template <typename Plugin>
auto map_file_path_to_plugin(const std::filesystem::path& path,
                             std::string_view default_plugin) -> const Plugin* {
  auto name
    = file_path_to_plugin_name(path).value_or(std::string{default_plugin});
  if (const auto* plugin = plugins::find<Plugin>(name))
    return plugin;
  return plugins::find<Plugin>(default_plugin);
}

constexpr auto extension_to_compression_map
  = std::array<std::pair<std::string_view, std::string_view>, 8>{
    {{".br", "brotli"},
     {".brotli", "brotli"},
     {".bz2", "bz2"},
     {".gz", "gzip"},
     {".gzip", "gzip"},
     {".lz4", "lz4"},
     {".zst", "zstd"},
     {".zstd", "zstd"}}};

auto map_file_path_to_compression_type(std::string_view path)
  -> std::pair<std::string_view, std::filesystem::path> {
  auto fspath = std::filesystem::path{path};
  auto ext = fspath.extension().string();
  if (auto it = std::ranges::find(extension_to_compression_map, ext,
                                  [](const auto& pair) {
                                    return pair.first;
                                  });
      it != extension_to_compression_map.end())
    return {it->second, fspath.replace_extension("")};
  return {"", fspath};
}

auto resolve_compression_operator(std::string_view op_name,
                                  std::string_view type) -> operator_ptr {
  if (type.empty())
    return nullptr;
  auto plugin = plugins::find_operator(op_name);
  TENZIR_DIAG_ASSERT(plugin);
  auto diag = null_diagnostic_handler{};
  auto p = tql::make_parser_interface(std::string{type}, diag);
  TENZIR_DIAG_ASSERT(p);
  auto oper = plugin->parse_operator(*p);
  TENZIR_DIAG_ASSERT(oper);
  return oper;
}
} // namespace

auto resolve_decompressor(located<std::string_view> path) -> operator_ptr {
  auto [type, _] = map_file_path_to_compression_type(path.inner);
  return resolve_compression_operator("decompress", type);
}

auto resolve_compressor(located<std::string_view> path) -> operator_ptr {
  auto [type, _] = map_file_path_to_compression_type(path.inner);
  return resolve_compression_operator("compress", type);
}

auto resolve_parser(located<std::string_view> path,
                    std::string_view default_parser)
  -> std::pair<operator_ptr, std::unique_ptr<plugin_parser>> {
  auto [decompress_type, path_without_decompress_ext]
    = map_file_path_to_compression_type(path.inner);
  auto decompress = resolve_compression_operator("decompress", decompress_type);
  auto plugin = map_file_path_to_plugin<parser_parser_plugin>(
    path_without_decompress_ext, default_parser);
  auto diag = null_diagnostic_handler{};
  auto p = tql::make_parser_interface("", diag);
  TENZIR_DIAG_ASSERT(p);
  auto parser = plugin->parse_parser(*p);
  TENZIR_DIAG_ASSERT(parser);
  return {std::move(decompress), std::move(parser)};
}

auto resolve_printer(located<std::string_view> path,
                     std::string_view default_printer)
  -> std::pair<operator_ptr, std::unique_ptr<plugin_printer>> {
  auto [compress_type, path_without_compress_ext]
    = map_file_path_to_compression_type(path.inner);
  auto compress = resolve_compression_operator("compress", compress_type);
  auto plugin = map_file_path_to_plugin<printer_parser_plugin>(
    path_without_compress_ext, default_printer);
  auto diag = null_diagnostic_handler{};
  auto p = tql::make_parser_interface("", diag);
  TENZIR_DIAG_ASSERT(p);
  auto printer = plugin->parse_printer(*p);
  TENZIR_DIAG_ASSERT(printer);
  return {std::move(compress), std::move(printer)};
}

} // namespace tenzir::detail
