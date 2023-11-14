//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/identifier.hpp>
#include <tenzir/detail/loader_saver_resolver.hpp>

namespace tenzir::detail {

namespace {

template <typename String>
class prepend_token final : public parser_interface {
public:
  prepend_token(located<String> token, parser_interface& next)
    : token_{std::move(token)}, next_{next} {
  }
  prepend_token(std::nullopt_t, parser_interface& next)
    : token_{std::nullopt}, next_{next} {
  }

  auto accept_shell_arg() -> std::optional<located<std::string>> override {
    if (token_) {
      auto tmp = make_located_string();
      token_ = std::nullopt;
      return tmp;
    }
    return next_.accept_shell_arg();
  }

  auto peek_shell_arg() -> std::optional<located<std::string>> override {
    if (token_) {
      return make_located_string();
    }
    return next_.peek_shell_arg();
  }

  auto accept_identifier() -> std::optional<identifier> override {
    TENZIR_ASSERT(not token_);
    return next_.accept_identifier();
  }

  auto peek_identifier() -> std::optional<identifier> override {
    TENZIR_ASSERT(not token_);
    return next_.peek_identifier();
  }

  auto accept_equals() -> std::optional<location> override {
    TENZIR_ASSERT(not token_);
    return next_.accept_equals();
  }

  auto accept_char(char c) -> std::optional<location> override {
    TENZIR_ASSERT(not token_);
    return next_.accept_char(c);
  }

  auto parse_operator() -> located<operator_ptr> override {
    TENZIR_ASSERT(not token_);
    return next_.parse_operator();
  }

  auto parse_expression() -> tql::expression override {
    TENZIR_ASSERT(not token_);
    return next_.parse_expression();
  }

  auto parse_legacy_expression() -> located<expression> override {
    TENZIR_ASSERT(not token_);
    return next_.parse_legacy_expression();
  }

  auto parse_extractor() -> tql::extractor override {
    TENZIR_ASSERT(not token_);
    return next_.parse_extractor();
  }

  auto at_end() -> bool override {
    return not token_ && next_.at_end();
  }

  auto current_span() -> location override {
    if (token_) {
      return token_->source;
    }
    return next_.current_span();
  }

private:
  auto make_located_string() const -> located<std::string> {
    TENZIR_ASSERT(token_);
    return {std::string{token_->inner}, token_->source};
  }

  std::optional<located<String>> token_;
  parser_interface& next_;
};

template <typename String>
auto make_located_string_view(const located<String>& str, size_t pos = 0,
                              size_t count = std::string::npos)
  -> located<std::string_view> {
  auto sub = std::string_view{str.inner}.substr(pos, count);
  auto source = location::unknown;
  if (str.source && (str.source.end - str.source.begin) == str.inner.size()) {
    source.begin = str.source.begin + pos;
    source.end = source.begin + sub.length();
  }
  return {sub, source};
}

template <typename Plugin>
struct try_plugin_by_uri_result {
  const Plugin* plugin{nullptr};
  located<std::string_view> scheme{};
  located<std::string_view> non_scheme{};

  [[nodiscard]] bool plugin_found() const {
    return plugin != nullptr;
  }
  [[nodiscard]] bool valid_uri_parsed() const {
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
auto try_plugin_by_uri(const located<std::string>& src)
  -> try_plugin_by_uri_result<Plugin> {
  // Not using caf::uri for anything else but checking for URI validity
  // This is because it makes the interaction with located<...> very difficult
  if (!caf::uri::can_parse(src.inner))
    return {};
  // In a valid URI, the first ':' is guaranteed to separate the scheme from
  // the rest
  TENZIR_ASSERT(src.inner.find(':') != std::string::npos);
  try_plugin_by_uri_result<Plugin> result{};
  auto scheme_len = src.inner.find(':');
  result.scheme = make_located_string_view(src, 0, scheme_len);
  auto non_scheme_offset = scheme_len + 1;
  if (src.inner.substr(non_scheme_offset).starts_with("//"))
    // If the URI has an `authority` component, it starts with "//"
    // We need to skip that before forwarding it to the loader
    non_scheme_offset += 2;
  result.non_scheme = make_located_string_view(src, non_scheme_offset);
  result.plugin = find_plugin_by_scheme<Plugin>(result.scheme.inner);
  return result;
}

template <typename ParserPlugin, typename Plugin, typename Parse>
auto resolve_impl(parser_interface& parser, const located<std::string>& name,
                  Parse&& parse) -> resolve_loader_saver_result<Plugin> {
  if (auto uri = try_plugin_by_uri<ParserPlugin>(name); uri.plugin_found()) {
    // Valid URI, with a matching plugin name
    if (uri.scheme.inner == uri.plugin->name()) {
      // URI scheme is identical to the plugin name:
      // Pass the URI along _without_ the scheme
      // This is to support URIs like file://foo,
      // which will be transformed to `file foo`
      auto r = prepend_token{uri.non_scheme, parser};
      auto parsed = parse(uri.plugin, r);
      TENZIR_DIAG_ASSERT(r.at_end());
      return {std::move(parsed), make_located_string_view(uri.scheme), true};
    }
    // URI scheme is different from the plugin name:
    // Assume that the URI scheme is special, and needs to be passed along
    // This is to support the `gcs`-connector, which needs
    // the `gs://` scheme
    auto r = prepend_token{name, parser};
    auto parsed = parse(uri.plugin, r);
    TENZIR_DIAG_ASSERT(r.at_end());
    return {std::move(parsed), make_located_string_view(uri.scheme), true};
  } else if (uri.valid_uri_parsed()) {
    // Valid URI, but no matching plugin name -> error
    return {nullptr, make_located_string_view(uri.scheme), true};
  }
  // Check if `name` could be a valid plugin name
  if (parsers::plugin_name(name.inner)) {
    // It could be a plugin name
    if (auto plugin = plugins::find<ParserPlugin>(name.inner)) {
      // Matches a plugin name, use that
      return {parse(plugin, parser), make_located_string_view(name), false};
    }
    // Not a loaded plugin, but it could've been ->
    // assume that it was a typo, or a plugin that's not loaded -> error
    return {nullptr, make_located_string_view(name), false};
  }
  // Try `file` loader, may be a path,
  // since it's not a URI or a valid plugin name
  auto plugin = plugins::find<ParserPlugin>("file");
  TENZIR_DIAG_ASSERT(plugin);
  auto name_sv = make_located_string_view(name);
  auto r = prepend_token{name_sv, parser};
  auto parsed = parse(plugin, r);
  TENZIR_DIAG_ASSERT(r.at_end());
  return {std::move(parsed), name_sv, false};
}
} // namespace

auto resolve_loader(parser_interface& parser, const located<std::string>& name)
  -> resolve_loader_saver_result<plugin_loader> {
  return resolve_impl<loader_parser_plugin, plugin_loader>(
    parser, name, [](const loader_parser_plugin* plugin, parser_interface& p) {
      return plugin->parse_loader(p);
    });
}

auto resolve_saver(parser_interface& parser, const located<std::string>& name)
  -> resolve_loader_saver_result<plugin_saver> {
  return resolve_impl<saver_parser_plugin, plugin_saver>(
    parser, name, [](const saver_parser_plugin* plugin, parser_interface& p) {
      return plugin->parse_saver(p);
    });
}

} // namespace tenzir::detail
