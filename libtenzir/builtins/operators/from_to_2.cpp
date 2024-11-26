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

  explicit from_events(std::vector<record> events)
    : events_{std::move(events)} {
  }

  auto name() const -> std::string override {
    return "tql2.from_events";
  }

  auto
  operator()(operator_control_plane& ctrl) const -> generator<table_slice> {
    // TODO: We are combining all events into a single schema. Is this what we
    // want, or do we want a more "precise" output if possible?
    auto msb = multi_series_builder{
      multi_series_builder::policy_default{},
      multi_series_builder::settings_type{
        .default_schema_name = "tenzir.from",
      },
      ctrl.diagnostics(),
    };
    for (auto& event : events_) {
      msb.data(event);
    }
    for (auto& slice : msb.finalize_as_table_slice()) {
      co_yield std::move(slice);
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
  std::vector<record> events_;
};

using from_events_plugin = operator_inspection_plugin<from_events>;

template <typename F>
using pair_for = std::pair<const operator_factory_plugin*,
                           decltype((std::declval<operator_factory_plugin>()
                                     .*std::declval<F>())())>;

auto find_given(std::string_view what, auto func, auto member,
                std::vector<std::string>& possibilities)
  -> pair_for<decltype(func)> {
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

auto find_connector_given(std::string_view what, auto func, auto member,
                          std::string_view path, location loc, const char* docs,
                          session ctx) -> pair_for<decltype(func)> {
  auto possibilities = std::vector<std::string>{};
  auto res = find_given(what, func, member, possibilities);
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
    .note("supported schemes for deduction: : `{}`",
          fmt::join(possibilities, "`, `"))
    .docs(docs)
    .emit(ctx);
  return {};
}

constexpr static auto extension_to_compression_map
  = std::array<std::pair<std::string_view, std::string_view>, 8>{{
    {".br", "brotli"},
    {".brotli", "brotli"},
    {".bz2", "bz2"},
    {".gz", "gzip"},
    {".gzip", "gzip"},
    {".lz4", "lz4"},
    {".zst", "zstd"},
    {".zstd", "zstd"},
  }};

auto determine_compression(std::string_view& file_ending)
  -> std::pair<std::string_view, std::string_view> {
  for (const auto& kvp : extension_to_compression_map) {
    const auto [extension, name] = kvp;
    if (file_ending.ends_with(extension)) {
      file_ending.remove_suffix(extension.size());
      return kvp;
    }
  }
  return {};
}

auto find_formatter_given(std::string_view what, auto func, auto member,
                          std::string_view path, std::string_view compression,
                          location loc, const char* docs,
                          session ctx) -> pair_for<decltype(func)> {
  auto possibilities = std::vector<std::string>{};
  auto res = find_given(what, func, member, possibilities);
  if (res.first) {
    return res;
  }
  std::ranges::sort(possibilities);
  if (loc.end - loc.begin == path.size() + 2) {
    auto file_start = path.find(what);
    loc.begin += file_start + 1;
    loc.end -= 1;
    loc.end -= compression.size();
  }
  auto diag
    = diagnostic::error("no known format for extension `{}`", what)
        .primary(loc)
        .note("supported extensions for format deduction:  `{}`",
              fmt::join(possibilities, "`, `"))
        .hint("you can pass a pipeline to handle compression and format")
        .docs(docs);
  if (compression.empty()) {
    diag = std::move(diag).note(
      "supported extensions for compression deduction: `{}`",
      fmt::join(extension_to_compression_map | std::views::keys, "`, `"));
  }
  std::move(diag).emit(ctx);
  return {};
}

auto strip_scheme(ast::expression& expr, std::string_view scheme) -> void {
  auto arg = expr.as<ast::constant>();
  TENZIR_ASSERT(arg);
  auto strip_size = scheme.size() + 3;
  // remove the quotes and scheme from the location
  arg->source.begin += 1 + strip_size;
  // remove the quotes from the location
  arg->source.end -= 1;
  match(
    arg->value,
    [strip_size](std::string& s) {
      s.erase(0, strip_size);
    },
    [](const auto&) {
      TENZIR_UNREACHABLE();
    });
}

auto to_located_string(ast::expression& expr) -> located<std::string> {
  auto arg = expr.as<ast::constant>();
  auto str = match(
    arg->value,
    [](std::string& s) -> std::string {
      return std::move(s);
    },
    [](const auto&) -> std::string {
      TENZIR_UNREACHABLE();
      return {};
    });
  return located{std::move(str), arg->source};
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

class from_plugin2 final : public virtual operator_factory_plugin {
  static auto
  create_pipeline_from_uri(std::string path, invocation inv,
                           session ctx) -> failure_or<operator_ptr> {
    const operator_factory_plugin* load_plugin = nullptr;
    const operator_factory_plugin* decompress_plugin = nullptr;
    const operator_factory_plugin* read_plugin = nullptr;
    auto load_properties = operator_factory_plugin::load_properties_t{};
    auto read_properties = operator_factory_plugin::read_properties_t{};
    const auto pipeline_count = std::ranges::count_if(
      inv.args, &ast::expression::is<ast::pipeline_expr>);
    if (pipeline_count > 1) {
      diagnostic::error("`from` accepts at most one pipeline").emit(ctx);
      return failure::promise();
    }
    auto pipeline_argument = inv.args.back().as<ast::pipeline_expr>();
    if (pipeline_count > 0) {
      auto it = std::ranges::find_if(inv.args,
                                     &ast::expression::is<ast::pipeline_expr>);
      if (it != std::prev(inv.args.end())) {
        diagnostic::error("data ingestion pipeline must be the last argument")
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
      std::tie(load_plugin, load_properties) = find_connector_given(
        url->scheme(), &operator_factory_plugin::load_properties,
        &operator_factory_plugin::load_properties_t::schemes, path,
        inv.args.front().get_location(), docs, ctx);
      if (load_plugin) {
        if (load_properties.strip_scheme) {
          strip_scheme(inv.args.front(), url->scheme());
        }
        if (load_properties.transform_uri) {
          auto uri_replacement = load_properties.transform_uri(
            to_located_string(inv.args.front()), ctx);
          TENZIR_TRACE("from operator: URI replacement size  : {}",
                       uri_replacement.size());
          TENZIR_ASSERT(not uri_replacement.empty());
          inv.args.erase(inv.args.begin());
          inv.args.insert(inv.args.begin(), uri_replacement.begin(),
                          uri_replacement.end());
          if (pipeline_argument) {
            pipeline_argument = inv.args.back().as<ast::pipeline_expr>();
          }
        }
      }
    } else {
      load_plugin = plugins::find<operator_factory_plugin>("tql2.load_file");
    }
    auto compression_name = std::string_view{};
    const bool has_pipeline_or_events
      = pipeline_argument or load_properties.events;
    if (not has_pipeline_or_events) {
      auto file = get_file(*url);
      if (file.empty()) {
        diagnostic::error("URL has no segments to deduce the format from")
          .primary(inv.args.front().get_location())
          .hint("you can pass a pipeline to handle compression and format")
          .docs(docs)
          .emit(ctx);
        goto post_deduction;
      }
      auto filename_loc = inv.args.front().get_location();
      if (filename_loc.end - filename_loc.begin == path.size() + 2) {
        auto file_start = path.find(file);
        filename_loc.begin += file_start + 1;
        filename_loc.end -= 1;
      }
      auto first_dot = file.find('.');
      if (first_dot == file.npos) {
        diagnostic::error("did not find extension in `{}`", file)
          .primary(filename_loc)
          .hint("you can pass a pipeline to handle compression and format")
          .emit(ctx);
        return failure::promise();
      }
      auto file_ending = std::string_view{file}.substr(first_dot);
      // determine compression based on ending
      auto compression_extension = std::string_view{};
      std::tie(compression_extension, compression_name)
        = determine_compression(file_ending);
      if (not compression_name.empty()) {
        for (const auto& p : plugins::get<operator_factory_plugin>()) {
          const auto name = p->name();
          if (name != "decompress") {
            continue;
          }
          decompress_plugin = p;
        }
        TENZIR_ASSERT(decompress_plugin);
      }
      // determine read operator based on file ending
      std::tie(read_plugin, read_properties) = find_formatter_given(
        file_ending, &operator_factory_plugin::read_properties,
        &operator_factory_plugin::read_properties_t::extensions, path,
        compression_extension, inv.args.front().get_location(), docs, ctx);
    }
  post_deduction:
    TENZIR_TRACE("from operator: given pipeline size   : {}",
                 pipeline_argument
                   ? static_cast<int>(pipeline_argument->inner.body.size())
                   : -1);
    TENZIR_TRACE("from operator: determined loader     : {}",
                 load_plugin ? load_plugin->name() : "none");
    TENZIR_TRACE("from operator: loader accepts pipe   : {}",
                 load_plugin ? load_properties.accepts_pipeline : false);
    TENZIR_TRACE("from operator: loader produces events: {}",
                 load_plugin ? load_properties.events : false);
    TENZIR_TRACE("from operator: determined decompress : {}",
                 decompress_plugin ? decompress_plugin->name() : "none");
    TENZIR_TRACE("from operator: determined read       : {}",
                 read_plugin ? read_plugin->name() : "none");
    if (not read_plugin and not has_pipeline_or_events) {
      read_plugin = load_properties.default_format;
      TENZIR_TRACE("from operator: fallback read         : {}",
                   read_plugin ? read_plugin->name() : "none");
    }
    if (not load_plugin) {
      return failure::promise();
    }
    if (not read_plugin and not has_pipeline_or_events) {
      return failure::promise();
    }
    if (not has_pipeline_or_events) {
      inv.args.emplace_back(ast::pipeline_expr{});
      pipeline_argument = inv.args.back().as<ast::pipeline_expr>();
      if (decompress_plugin) {
        auto decompress_ent = ast::entity{std::vector{
          ast::identifier{strip_prefix(decompress_plugin->name()),
                          location::unknown},
        }};
        pipeline_argument->inner.body.emplace_back(ast::invocation{
          std::move(decompress_ent),
          {ast::constant{std::string{compression_name}, location::unknown}}});
      }
      auto read_ent = ast::entity{std::vector{
        ast::identifier{strip_prefix(read_plugin->name()), location::unknown},
      }};
      pipeline_argument->inner.body.emplace_back(
        ast::invocation{std::move(read_ent), {}});
      TENZIR_ASSERT(resolve_entities(pipeline_argument->inner, ctx));
    }
    TENZIR_TRACE("from operator: final pipeline        :");
    for (auto& arg : inv.args) {
      TENZIR_TRACE("    {:?}", arg);
    }
    if (load_properties.accepts_pipeline) {
      return load_plugin->make(std::move(inv), std::move(ctx));
    } else {
      auto compiled_pipeline = pipeline{};
      if (pipeline_argument) {
        TRY(compiled_pipeline,
            compile(std::move(pipeline_argument->inner), ctx));
        TENZIR_TRACE("from operator: compiled pipeline ops : {}",
                     compiled_pipeline.operators().size());
        inv.args.pop_back();
      }
      TRY(auto load_op, load_plugin->make(std::move(inv), ctx));
      compiled_pipeline.prepend(std::move(load_op));
      return std::make_unique<pipeline>(std::move(compiled_pipeline));
    }
  }

public:
  constexpr static auto docs = "https://docs.tenzir.com/tql2/operators/from";
  auto name() const -> std::string override {
    return "tql2.from";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    if (inv.args.empty()) {
      diagnostic::error("expected positional argument `<path/url/events>`")
        .primary(inv.self)
        .docs(docs)
        .emit(ctx);
      return failure::promise();
    }
    auto& expr = inv.args[0];
    TRY(auto value, const_eval(expr, ctx));
    auto events = std::vector<record>{};
    using ret = std::variant<bool, failure_or<operator_ptr>>;
    auto extract_single_event = [&](record& event) -> ret {
      events.push_back(std::move(event));
      return true;
    };
    auto extract_multiple_events = [&](list& event_list) -> ret {
      for (auto& event : event_list) {
        auto event_record = try_as<record>(&event);
        if (not event_record) {
          auto t = type::infer(value);
          diagnostic::error("expected list of records")
            .primary(expr, "got `{}`", t ? t->kind() : type_kind{})
            .docs(docs)
            .emit(ctx);
          return failure::promise();
        }
        events.push_back(std::move(*event_record));
      }
      return true;
    };
    auto result = match(
      value, extract_single_event, extract_multiple_events,
      [&](std::string& path) -> ret {
        return create_pipeline_from_uri(path, std::move(inv), std::move(ctx));
      },
      [&]<typename T>(T& value) -> ret
      // requires(not std::same_as<T, record>)
      {
        auto t = type::infer(value);
        diagnostic::error("expected a URI, record or list of records")
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
      result = match(value, extract_single_event, extract_multiple_events,
                     [&](const auto&) -> ret {
                       const auto t = type::infer(value);
                       diagnostic::error(
                         "expected further records or lists of records")
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
  static auto
  create_pipeline_from_uri(std::string path, invocation inv,
                           session ctx) -> failure_or<operator_ptr> {
    const operator_factory_plugin* save_plugin = nullptr;
    const operator_factory_plugin* compress_plugin = nullptr;
    const operator_factory_plugin* write_plugin = nullptr;
    auto save_properties = operator_factory_plugin::save_properties_t{};
    auto write_properties = operator_factory_plugin::write_properties_t{};
    const auto pipeline_count = std::ranges::count_if(
      inv.args, &ast::expression::is<ast::pipeline_expr>);
    if (pipeline_count > 1) {
      diagnostic::error("`to` accepts at most one pipeline").emit(ctx);
      return failure::promise();
    }
    auto pipeline_argument = inv.args.back().as<ast::pipeline_expr>();
    if (pipeline_count > 0) {
      auto it = std::ranges::find_if(inv.args,
                                     &ast::expression::is<ast::pipeline_expr>);
      if (it != std::prev(inv.args.end())) {
        diagnostic::error("writing pipeline must be the last argument")
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
      std::tie(save_plugin, save_properties) = find_connector_given(
        url->scheme(), &operator_factory_plugin::save_properties,
        &operator_factory_plugin::save_properties_t::schemes, path,
        inv.args.front().get_location(), docs, ctx);
      if (save_plugin) {
        if (save_properties.strip_scheme) {
          strip_scheme(inv.args.front(), url->scheme());
        }
        if (save_properties.transform_uri) {
          auto uri_replacement = save_properties.transform_uri(
            to_located_string(inv.args.front()), ctx);
          TENZIR_TRACE("to operator: URI replacement size  : {}",
                       uri_replacement.size());
          TENZIR_ASSERT(not uri_replacement.empty());
          inv.args.erase(inv.args.begin());
          inv.args.insert(inv.args.begin(), uri_replacement.begin(),
                          uri_replacement.end());
          if (pipeline_argument) {
            pipeline_argument = inv.args.back().as<ast::pipeline_expr>();
          }
        }
      }
    } else {
      save_plugin = plugins::find<operator_factory_plugin>("tql2.save_file");
    }
    const bool has_pipeline_or_events
      = pipeline_argument or save_properties.events;
    auto compression_name = std::string_view{};
    if (not has_pipeline_or_events) {
      auto file = get_file(*url);
      if (file.empty()) {
        diagnostic::error("URL has no segments to deduce the format from")
          .primary(inv.args.front().get_location())
          .hint("you can pass a pipeline to handle compression and format")
          .docs(docs)
          .emit(ctx);
        goto post_deduction;
      }
      auto filename_loc = inv.args.front().get_location();
      if (filename_loc.end - filename_loc.begin == path.size() + 2) {
        auto file_start = path.find(file);
        filename_loc.begin += file_start + 1;
        filename_loc.end -= 1;
      }
      auto first_dot = file.find('.');
      if (first_dot == file.npos) {
        diagnostic::error("did not find extension in `{}`", file)
          .primary(filename_loc)
          .hint("you can pass a pipeline to handle compression and format")
          .emit(ctx);
        return failure::promise();
      }
      auto file_ending = std::string_view{file}.substr(first_dot);
      // determine compression based on ending
      auto compression_extension = std::string_view{};
      std::tie(compression_extension, compression_name)
        = determine_compression(file_ending);
      if (not compression_name.empty()) {
        for (const auto& p : plugins::get<operator_factory_plugin>()) {
          const auto name = p->name();
          // TODO, the decompress operators should ultimately be separate
          // operators
          if (name != "compress") {
            continue;
          }
          compress_plugin = p;
        }
        TENZIR_ASSERT(compress_plugin);
      }
      // determine write operator based on file ending
      std::tie(write_plugin, write_properties) = find_formatter_given(
        file_ending, &operator_factory_plugin::write_properties,
        &operator_factory_plugin::write_properties_t::extensions, path,
        compression_extension, inv.args.front().get_location(), docs, ctx);
    }
  post_deduction:
    TENZIR_TRACE("to operator: given pipeline size   : {}",
                 pipeline_argument
                   ? static_cast<int>(pipeline_argument->inner.body.size())
                   : -1);
    TENZIR_TRACE("to operator: determined loader     : {}",
                 save_plugin ? save_plugin->name() : "none");
    TENZIR_TRACE("to operator: saver accepts pipe    : {}",
                 save_plugin ? save_properties.accepts_pipeline : false);
    TENZIR_TRACE("to operator: saver accepts events  : {}",
                 save_plugin ? save_properties.events : false);
    TENZIR_TRACE("to operator: determined decompress : {}",
                 compress_plugin ? compress_plugin->name() : "none");
    TENZIR_TRACE("to operator: determined read       : {}",
                 write_plugin ? write_plugin->name() : "none");
    if (not write_plugin and not has_pipeline_or_events) {
      write_plugin = save_properties.default_format;
      TENZIR_TRACE("to operator: fallback read         : {}",
                   write_plugin ? write_plugin->name() : "none");
    }
    if (not save_plugin) {
      return failure::promise();
    }
    if (not write_plugin and not has_pipeline_or_events) {
      return failure::promise();
    }
    if (not has_pipeline_or_events) {
      inv.args.emplace_back(ast::pipeline_expr{});
      pipeline_argument = inv.args.back().as<ast::pipeline_expr>();
      if (compress_plugin) {
        auto decompress_ent = ast::entity{std::vector{
          ast::identifier{strip_prefix(compress_plugin->name()),
                          location::unknown},
        }};
        pipeline_argument->inner.body.emplace_back(ast::invocation{
          std::move(decompress_ent),
          {ast::constant{std::string{compression_name}, location::unknown}}});
      }
      auto read_ent = ast::entity{std::vector{
        ast::identifier{strip_prefix(write_plugin->name()), location::unknown},
      }};
      pipeline_argument->inner.body.emplace_back(
        ast::invocation{std::move(read_ent), {}});
      TENZIR_ASSERT(resolve_entities(pipeline_argument->inner, ctx));
    }
    TENZIR_TRACE("to operator: final pipeline        :");
    for (auto& arg : inv.args) {
      TENZIR_TRACE("    {:?}", arg);
    }
    if (save_plugin->load_properties().accepts_pipeline) {
      return save_plugin->make(std::move(inv), std::move(ctx));
    } else {
      auto compiled_pipeline = pipeline{};
      if (pipeline_argument) {
        TRY(compiled_pipeline,
            compile(std::move(pipeline_argument->inner), ctx));
        TENZIR_TRACE("from operator: compiled pipeline ops : {}",
                     compiled_pipeline.operators().size());
        inv.args.pop_back();
      }
      TRY(auto load_op, save_plugin->make(std::move(inv), ctx));
      compiled_pipeline.append(std::move(load_op));
      return std::make_unique<pipeline>(std::move(compiled_pipeline));
    }
  }

public:
  constexpr static auto docs = "https://docs.tenzir.com/tql2/operators/tp";
  auto name() const -> std::string override {
    return "tql2.to";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    if (inv.args.empty()) {
      diagnostic::error("expected positional argument `<path>`")
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
        return create_pipeline_from_uri(path, std::move(inv), std::move(ctx));
      },
      [&](auto&) -> failure_or<operator_ptr> {
        diagnostic::error("expected a URI")
          .primary(inv.args[0])
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
