//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/prepend_token.hpp>
#include <tenzir/series_builder.hpp>
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

  auto operator()() const -> generator<table_slice> {
    // TODO: We are combining all events into a single schema. Is this what we
    // want, or do we want a more "precise" output if possible?
    auto sb = series_builder{};
    for (auto& event : events_) {
      sb.data(event);
    }
    auto slices = sb.finish_as_table_slice("tenzir.from");
    for (auto& slice : slices) {
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

auto find_given(std::string_view what, auto func, auto member,
                std::vector<std::string>& possibilities)
  -> const operator_factory_plugin* {
  for (const auto& p : plugins::get<operator_factory_plugin>()) {
    auto properties = (p->*func)();
    for (auto possibility : properties.*member) {
      if (what.ends_with(possibility)) {
        return p;
        break;
      }
      possibilities.push_back(std::move(possibility));
    }
  }
  return nullptr;
}

auto find_connector_given(std::string_view what, auto func, auto member,
                          std::string_view path, location loc,
                          session ctx) -> const operator_factory_plugin* {
  auto possibilities = std::vector<std::string>{};
  auto plugin = find_given(what, func, member, possibilities);
  if (plugin) {
    return plugin;
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
    .emit(ctx);
  return nullptr;
}

auto find_formatter_given(std::string_view what, auto func, auto member,
                          std::string_view path, location loc, const char* docs,
                          session ctx) -> const operator_factory_plugin* {
  auto possibilities = std::vector<std::string>{};
  auto plugin = find_given(what, func, member, possibilities);
  if (plugin) {
    return plugin;
  }
  std::ranges::sort(possibilities);
  if (loc.end - loc.begin == path.size() + 2) {
    auto file_start = path.find(what);
    loc.begin += file_start + 1;
    loc.end -= 1; // TODO this may be shorted in case we have already cut out
                  // the compression
  }
  diagnostic::error("no known format for extension `{}`", what)
    .primary(loc)
    .note("supported extensions for deduction:  `{}`",
          fmt::join(possibilities, "`, `"))
    .note("this could also be caused by an unknown compression extension")
    .hint("you can pass a pipeline to handle compression and format")
    .docs(docs)
    .emit(ctx);
  return nullptr;
}

auto determine_compression(std::string_view& file_ending) -> std::string_view {
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
  for (const auto& [extension, name] : extension_to_compression_map) {
    if (file_ending.ends_with(extension)) {
      file_ending.remove_suffix(extension.size());
      return name;
    }
  }
  return {};
}

auto strip_prefix(std::string name) -> std::string {
  constexpr auto prefix = std::string_view{"tql2."};
  if (name.starts_with(prefix)) {
    return std::move(name).substr(prefix.size());
  }
  return name;
}

class from_plugin2 final : public virtual operator_factory_plugin {
  static auto
  create_pipeline_from_uri(std::string_view path, invocation inv,
                           session ctx) -> failure_or<operator_ptr> {
    const operator_factory_plugin* load_plugin = nullptr;
    const operator_factory_plugin* decompress_plugin = nullptr;
    const operator_factory_plugin* read_plugin = nullptr;
    const auto pipeline_count = std::ranges::count_if(
      inv.args, &ast::expression::is<ast::pipeline_expr>);
    if (pipeline_count > 1) {
      diagnostic::error(
        "`from` can currently not handle more than one nested pipeline")
        .emit(ctx);
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
      diagnostic::error("invalid URI")
        .primary(inv.args.front().get_location())
        .emit(ctx);
      return failure::promise();
    }
    // determine loader based on schema
    if (url->has_scheme()) {
      load_plugin = find_connector_given(
        url->scheme(), &operator_factory_plugin::load_properties,
        &operator_factory_plugin::load_properties_t::schemes, path,
        inv.args.front().get_location(), ctx);
    } else {
      load_plugin = plugins::find<operator_factory_plugin>("tql2.load_file");
    }
    auto compression_name = std::string_view{};
    if (not pipeline_argument) {
      const auto& file = url->segments().back();
      auto first_dot = file.find('.');
      if (first_dot == file.npos) {
        diagnostic::error("did not find extension in `{}`", file)
          .primary(inv.args.front().get_location())
          .emit(ctx);
        return failure::promise();
      }
      auto file_ending = std::string_view{file}.substr(first_dot);
      auto filename_loc = inv.args.front().get_location();
      if (filename_loc.end - filename_loc.begin == path.size() + 2) {
        auto file_start = path.find(file);
        filename_loc.begin += file_start + 1;
        filename_loc.end -= 1;
      }
      // determine compression based on ending
      compression_name = determine_compression(file_ending);
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
      read_plugin = find_formatter_given(
        file_ending, &operator_factory_plugin::read_properties,
        &operator_factory_plugin::read_properties_t::extensions, path,
        inv.args.front().get_location(), docs, ctx);
    }
    TENZIR_TRACE("from operator: given pipeline size   : {}",
                 pipeline_argument
                   ? static_cast<int>(pipeline_argument->inner.body.size())
                   : -1);
    TENZIR_TRACE("from operator: determined loader     : {}",
                 load_plugin ? load_plugin->name() : "none");
    TENZIR_TRACE("from operator: loader accepts pipe   : {}",
                 load_plugin ? load_plugin->load_properties().accepts_pipeline
                             : false);
    TENZIR_TRACE("from operator: determined decompress : {}",
                 decompress_plugin ? decompress_plugin->name() : "none");
    TENZIR_TRACE("from operator: determined read       : {}",
                 read_plugin ? read_plugin->name() : "none");
    if (not load_plugin or not read_plugin) {
      return failure::promise();
    }
    if (not pipeline_argument) {
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
    if (load_plugin->load_properties().accepts_pipeline) {
      return load_plugin->make(std::move(inv), std::move(ctx));
    } else {
      TRY(auto compiled_pipeline,
          compile(std::move(pipeline_argument->inner), ctx));
      TENZIR_TRACE("from operator: compiled pipeline ops : {}",
                   compiled_pipeline.operators().size());
      inv.args.pop_back();
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
    auto f = detail::overload{
      [&](record& event) -> failure_or<operator_ptr> {
        auto events = std::vector<record>{};
        events.push_back(std::move(event));
        return std::make_unique<from_events>(std::move(events));
      },
      [&](list& event_list) -> failure_or<operator_ptr> {
        auto events = std::vector<record>{};
        for (auto& event : event_list) {
          auto event_record = caf::get_if<record>(&event);
          if (not event_record) {
            diagnostic::error("expected list of records")
              .primary(expr)
              .docs(docs)
              .emit(ctx);
            return failure::promise();
          }
          events.push_back(std::move(*event_record));
        }
        return std::make_unique<from_events>(std::move(events));
      },
      [&](std::string& path) -> failure_or<operator_ptr> {
        return create_pipeline_from_uri(path, std::move(inv), std::move(ctx));
      },
      [&](auto&) -> failure_or<operator_ptr> {
        diagnostic::error("expected a URI, record or list of records")
          .primary(inv.args[0])
          .docs(docs)
          .emit(ctx);
        return failure::promise();
      },
    };
    return caf::visit(f, value);
  }
};

class to_plugin2 final : public virtual operator_factory_plugin {
  static auto
  create_pipeline_from_uri(std::string_view path, invocation inv,
                           session ctx) -> failure_or<operator_ptr> {
    const operator_factory_plugin* save_plugin = nullptr;
    const operator_factory_plugin* compress_plugin = nullptr;
    const operator_factory_plugin* write_plugin = nullptr;
    const auto pipeline_count = std::ranges::count_if(
      inv.args, &ast::expression::is<ast::pipeline_expr>);
    if (pipeline_count > 1) {
      diagnostic::error(
        "`to` can currently not handle more than one nested pipeline")
        .emit(ctx);
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
      diagnostic::error("invalid URI")
        .primary(inv.args.front().get_location())
        .emit(ctx);
      return failure::promise();
    }
    // determine loader based on schema
    if (url->has_scheme()) {
      save_plugin = find_connector_given(
        url->scheme(), &operator_factory_plugin::save_properties,
        &operator_factory_plugin::save_properties_t::schemes, path,
        inv.args.front().get_location(), ctx);
    } else {
      save_plugin = plugins::find<operator_factory_plugin>("tql2.save_file");
    }
    auto compression_name = std::string_view{};
    if (not pipeline_argument) {
      const auto& file = url->segments().back();
      auto first_dot = file.find('.');
      auto file_ending = std::string_view{file}.substr(first_dot);
      auto filename_loc = inv.args.front().get_location();
      if (filename_loc.end - filename_loc.begin == path.size() + 2) {
        auto file_start = path.find(file);
        filename_loc.begin += file_start + 1;
        filename_loc.end -= 1;
      }
      if (first_dot == file.npos) {
        diagnostic::error("did not find extension in filename `{}`",
                          file_ending)
          .primary(filename_loc)
          .emit(ctx);
        return failure::promise();
      }
      // determine compression based on ending
      compression_name = determine_compression(file_ending);
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
      write_plugin = find_formatter_given(
        file_ending, &operator_factory_plugin::write_properties,
        &operator_factory_plugin::write_properties_t::extensions, path,
        inv.args.front().get_location(), docs, ctx);
    }
    TENZIR_TRACE("to operator: given pipeline size   : {}",
                 pipeline_argument
                   ? static_cast<int>(pipeline_argument->inner.body.size())
                   : -1);
    TENZIR_TRACE("to operator: determined loader     : {}",
                 save_plugin ? save_plugin->name() : "none");
    TENZIR_TRACE("to operator: loader accepts pipe   : {}",
                 save_plugin ? save_plugin->save_properties().accepts_pipeline
                             : false);
    TENZIR_TRACE("to operator: determined decompress : {}",
                 compress_plugin ? compress_plugin->name() : "none");
    TENZIR_TRACE("to operator: determined read       : {}",
                 write_plugin ? write_plugin->name() : "none");
    if (not save_plugin or not write_plugin) {
      return failure::promise();
    }
    if (not pipeline_argument) {
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
    if (save_plugin->load_properties().accepts_pipeline) {
      return save_plugin->make(std::move(inv), std::move(ctx));
    } else {
      TRY(auto compiled_pipeline,
          compile(std::move(pipeline_argument->inner), ctx));
      TENZIR_TRACE("from operator: compiled pipeline ops : {}",
                   compiled_pipeline.operators().size());
      inv.args.pop_back();
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
    auto f = detail::overload{
      [&](std::string& path) -> failure_or<operator_ptr> {
        return create_pipeline_from_uri(path, std::move(inv), std::move(ctx));
      },
      [&](auto&) -> failure_or<operator_ptr> {
        diagnostic::error("expected a URI")
          .primary(inv.args[0])
          .docs(docs)
          .emit(ctx);
        return failure::promise();
      },
    };
    return caf::visit(f, value);
  }
};

} // namespace
} // namespace tenzir::plugins::from

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_events_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::to_plugin2)
