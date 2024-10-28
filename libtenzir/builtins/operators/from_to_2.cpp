//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/detail/loader_saver_resolver.hpp>
#include <tenzir/diagnostics.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/prepend_token.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql/fwd.hpp>
#include <tenzir/tql/parser.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/exec.hpp>
#include <tenzir/tql2/plugin.hpp>

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
        diagnostic::error("parsing pipeline must be the last argument")
          .primary(it->get_location())
          .primary(inv.args.back().get_location())
          .emit(ctx);
        return failure::promise();
      }
    }
    auto url = boost::urls::parse_uri_reference(path);
    if (not url) {
      diagnostic::error("Invalid URI")
        .primary(inv.args.front().get_location())
        .emit(ctx);
      return failure::promise();
    }
    // determine loader based on schema
    if (url->has_scheme()) {
      for (const auto& p : plugins::get<operator_factory_plugin>()) {
        const auto name = p->name();
        if (not(name.starts_with("load_") or name.starts_with("tql2.load_"))) {
          continue;
        }
        for (auto schema : p->load_schemes()) {
          if (schema == url->scheme()) {
            load_plugin = p;
            break;
          }
        }
        if (load_plugin) {
          break;
        }
      }
      if (not load_plugin) {
        diagnostic::error("Could not determine load operator for scheme `{}`",
                          url->scheme())
          .primary(inv.args.front().get_location())
          .emit(ctx);
        return failure::promise();
      }
    } else {
      load_plugin = plugins::find<operator_factory_plugin>("tql2.load_file");
    }
    TENZIR_ASSERT(load_plugin);
    auto compression_name = std::string_view{};
    if (not pipeline_argument) {
      const auto& file = url->segments().back();
      auto first_dot = file.find('.');
      if (first_dot == file.npos) {
        diagnostic::error("could not infer file type from URI")
          .primary(inv.args.front().get_location())
          .emit(ctx);
        return failure::promise();
      }
      auto filename = std::string_view{file}.substr(first_dot);
      // determine compression based on ending
      {
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
          if (filename.ends_with(extension)) {
            filename.remove_suffix(extension.size());
            compression_name = name;
            break;
          }
        }
        if (not compression_name.empty()) {
          for (const auto& p : plugins::get<operator_factory_plugin>()) {
            const auto name = p->name();
            // TODO, the decompress operators should ultimately be separate
            // operators
            if (name != "decompress") {
              continue;
            }
            decompress_plugin = p;
          }
          TENZIR_ASSERT(decompress_plugin);
        }
      }
      // determine read operator based on file ending
      {
        for (const auto& p : plugins::get<operator_factory_plugin>()) {
          const auto name = p->name();
          if (not(name.starts_with("read_")
                  or name.starts_with("tql2.read_"))) {
            continue;
          }
          // TODO it may be better to use find here to give better error
          // messages if we find it in the middle of the filename
          // this may happen for unknown compressions
          for (auto extension : p->read_extensions()) {
            if (filename.ends_with(extension)) {
              read_plugin = p;
              break;
            }
          }
        }
        if (not read_plugin) {
          diagnostic::error("could not infer format from uri")
            .primary(inv.args.front().get_location())
            .hint("You can pass a pipeline to `from`")
            .emit(ctx);
          return failure::promise();
        }
      }
    }
    TENZIR_TRACE("from operator: given pipeline size   : {}",
                 pipeline_argument
                   ? static_cast<int>(pipeline_argument->inner.body.size())
                   : -1);
    TENZIR_TRACE("from operator: determined loader     : {}",
                 load_plugin ? load_plugin->name() : "none");
    TENZIR_TRACE("from operator: loader accepts pipe   : {}",
                 load_plugin->load_accepts_pipeline());
    TENZIR_TRACE("from operator: determined decompress : {}",
                 decompress_plugin ? decompress_plugin->name() : "none");
    TENZIR_TRACE("from operator: determined read       : {}",
                 read_plugin ? read_plugin->name() : "none");
    if (load_plugin->load_accepts_pipeline()) {
      if (not pipeline_argument) {
        inv.args.emplace_back(ast::pipeline_expr{});
        pipeline_argument = inv.args.back().as<ast::pipeline_expr>();
        if (decompress_plugin) {
          auto decompress_ent = ast::entity{std::vector{
            ast::identifier{decompress_plugin->name(),
                            inv.args.front().get_location()},
          }};
          pipeline_argument->inner.body.emplace_back(
            ast::invocation{std::move(decompress_ent), {}});
        }
        auto read_ent = ast::entity{std::vector{
          ast::identifier{read_plugin->name(), inv.args.front().get_location()},
        }};
        pipeline_argument->inner.body.emplace_back(
          ast::invocation{std::move(read_ent), {}});
      }
      return load_plugin->make(std::move(inv), std::move(ctx));
    } else {
      auto result = std::vector<operator_ptr>{};
      if (pipeline_argument) {
        TRY(auto compiled_pipeline,
            compile(std::move(pipeline_argument->inner), ctx));
        TENZIR_TRACE("from operator: compiled pipeline ops : {}",
                     compiled_pipeline.operators().size());
        result.reserve(1 + compiled_pipeline.operators().size());
        for (auto&& op : std::move(compiled_pipeline).unwrap()) {
          result.push_back(std::move(op));
        }
        /// remove the pipeline argument, as we dont want to pass it to the
        /// loader if it doesnt accept one
        inv.args.pop_back();
      } else {
        if (decompress_plugin) {
          auto decompress_op = operator_ptr{};
          TRY(decompress_op,
              decompress_plugin->make(
                invocation{inv.self,
                           {ast::constant{std::string{compression_name},
                                          location::unknown}}},
                ctx));
          inv.args.clear();
          result.emplace_back(std::move(decompress_op));
        }
        TRY(auto read_op, read_plugin->make(invocation{inv.self, {}}, ctx));
        result.emplace_back(std::move(read_op));
        TENZIR_TRACE("from operator: generated deduced ops : {}",
                     result.size());
      }
      TRY(auto load_op, load_plugin->make(std::move(inv), ctx));
      result.insert(result.begin(), std::move(load_op));
      return std::make_unique<pipeline>(std::move(result));
    }
  }

public:
  auto name() const -> std::string override {
    return "tql2.from";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto docs = "https://docs.tenzir.com/tql2/operators/from";
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
        "`from` can currently not handle more than one nested pipeline")
        .emit(ctx);
      return failure::promise();
    }
    auto pipeline_argument = inv.args.back().as<ast::pipeline_expr>();
    if (pipeline_count > 0) {
      auto it = std::ranges::find_if(inv.args,
                                     &ast::expression::is<ast::pipeline_expr>);
      if (it != std::prev(inv.args.end())) {
        diagnostic::error("parsing pipeline must be the last argument")
          .primary(it->get_location())
          .primary(inv.args.back().get_location())
          .emit(ctx);
        return failure::promise();
      }
    }
    auto url = boost::urls::parse_uri_reference(path);
    if (not url) {
      diagnostic::error("Invalid URI")
        .primary(inv.args.front().get_location())
        .emit(ctx);
      return failure::promise();
    }
    // determine loader based on schema
    if (url->has_scheme()) {
      for (const auto& p : plugins::get<operator_factory_plugin>()) {
        const auto name = p->name();
        if (not(name.starts_with("save_") or name.starts_with("tql2.save_"))) {
          continue;
        }
        for (auto schema : p->save_schemes()) {
          if (schema == url->scheme()) {
            save_plugin = p;
            break;
          }
        }
        if (save_plugin) {
          break;
        }
      }
      if (not save_plugin) {
        diagnostic::error("Could not determine save operator for scheme `{}`",
                          url->scheme())
          .primary(inv.args.front().get_location())
          .emit(ctx);
        return failure::promise();
      }
    } else {
      save_plugin = plugins::find<operator_factory_plugin>("tql2.save_file");
    }
    TENZIR_ASSERT(save_plugin);
    auto compression_name = std::string_view{};
    if (not pipeline_argument) {
      const auto& file = url->segments().back();
      auto first_dot = file.find('.');
      if (first_dot == file.npos) {
        diagnostic::error("could not infer file type from URI")
          .primary(inv.args.front().get_location())
          .emit(ctx);
        return failure::promise();
      }
      auto filename = std::string_view{file}.substr(first_dot);
      // determine compression based on ending
      {
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
          if (filename.ends_with(extension)) {
            filename.remove_suffix(extension.size());
            compression_name = name;
            break;
          }
        }
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
      }
      // determine read operator based on file ending
      {
        for (const auto& p : plugins::get<operator_factory_plugin>()) {
          const auto name = p->name();
          if (not(name.starts_with("write_")
                  or name.starts_with("tql2.write_"))) {
            continue;
          }
          // TODO it may be better to use find here to give better error
          // messages if we find it in the middle of the filename
          // this may happen for unknown compressions
          for (auto extension : p->write_extensions()) {
            if (filename.ends_with(extension)) {
              write_plugin = p;
              break;
            }
          }
          if (write_plugin) {
            break;
          }
        }
        if (not write_plugin) {
          diagnostic::error("could not infer format from uri")
            .primary(inv.args.front().get_location())
            .hint("You can pass a pipeline to `from`")
            .emit(ctx);
          return failure::promise();
        }
      }
    }
    TENZIR_TRACE("to operator: given pipeline size   : {}",
                 pipeline_argument
                   ? static_cast<int>(pipeline_argument->inner.body.size())
                   : -1);
    TENZIR_TRACE("to operator: determined loader     : {}",
                 save_plugin ? save_plugin->name() : "none");
    TENZIR_TRACE("to operator: loader accepts pipe   : {}",
                 save_plugin->load_accepts_pipeline());
    TENZIR_TRACE("to operator: determined decompress : {}",
                 compress_plugin ? compress_plugin->name() : "none");
    TENZIR_TRACE("to operator: determined read       : {}",
                 write_plugin ? write_plugin->name() : "none");
    if (save_plugin->load_accepts_pipeline()) {
      if (not pipeline_argument) {
        inv.args.emplace_back(ast::pipeline_expr{});
        pipeline_argument = inv.args.back().as<ast::pipeline_expr>();
        auto write_ent = ast::entity{std::vector{
          ast::identifier{write_plugin->name(),
                          inv.args.front().get_location()},
        }};
        pipeline_argument->inner.body.emplace_back(
          ast::invocation{std::move(write_ent), {}});
        if (compress_plugin) {
          auto compress_ent = ast::entity{std::vector{
            ast::identifier{compress_plugin->name(),
                            inv.args.front().get_location()},
          }};
          pipeline_argument->inner.body.emplace_back(
            ast::invocation{std::move(compress_ent), {}});
        }
      }
      return save_plugin->make(std::move(inv), std::move(ctx));
    } else {
      auto result = std::vector<operator_ptr>{};
      if (pipeline_argument) {
        TRY(auto compiled_pipeline,
            compile(std::move(pipeline_argument->inner), ctx));
        TENZIR_TRACE("to operator: compiled pipeline ops : {}",
                     compiled_pipeline.operators().size());
        result.reserve(1 + compiled_pipeline.operators().size());
        for (auto&& op : std::move(compiled_pipeline).unwrap()) {
          result.push_back(std::move(op));
        }
        /// remove the pipeline argument, as we dont want to pass it to the
        /// loader if it doesnt accept one
        inv.args.pop_back();
      } else {
        TRY(auto write_op, write_plugin->make(invocation{inv.self, {}}, ctx));
        result.emplace_back(std::move(write_op));
        if (compress_plugin) {
          auto compress_op = operator_ptr{};
          inv.args.emplace_back();
          TRY(compress_op,
              compress_plugin->make(
                invocation{inv.self,
                           {ast::constant{std::string{compression_name},
                                          location::unknown}}},
                ctx));
          inv.args.clear();
          result.emplace_back(std::move(compress_op));
        }
        TENZIR_TRACE("to operator: generated deduced ops : {}", result.size());
      }
      TRY(auto save_op, save_plugin->make(std::move(inv), ctx));
      result.push_back(std::move(save_op));
      return std::make_unique<pipeline>(std::move(result));
    }
  }

public:
  auto name() const -> std::string override {
    return "tql2.to";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto docs = "https://docs.tenzir.com/tql2/operators/from";
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

class load_plugin2 final : virtual public operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.load";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto usage = "load <url/path>, [options...]";
    auto docs = "https://docs.tenzir.com/operators/load";
    if (inv.args.empty()) {
      diagnostic::error("expected at least one argument")
        .primary(inv.self)
        .usage(usage)
        .docs(docs)
        .emit(ctx);
      return failure::promise();
    }
    TRY(auto string_data, const_eval(inv.args[0], ctx));
    auto string = caf::get_if<std::string>(&string_data);
    if (not string) {
      diagnostic::error("expected string")
        .primary(inv.args[0])
        .usage(usage)
        .docs(docs)
        .emit(ctx);
      return failure::promise();
    }
    auto uri = arrow::util::Uri{};
    if (not uri.Parse(*string).ok()) {
      auto target = plugins::find<operator_factory_plugin>("tql2.load_file");
      TENZIR_ASSERT(target);
      return target->make(inv, ctx);
    }
    auto scheme = uri.scheme();
    auto supported = std::vector<std::string>{};
    for (auto plugin : plugins::get<operator_factory_plugin>()) {
      auto plugin_schemes = plugin->load_schemes();
      if (std::ranges::find(plugin_schemes, scheme) != plugin_schemes.end()) {
        return plugin->make(inv, ctx);
      }
      supported.insert(supported.end(), plugin_schemes.begin(),
                       plugin_schemes.end());
    }
    std::ranges::sort(supported);
    diagnostic::error("encountered unsupported scheme `{}`", scheme)
      .primary(inv.args[0])
      .hint("must be one of: {}", fmt::join(supported, ", "))
      .emit(ctx);
    return failure::promise();
  }
};

class save_plugin2 final : virtual public operator_factory_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.save";
  }

  auto
  make(invocation inv, session ctx) const -> failure_or<operator_ptr> override {
    auto usage = "save <url/path>, [options...]";
    auto docs = "https://docs.tenzir.com/operators/save";
    if (inv.args.empty()) {
      diagnostic::error("expected at least one argument")
        .primary(inv.self)
        .usage(usage)
        .docs(docs)
        .emit(ctx);
      return failure::promise();
    }
    TRY(auto string_data, const_eval(inv.args[0], ctx));
    auto string = caf::get_if<std::string>(&string_data);
    if (not string) {
      diagnostic::error("expected string")
        .primary(inv.args[0])
        .usage(usage)
        .docs(docs)
        .emit(ctx);
      return failure::promise();
    }
    auto uri = arrow::util::Uri{};
    if (not uri.Parse(*string).ok()) {
      auto target = plugins::find<operator_factory_plugin>("tql2.save_file");
      TENZIR_ASSERT(target);
      return target->make(inv, ctx);
    }
    auto scheme = uri.scheme();
    auto supported = std::vector<std::string>{};
    for (auto plugin : plugins::get<operator_factory_plugin>()) {
      auto plugin_schemes = plugin->save_schemes();
      if (std::ranges::find(plugin_schemes, scheme) != plugin_schemes.end()) {
        return plugin->make(inv, ctx);
      }
      supported.insert(supported.end(), plugin_schemes.begin(),
                       plugin_schemes.end());
    }
    std::ranges::sort(supported);
    diagnostic::error("encountered unsupported scheme `{}`", scheme)
      .primary(inv.args[0])
      .hint("must be one of: {}", fmt::join(supported, ", "))
      .emit(ctx);
    return failure::promise();
  }
};

} // namespace
} // namespace tenzir::plugins::from

TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_events_plugin)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::from_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::to_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::load_plugin2)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::from::save_plugin2)
