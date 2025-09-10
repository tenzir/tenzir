//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/url.hpp"
#include "tenzir/detail/zip_iterator.hpp"
#include "tenzir/si_literals.hpp"
#include "tenzir/tql2/eval.hpp"
#include "tenzir/tql2/exec.hpp"
#include "tenzir/tql2/plugin.hpp"

#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

// Include our compatibility header for Boost < 1.86
#include <boost/version.hpp>
#if BOOST_VERSION < 108600 || defined(TENZIR_FORCE_BOOST_UUID_COMPAT)
#  include <tenzir/detail/boost_uuid_generators.hpp>
#endif

#include <mutex>
#include <ranges>
#include <string_view>

// TODO: This implementation is a rough sketch and needs some cleanup eventually.

namespace tenzir::plugins::to_hive {

namespace {

struct operator_args {
  friend auto inspect(auto& f, operator_args& x) -> bool {
    return f.object(x).fields(f.field("uri", x.uri), f.field("by", x.by),
                              f.field("extension", x.extension),
                              f.field("writer", x.writer),
                              f.field("timeout", x.timeout),
                              f.field("max_size", x.max_size));
  }

  located<std::string> uri;
  std::vector<ast::field_path> by;
  std::string extension;
  pipeline writer;
  duration timeout{};
  uint64_t max_size{};
};

// Thread-safe UUIDv7 generator for unique file naming
namespace {
std::mutex uuid_mutex;
boost::uuids::time_generator_v7 uuid_gen;
} // namespace

// TODO: Don't we have this already?
auto is_empty(const table_slice& x) -> bool {
  return x.rows() == 0;
}

auto is_empty(const chunk_ptr& x) -> bool {
  return not x || x->size() == 0;
}

auto is_empty(std::monostate) -> bool {
  return true;
}

template <class Input, class Output>
class pipe_wrapper {
public:
  explicit pipe_wrapper(pipeline pipe, operator_control_plane& ctrl)
    : input_{std::make_unique<std::optional<Input>>(Input{})},
      pipe_{std::move(pipe)} {
    // TODO: Can this fail?
    auto gen = pipe_.instantiate(
      std::invoke(
        [](std::optional<Input>& input) -> generator<Input> {
          while (input) {
            co_yield std::exchange(*input, {});
          }
        },
        *input_),
      ctrl);
    TENZIR_ASSERT(gen);
    auto cast = std::get_if<generator<Output>>(&*gen);
    TENZIR_ASSERT(cast, fmt::format("expected pipeline {:?} to return {}",
                                    pipe_, operator_type_name<Output>()));
    gen_ = std::move(*cast);
  }

  auto feed(Input input) -> Output {
    // TODO: Concrete example where this can fail: When the schema varies and we
    // feed `parquet` here, the printer can report can error and be done. This
    // still works in tests, but we very likely need some special handling here.
    TENZIR_ASSERT(input_);
    TENZIR_ASSERT(*input_);
    TENZIR_ASSERT(is_empty(**input_));
    TENZIR_ASSERT(not gen_.exhausted());
    *input_ = std::move(input);
    while (true) {
      TENZIR_TRACE("advancing generator");
      auto output = gen_.next();
      TENZIR_ASSERT(output);
      TENZIR_ASSERT(not gen_.exhausted());
      TENZIR_ASSERT(input_);
      TENZIR_ASSERT(*input_);
      if (is_empty(**input_)) {
        // TODO: We do not really know that we immediately get the output. In
        // general, this model is a bit questionable.
        return std::move(*output);
      }
      TENZIR_TRACE("continue iterating because input was not taken");
      TENZIR_ASSERT(is_empty(*output));
    }
  }

  // TODO: Deduplicate these functions.
  [[nodiscard]] auto run_to_completion() -> std::vector<Output>
    requires(not std::same_as<Output, std::monostate>)
  {
    auto output = std::vector<Output>{};
    TENZIR_ASSERT(input_);
    TENZIR_ASSERT(*input_);
    TENZIR_ASSERT(is_empty(**input_));
    input_->reset();
    while (auto next = gen_.next()) {
      if (not is_empty(*next)) {
        output.push_back(std::move(*next));
      }
    }
    TENZIR_ASSERT(gen_.exhausted());
    return output;
  }

  void run_to_completion()
    requires std::same_as<Output, std::monostate>
  {
    TENZIR_ASSERT(input_);
    TENZIR_ASSERT(*input_);
    TENZIR_ASSERT(is_empty(**input_));
    input_->reset();
    while (auto next = gen_.next()) {
    }
    TENZIR_ASSERT(gen_.exhausted());
  }

private:
  // TODO: Destruction order?
  // Empty optional signals completion.
  std::unique_ptr<std::optional<Input>> input_;
  pipeline pipe_;
  generator<Output> gen_;
};

struct group_t {
  group_t(pipeline write, pipeline save, operator_control_plane& ctrl)
    : write{std::move(write), ctrl}, save{std::move(save), ctrl} {
  }

  void run_to_completion() {
    for (auto chunk : write.run_to_completion()) {
      save.feed(std::move(chunk));
    }
    save.run_to_completion();
  }

  time created = time::clock::now();
  size_t bytes_written = 0;
  pipe_wrapper<table_slice, chunk_ptr> write;
  pipe_wrapper<chunk_ptr, std::monostate> save;
};

// TODO: No need to recompute.
// TODO: This name might not be the best.
auto selector_to_name(const ast::field_path& sel) -> std::string {
  auto path = sel.path() | std::views::transform(&ast::field_path::segment::id)
              | std::views::transform(&ast::identifier::name);
  return fmt::to_string(fmt::join(path, "."));
}

// TODO: Un-copy-paste this?
auto remove_columns(const table_slice& slice,
                    std::span<const ast::field_path> selectors) -> table_slice {
  auto transformations = std::vector<indexed_transformation>{};
  for (auto& sel : selectors) {
    auto resolved = resolve(sel, slice.schema());
    std::move(resolved).match(
      [&](offset off) {
        transformations.emplace_back(
          std::move(off),
          [](struct record_type::field, std::shared_ptr<arrow::Array>) {
            return indexed_transformation::result_type{};
          });
      },
      [&](const resolve_error&) {});
  }
  return transform_columns(slice, std::move(transformations));
}

auto extend_url_path(boost::urls::url url_view, std::string_view path)
  -> std::string {
  auto extended_path
    = std::filesystem::path{url_view.path()}.concat(path).lexically_normal();
  auto url = boost::urls::url{url_view};
  url.set_path(extended_path.string());
  return fmt::to_string(url);
}

auto get_extension(std::string_view method_name) -> std::string {
  if (method_name == "brotli") {
    return "br";
  }
  if (method_name == "bz2") {
    return "bz2";
  }
  if (method_name == "gzip") {
    return "gz";
  }
  if (method_name == "lz4") {
    return "lz4";
  }
  if (method_name == "zstd") {
    return "zst";
  }
  return {};
}

auto make_saver(located<std::string_view> url, diagnostic_handler& dh)
  -> failure_or<pipeline> {
  // We need our own diagnostic handler here, as `parse_and_compile` will refer
  // to locations in this pipeline.
  auto collecter = collecting_diagnostic_handler{};
  auto provider = session_provider::make(collecter);
  auto ctx = provider.as_session();
  auto saver
    = parse_and_compile(fmt::format("to {:?} {{ pass }}", url.inner), ctx);
  for (auto&& diag : std::move(collecter).collect()) {
    for (auto& annotation : diag.annotations) {
      annotation.source = url.source;
    }
    dh.emit(std::move(diag));
  }
  return saver;
}

class to_hive final : public crtp_operator<to_hive> {
public:
  to_hive() = default;

  explicit to_hive(operator_args args) : args_{std::move(args)} {
  }

  auto
  operator()(generator<table_slice> input, operator_control_plane& ctrl) const
    -> generator<std::monostate> {
    // TODO: This should check whether the root directory is empty first and at
    // least produce a warning in that case.
    // TODO: Using `data` is not optimal, but okay for now.
    auto groups = std::unordered_map<data, group_t>{};
    auto base_url = boost::urls::parse_uri_reference(args_.uri.inner);
    TENZIR_ASSERT(base_url);
    auto process = [&](table_slice slice) {
      auto by = std::vector<multi_series>{};
      for (auto& sel : args_.by) {
        auto values = eval(sel.inner(), slice, ctrl.diagnostics());
        by.push_back(std::move(values));
      }
      slice = remove_columns(slice, args_.by);
      auto find_or_create_group
        = [&](int64_t row) -> decltype(groups)::iterator {
        auto key = list{};
        for (auto& partition_point : by) {
          TENZIR_ASSERT(row < partition_point.length());
          key.push_back(materialize(partition_point.value_at(row)));
        }
        auto key_data = data{std::move(key)};
        auto it = groups.find(key_data);
        if (it == groups.end()) {
          TENZIR_TRACE("creating group for: {}", key_data);
          auto relative_path = std::string{};
          for (auto [sel, data] :
               detail::zip_equal(args_.by, as<list>(key_data))) {
            auto f = detail::overload{
              [](int64_t x) {
                return fmt::to_string(x);
              },
              [](std::string& x) {
                return x;
              },
              [&](auto&) {
                // TODO: How to stringify everything else?
                return fmt::to_string(data);
              },
            };
            relative_path
              += fmt::format("/{}={}", selector_to_name(sel), match(data, f));
          }
          auto uuid = std::invoke([&] {
            auto lock = std::lock_guard{uuid_mutex};
            return uuid_gen();
          });
          relative_path += fmt::format("/{}.{}", boost::uuids::to_string(uuid),
                                       args_.extension);
          auto partitioned_url = extend_url_path(*base_url, relative_path);
          TENZIR_TRACE("creating saver with path {}", partitioned_url);
          // TODO: Even though we check this before with a test URL, this can
          // still fail afterwards in theory.
          auto saver = make_saver({partitioned_url, args_.uri.source},
                                  ctrl.diagnostics());
          TENZIR_ASSERT(saver);
          it = groups.emplace_hint(it, std::move(key_data),
                                   group_t{args_.writer, std::move(*saver),
                                           ctrl});
        };
        return it;
      };
      auto rows = detail::narrow<int64_t>(slice.rows());
      TENZIR_ASSERT(rows > 0);
      auto current_start = int64_t{0};
      auto current_group = &find_or_create_group(0)->second;
      // We add a "virtual row" that always flushes.
      for (auto row = int64_t{0}; row < rows + 1; ++row) {
        auto it = row == rows ? groups.end() : find_or_create_group(row);
        TENZIR_TRACE("row {} lands at {}", row,
                     it == groups.end() ? nullptr : fmt::ptr(&it->second));
        if (it != groups.end() && &it->second == current_group) {
          continue;
        }
        TENZIR_ASSERT(current_group);
        auto& flush_group = *current_group;
        current_group = &it->second;
        TENZIR_TRACE("detected change - writing {} rows", row - current_start);
        // TODO: Instead of writing the subslice directly, we could first
        // collect all slices for that partition and then write once afterwards.
        // This will probably be significantly more efficient when the partition
        // changes with high frequency.
        auto chunk
          = flush_group.write.feed(subslice(slice, current_start, row));
        current_start = row;
        if (chunk) {
          flush_group.bytes_written += chunk->size();
          TENZIR_TRACE("saving {} bytes", chunk->size());
          flush_group.save.feed(std::move(chunk));
          TENZIR_TRACE("saving done");
        }
        if (flush_group.bytes_written > args_.max_size) {
          TENZIR_TRACE("ending group because of size limit");
          flush_group.run_to_completion();
          // TODO: Make this nice. We iterate and search for the pointer right
          // now because we would invalidate the iterator if we held one.
          for (auto it = groups.begin(); it != groups.end();) {
            if (&it->second == &flush_group) {
              it = groups.erase(it);
            } else {
              ++it;
            }
          }
        }
      }
      TENZIR_TRACE("done processing slice");
    };
    for (auto&& slice : input) {
      // TODO: Not iterate all groups every iteration?
      auto now = time::clock::now();
      for (auto it = groups.begin(); it != groups.end();) {
        auto& group = it->second;
        if (now - group.created > args_.timeout) {
          group.run_to_completion();
          it = groups.erase(it);
          continue;
        }
        ++it;
      }
      if (slice.rows() == 0) {
        co_yield {};
        continue;
      }
      process(std::move(slice));
      co_yield {};
    }
    for (auto& [_, group] : groups) {
      group.run_to_completion();
    }
  }

  auto name() const -> std::string override {
    return "to_hive";
  }

  auto location() const -> operator_location override {
    return operator_location::local;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    TENZIR_UNUSED(filter, order);
    return do_not_optimize(*this);
  }

  friend auto inspect(auto& f, to_hive& x) -> bool {
    return f.apply(x.args_);
  }

  operator_args args_;
};

class plugin : public virtual operator_plugin2<to_hive> {
public:
  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    using namespace si_literals;
    using namespace std::literals;
    auto uri = located<std::string>{};
    auto by_expr = ast::expression{};
    auto timeout = std::optional<located<duration>>{};
    auto max_size = std::optional<located<uint64_t>>{};
    auto format = located<std::string>{};
    auto compression = std::optional<located<std::string>>{};
    TRY(argument_parser2::operator_(name())
          .positional("uri", uri)
          .named("partition_by", by_expr, "list<field>")
          .named("format", format)
          .named("compression", compression)
          .named("timeout", timeout)
          .named("max_size", max_size)
          .parse(inv, ctx));
    auto by_list = std::get_if<ast::list>(&*by_expr.kind);
    if (not by_list) {
      diagnostic::error("expected a list of selector")
        .primary(by_expr)
        .emit(ctx);
      return failure::promise();
    }
    auto by = std::vector<ast::field_path>{};
    by.reserve(by_list->items.size());
    for (auto& item : by_list->items) {
      auto expr = std::get_if<ast::expression>(&item);
      if (not expr) {
        diagnostic::error("expected a selector")
          .primary(into_location(item))
          .emit(ctx);
        return failure::promise();
      }
      auto sel = ast::field_path::try_from(*expr);
      if (not sel) {
        diagnostic::error("expected a selector").primary(item).emit(ctx);
        return failure::promise();
      }
      by.push_back(std::move(*sel));
    }
    if (timeout && timeout->inner <= duration::zero()) {
      diagnostic::error("timeout must be positive").primary(*timeout).emit(ctx);
      return failure::promise();
    }
    // TODO: `json` should be `ndjson` (probably not only here).
    auto writer_definition = fmt::format(
      "write {}", format.inner == "json" ? "json -c" : format.inner);
    if (compression) {
      fmt::format_to(std::back_inserter(writer_definition), "| compress \"{}\"",
                     compression->inner);
    }
    auto writer = pipeline::internal_parse(writer_definition);
    if (not writer) {
      // TODO: This could also be a different error (e.g., for `xsv`).
      diagnostic::error("invalid format `{}`", format.inner)
        .primary(format)
        .emit(ctx);
      return failure::promise();
    }
    auto url_view = boost::urls::parse_uri_reference(uri.inner);
    if (not url_view) {
      diagnostic::error("invalid URL `{}`", uri.inner).primary(uri).emit(ctx);
      return failure::promise();
    }
    auto test_uri = extend_url_path(
      *url_view, fmt::format("/__partitions__/0.{}", format.inner));
    TRY(make_saver({test_uri, uri.source}, ctx));
    if (format.inner == "parquet" && max_size) {
      // TODO: This is not great.
      diagnostic::error(
        "`max_size` is not yet supported by the `parquet` format")
        .primary(*max_size)
        .emit(ctx);
      return failure::promise();
    }
    auto extension = format.inner;
    if (compression) {
      extension
        = fmt::format("{}.{}", format.inner, get_extension(compression->inner));
    }
    return std::make_unique<to_hive>(operator_args{
      .uri = located{fmt::to_string(*url_view), uri.source},
      .by = std::move(by),
      // TODO: Not always right.
      .extension = std::move(extension),
      .writer = std::move(*writer),
      .timeout = timeout ? timeout->inner : 5min,
      .max_size = max_size ? max_size->inner : 100_M,
    });
  }
};

} // namespace

} // namespace tenzir::plugins::to_hive

TENZIR_REGISTER_PLUGIN(tenzir::plugins::to_hive::plugin)
