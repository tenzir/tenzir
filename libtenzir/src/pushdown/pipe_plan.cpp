//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pushdown/pipe_plan.hpp"

#include "tenzir/detail/string.hpp"
#include "tenzir/ref.hpp"
#include "tenzir/try.hpp"
#include "tenzir/variant_traits.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <utility>

namespace tenzir::pushdown {

namespace {

/// Whether `names` contains `name`, compared without regard to ASCII case,
/// since a target may resolve names either way.
auto contains_name(std::span<std::string const> names, std::string_view name)
  -> bool {
  return std::ranges::any_of(names, [&](std::string const& other) {
    return detail::ascii_icase_equal(other, name);
  });
}

/// Adds the top-level names that `stages` reference to `names`.
auto collect_names(std::span<ir::QueryStage const> stages)
  -> std::vector<std::string> {
  auto result = std::vector<std::string>{};
  auto const add = [&](std::span<std::string const> path) {
    if (not path.empty() and not contains_name(result, path.front())) {
      result.push_back(path.front());
    }
  };
  auto const add_refs = [&](ast::expression const& expr) {
    auto refs = ast::collect_refs(expr);
    if (not refs) {
      return;
    }
    for (auto const& path : refs->field_paths) {
      if (not path.path().empty()) {
        add(std::vector{std::string{path.path().front().id.name}});
      }
    }
  };
  for (auto const& stage : stages) {
    match(
      stage.kind,
      [&](ir::QueryStage::Filter const& x) {
        add_refs(x.predicate);
      },
      [&](ir::QueryStage::Select const& x) {
        for (auto const& field : x.fields) {
          add(field.input);
          add(field.output);
        }
      },
      [&](ir::QueryStage::Drop const& x) {
        for (auto const& path : x.fields) {
          add(path);
        }
      },
      [&](ir::QueryStage::Aggregate const& x) {
        for (auto const& column : x.columns) {
          add(column.output);
          std::ranges::for_each(column.args, add_refs);
        }
      },
      [&](ir::QueryStage::Sort const& x) {
        for (auto const& key : x.keys) {
          add(key.field);
        }
      },
      [](ir::QueryStage::Limit const&) {});
  }
  return result;
}

/// Whether `expr` is the top-level field `name`.
auto is_field(ast::expression const& expr, std::string_view name) -> bool {
  auto path = ir::field_segments(expr);
  return path and path->size() == 1 and path->front() == name;
}

/// Whether `expr` holds a time literal.
auto mentions_time(ast::expression const& expr) -> bool {
  if (auto const* constant = try_as<ast::constant>(expr)) {
    if (is<time>(constant->value)) {
      return true;
    }
    // The optimizer folds a list of constants into one constant.
    if (auto const* xs = try_as<tenzir::list>(constant->value)) {
      return std::ranges::any_of(*xs, [](auto const& x) {
        return is<time>(x);
      });
    }
    return false;
  }
  if (auto const* x = try_as<ast::binary_expr>(expr)) {
    return mentions_time(x->left) or mentions_time(x->right);
  }
  if (auto const* x = try_as<ast::unary_expr>(expr)) {
    return mentions_time(x->expr);
  }
  if (auto const* x = try_as<ast::function_call>(expr)) {
    return std::ranges::any_of(x->args, [](ast::expression const& arg) {
      return mentions_time(arg);
    });
  }
  if (auto const* x = try_as<ast::list>(expr)) {
    return std::ranges::any_of(x->items, [](auto const& item) {
      auto const* element = try_as<ast::expression>(item);
      return element and mentions_time(*element);
    });
  }
  return false;
}

/// Whether every time literal in `expr` is an operand of a comparison with
/// the top-level field `name`, or of a test of its membership in a list.
auto times_compare_with(ast::expression const& expr, std::string_view name)
  -> bool {
  if (try_as<ast::constant>(expr)) {
    // Also a list of constants, which the optimizer folds into one constant.
    return not mentions_time(expr);
  }
  if (auto const* x = try_as<ast::binary_expr>(expr)) {
    using enum ast::binary_op;
    if (x->op == in and mentions_time(x->right)) {
      return is_field(x->left, name);
    }
    auto const comparison = x->op == eq or x->op == neq or x->op == lt
                            or x->op == leq or x->op == gt or x->op == geq;
    for (auto const* side : {&x->left, &x->right}) {
      auto const* constant = try_as<ast::constant>(*side);
      if (not constant or not is<time>(constant->value)) {
        continue;
      }
      auto const& other = side == &x->left ? x->right : x->left;
      return comparison and is_field(other, name);
    }
    return times_compare_with(x->left, name)
           and times_compare_with(x->right, name);
  }
  if (auto const* x = try_as<ast::unary_expr>(expr)) {
    return times_compare_with(x->expr, name);
  }
  if (auto const* x = try_as<ast::function_call>(expr)) {
    return std::ranges::all_of(x->args, [&](ast::expression const& arg) {
      return times_compare_with(arg, name);
    });
  }
  if (auto const* x = try_as<ast::list>(expr)) {
    return std::ranges::all_of(x->items, [&](auto const& item) {
      auto const* element = try_as<ast::expression>(item);
      return not element or times_compare_with(*element, name);
    });
  }
  return true;
}

/// Builds a `PipeQuery` stage by stage.
class PipeBuilder {
public:
  PipeBuilder(PipeQueryDialect const& dialect, PipeOptions const& options,
              std::vector<std::string> names)
    : dialect_{dialect},
      options_{options},
      names_{std::move(names)},
      native_time_{options.time_column and options.native_time} {
    if (native_time_) {
      decode_.emplace_back(*options.time_column, Decode::time);
    }
  }

  auto add(ir::QueryStage const& stage) -> bool {
    return match(stage.kind, [&](auto const& x) {
      return apply(x);
    });
  }

  auto finish() && -> PipeQuery {
    auto fields = Option<LayoutFields>{};
    if (fields_) {
      keep_decode(*fields_, {});
      fields.emplace();
      for (auto& entry : *fields_) {
        fields->emplace_back(std::move(entry.path), std::move(entry.column));
      }
    }
    return PipeQuery{
      .base_terms = std::move(base_terms_),
      .stages = std::move(stages_),
      .layout = Layout{
        .fields = std::move(fields),
        .hidden = {},
        .schema_name = std::move(schema_name_),
        .decode = std::move(decode_),
      },
      .aggregates = aggregates_,
      .window = window_,
      .names = std::move(names_),
    };
  }

private:
  /// A field of an enumerated output, with the column that holds it.
  struct Entry {
    std::vector<std::string> path;
    std::string column;
  };

  /// Introduces a name for the query.
  auto fresh(std::string_view kind) -> std::string {
    while (true) {
      auto name = dialect_->generated_name(kind, counter_++);
      if (not contains_name(names_, name)) {
        names_.push_back(name);
        return name;
      }
    }
  }

  /// Returns the column that holds the field at `path`.
  auto bind(std::span<std::string const> path) const -> Option<std::string> {
    if (not fields_) {
      if (std::ranges::contains(excluded_, path.front())) {
        return None{};
      }
      TRY(auto column, dialect_->bind_path(path));
      if (std::ranges::contains(excluded_, column)) {
        return None{};
      }
      return column;
    }
    for (auto const& entry : *fields_) {
      if (std::ranges::equal(entry.path, path)) {
        return entry.column;
      }
    }
    return None{};
  }

  auto renderer() const -> ExpressionRenderer {
    return ExpressionRenderer{
      *dialect_,
      [this](std::span<std::string const> path) -> Option<std::string> {
        TRY(auto column, bind(path));
        return dialect_->bind_column(column);
      }};
  }

  /// Returns a column that a command can name for the value of `column`,
  /// which is `column` itself or an alias in `aliases`. The stage assigns its
  /// aliases, so it names them, too.
  auto nameable(std::string const& column, std::vector<PipeAlias>& aliases)
    -> std::string {
    if (dialect_->name_column(column)
        or std::ranges::contains(aliases, column, &PipeAlias::name)) {
      return column;
    }
    for (auto const& alias : aliases) {
      if (alias.input == column) {
        return alias.name;
      }
    }
    auto name = fresh("column");
    aliases.push_back({.name = name, .input = column, .value = None{}});
    return name;
  }

  /// Returns the column that holds the field `expr`, or else an alias in
  /// `aliases` that computes it, without time literals, which compare only
  /// with the time column in a filter.
  auto column_of(ast::expression const& expr, std::vector<PipeAlias>& aliases)
    -> Option<std::string> {
    if (auto path = ir::field_segments(expr)) {
      return bind(*path);
    }
    if (mentions_time(expr)) {
      return None{};
    }
    TRY(auto value, renderer().value(expr));
    auto it = std::ranges::find(aliases, Option{value}, &PipeAlias::value);
    if (it != aliases.end()) {
      return it->name;
    }
    auto name = fresh("column");
    aliases.push_back({.name = name, .input = {}, .value = std::move(value)});
    return name;
  }

  /// Updates whether the field named after the time column holds the
  /// target's own time, after a stage that enumerates its output. It does so
  /// only if one of `fields`, as pairs of output and input paths, keeps it.
  template <class Fields>
  auto keep_native_time(Fields const& fields) -> void {
    if (not native_time_) {
      return;
    }
    auto const time = std::vector{*options_->time_column};
    native_time_ = std::ranges::any_of(fields, [&](auto const& field) {
      return field.first == time and field.second == time;
    });
  }

  /// Keeps the columns of `output` that hold a column that does not decode
  /// natively, either themselves or as an alias of one in `aliases`.
  auto keep_decode(std::span<Entry const> output,
                   std::span<PipeAlias const> aliases) -> void {
    auto result = std::vector<Decoded>{};
    for (auto const& entry : output) {
      auto const alias
        = std::ranges::find(aliases, entry.column, &PipeAlias::name);
      auto const& source = alias != aliases.end() ? alias->input : entry.column;
      auto const it = std::ranges::find(decode_, source, &Decoded::first);
      if (it != decode_.end()
          and not std::ranges::contains(result, entry.column,
                                        &Decoded::first)) {
        result.emplace_back(entry.column, it->second);
      }
    }
    decode_ = std::move(result);
  }

  auto apply(ir::QueryStage::Filter const& x) -> bool {
    if (options_->time_column) {
      // Only the target's own time compares with a time literal.
      if (not times_compare_with(x.predicate, *options_->time_column)
          or (not native_time_ and mentions_time(x.predicate))) {
        return false;
      }
      // The comparisons narrow the window while the field named after the
      // time column holds the target's own time.
      auto const column = bind(std::vector{*options_->time_column});
      if (narrowing_ and options_->narrow and native_time_ and column
          and std::ranges::contains(decode_, Decoded{*column, Decode::time})) {
        window_ = intersect(window_,
                            time_window(x.predicate, *options_->time_column));
      }
    }
    auto predicate = Option<std::string>{};
    if (leading_ and options_->extend_base) {
      // Each conjunct either extends the base query or stays in the filter
      // stage, which keeps the others whole.
      auto const and_ = dialect_->binary_operator(ast::binary_op::and_);
      if (not and_) {
        return false;
      }
      for (auto const& conjunct : ir::split_conjunction(x.predicate)) {
        if (auto term = dialect_->base_term(conjunct)) {
          base_terms_.push_back(std::move(*term));
          continue;
        }
        auto rendered = renderer().predicate(conjunct);
        if (not rendered) {
          return false;
        }
        predicate = predicate
                      ? fmt::format("({} {} {})", *predicate, *and_, *rendered)
                      : std::move(*rendered);
      }
      if (not predicate) {
        return true;
      }
    } else {
      predicate = renderer().predicate(x.predicate);
      if (not predicate) {
        return false;
      }
    }
    auto text = dialect_->filter_stage(*predicate);
    if (not text) {
      return false;
    }
    stages_ += *text;
    return true;
  }

  auto apply(ir::QueryStage::Select const& x) -> bool {
    leading_ = false;
    if (x.fields.empty()) {
      return false;
    }
    // Every input refers to the fields before the stage.
    auto inputs = std::vector<std::string>{};
    for (auto const& field : x.fields) {
      auto column = bind(field.input);
      if (not column) {
        return false;
      }
      inputs.push_back(std::move(*column));
    }
    auto aliases = std::vector<PipeAlias>{};
    auto columns = std::vector<std::string>{};
    auto output = std::vector<Entry>{};
    for (auto i = size_t{0}; i < x.fields.size(); ++i) {
      auto column = nameable(inputs[i], aliases);
      if (not std::ranges::contains(columns, column)) {
        columns.push_back(column);
      }
      output.push_back({.path = x.fields[i].output, .column = column});
    }
    auto text = dialect_->select_stage(aliases, columns);
    if (not text) {
      return false;
    }
    stages_ += *text;
    if (options_->time_column) {
      auto pairs = std::vector<
        std::pair<std::vector<std::string>, std::vector<std::string>>>{};
      for (auto const& field : x.fields) {
        pairs.emplace_back(field.output, field.input);
      }
      keep_native_time(pairs);
    }
    keep_decode(output, aliases);
    fields_ = std::move(output);
    excluded_.clear();
    return true;
  }

  auto apply(ir::QueryStage::Drop const& x) -> bool {
    leading_ = false;
    if (options_->time_column
        and std::ranges::contains(x.fields,
                                  std::vector{*options_->time_column})) {
      native_time_ = false;
    }
    if (fields_) {
      // The layout leaves the dropped fields out.
      for (auto const& path : x.fields) {
        auto const inside = std::ranges::any_of(*fields_, [&](auto& entry) {
          return entry.path.size() < path.size()
                 and std::ranges::equal(
                   entry.path, std::span{path}.first(entry.path.size()));
        });
        if (inside) {
          return false;
        }
        std::erase_if(*fields_, [&](Entry const& entry) {
          return entry.path.size() >= path.size()
                 and std::ranges::equal(
                   path, std::span{entry.path}.first(path.size()));
        });
      }
      // A row without fields may vanish.
      return not fields_->empty();
    }
    auto columns = std::vector<std::string>{};
    for (auto const& path : x.fields) {
      if (path.size() != 1 or not dialect_->name_column(path.front())) {
        return false;
      }
      if (not std::ranges::contains(excluded_, path.front())
          and not std::ranges::contains(columns, path.front())) {
        columns.push_back(path.front());
      }
    }
    if (columns.empty()) {
      return true;
    }
    auto text = dialect_->drop_stage(columns);
    if (not text) {
      return false;
    }
    stages_ += *text;
    std::erase_if(decode_, [&](Decoded const& x) {
      return std::ranges::contains(columns, x.first);
    });
    excluded_.append_range(std::move(columns));
    return true;
  }

  auto apply(ir::QueryStage::Aggregate const& x) -> bool {
    leading_ = false;
    narrowing_ = false;
    auto aliases = std::vector<PipeAlias>{};
    auto functions = std::vector<PipeFunction>{};
    auto keys = std::vector<std::string>{};
    auto output = std::vector<Entry>{};
    auto decode = std::vector<Decoded>{};
    // A field decodes as its column, also as the target's own time.
    auto const decode_of = [&](ast::expression const& arg) -> Option<Decode> {
      auto path = ir::field_segments(arg);
      if (not path) {
        return dialect_->value_decode(arg);
      }
      auto column = bind(*path);
      auto it = column ? std::ranges::find(decode_, *column, &Decoded::first)
                       : decode_.end();
      return it == decode_.end() ? Decode::native : it->second;
    };
    for (auto const& column : x.columns) {
      // A target computes on columns, so a key and a value argument read a
      // column or an alias. A predicate argument renders as for a filter, but
      // without time literals, which compare only with the time column there.
      auto const fill = [&](ast::expression const& arg,
                            bool predicate) -> Option<std::string> {
        if (predicate) {
          return mentions_time(arg) ? Option<std::string>{}
                                    : renderer().predicate(arg);
        }
        TRY(auto input, column_of(arg, aliases));
        return dialect_->name_column(input).or_else([&] {
          return dialect_->bind_column(input);
        });
      };
      if (not column.function) {
        auto input = column_of(column.args.front(), aliases);
        if (not input) {
          return false;
        }
        auto key = nameable(*input, aliases);
        if (not std::ranges::contains(keys, key)) {
          keys.push_back(key);
          auto kind = aggregate_decode(*dialect_, column, decode_of);
          if (not kind) {
            return false;
          }
          if (*kind != Decode::native) {
            decode.emplace_back(key, *kind);
          }
        }
        output.push_back({.path = column.output, .column = std::move(key)});
        continue;
      }
      auto const* row = find_row(dialect_->aggregations(), *column.function,
                                 column.args.size());
      auto call
        = row ? spell(row->spelling, column.args, fill) : Option<std::string>{};
      if (not call) {
        return false;
      }
      // A target may compute a repeated call once, under one of its names.
      auto existing = std::ranges::find(functions, *call, &PipeFunction::call);
      auto name = std::string{};
      if (existing != functions.end()) {
        name = existing->name;
      } else {
        name = fresh("column");
        functions.push_back({.name = name, .call = std::move(*call)});
        auto kind = aggregate_decode(*dialect_, column, decode_of);
        if (kind and *kind != Decode::native) {
          decode.emplace_back(name, *kind);
        }
      }
      output.push_back({.path = column.output, .column = std::move(name)});
    }
    if (functions.empty()) {
      // A summary of group keys only, or of nothing, still needs a function.
      auto const* row = find_row(dialect_->aggregations(), "count", 0);
      if (not row) {
        return false;
      }
      functions.push_back(
        {.name = fresh("count"), .call = std::string{row->spelling}});
    }
    auto text = dialect_->aggregate_stage(aliases, functions, keys);
    if (not text) {
      return false;
    }
    stages_ += *text;
    if (options_->time_column) {
      // A group key keeps the target's own time, and so do its extremes.
      auto pairs = std::vector<
        std::pair<std::vector<std::string>, std::vector<std::string>>>{};
      for (auto const& column : x.columns) {
        auto input = column.args.empty()
                       ? None{}
                       : ir::field_segments(column.args.front());
        if (input
            and (not column.function or *column.function == "min"
                 or *column.function == "max")) {
          pairs.emplace_back(column.output, std::move(*input));
        }
      }
      keep_native_time(pairs);
    }
    fields_ = std::move(output);
    excluded_.clear();
    decode_ = std::move(decode);
    schema_name_ = x.schema_name;
    aggregates_ = true;
    return true;
  }

  auto apply(ir::QueryStage::Sort const& x) -> bool {
    leading_ = false;
    narrowing_ = false;
    if (x.keys.empty() or (x.bound and *x.bound == 0)) {
      return false;
    }
    auto keys = std::vector<PipeSortKey>{};
    for (auto const& key : x.keys) {
      auto column = bind(key.field);
      if (not column) {
        return false;
      }
      keys.push_back(
        {.column = std::move(*column), .descending = key.descending});
    }
    auto text = dialect_->sort_stage(keys, x.bound);
    if (not text) {
      return false;
    }
    stages_ += *text;
    return true;
  }

  auto apply(ir::QueryStage::Limit const& x) -> bool {
    leading_ = false;
    narrowing_ = false;
    auto text = dialect_->limit_stage(x.count);
    if (not text) {
      return false;
    }
    stages_ += *text;
    return true;
  }

  Ref<PipeQueryDialect const> dialect_;
  Ref<PipeOptions const> options_;
  /// The names that the stages reference or introduce.
  std::vector<std::string> names_;
  size_t counter_ = 0;
  /// The enumerated fields of the current output, or `None` if the rows are
  /// the events of the base query.
  Option<std::vector<Entry>> fields_;
  /// The top-level fields that a drop removed from the events.
  std::vector<std::string> excluded_;
  /// Whether only filters came before.
  bool leading_ = true;
  /// Whether no aggregation, sort, or limit came before.
  bool narrowing_ = true;
  std::vector<std::string> base_terms_;
  std::string stages_;
  bool aggregates_ = false;
  /// Whether the field named after the time column holds the target's own
  /// time, as far as the syntax of the stages tells.
  bool native_time_ = false;
  /// The columns of the current output that do not decode natively, such as
  /// those that hold the target's own time.
  std::vector<Decoded> decode_;
  TimeWindow window_;
  Option<std::string> schema_name_;
};

} // namespace

auto PipeQueryDialect::bind_path(std::span<std::string const> path) const
  -> Option<std::string> {
  if (path.size() != 1) {
    return None{};
  }
  return path.front();
}

auto PipeQueryDialect::alias_value(PipeAlias const& alias) const
  -> Option<std::string> {
  return alias.value ? alias.value : bind_column(alias.input);
}

auto PipeQueryDialect::base_term(ast::expression const& conjunct) const
  -> Option<std::string> {
  TENZIR_UNUSED(conjunct);
  return None{};
}

auto fresh_name(PipeQuery& query, PipeQueryDialect const& dialect,
                std::string_view kind) -> std::string {
  for (auto i = size_t{0};; ++i) {
    auto name = dialect.generated_name(kind, i);
    if (not contains_name(query.names, name)) {
      query.names.push_back(name);
      return name;
    }
  }
}

auto render_pipe(std::span<ir::QueryStage const> stages,
                 PipeQueryDialect const& dialect, PipeOptions const& options)
  -> Option<PipeQuery> {
  auto builder = PipeBuilder{dialect, options, collect_names(stages)};
  for (auto const& stage : stages) {
    if (not builder.add(stage)) {
      return None{};
    }
  }
  return std::move(builder).finish();
}

} // namespace tenzir::pushdown
