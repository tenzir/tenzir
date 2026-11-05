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
  for (auto const& stage : stages) {
    match(
      stage.kind,
      [&](ir::QueryStage::Filter const& x) {
        auto refs = ast::collect_refs(x.predicate);
        if (not refs) {
          return;
        }
        for (auto const& path : refs->field_paths) {
          if (not path.path().empty()) {
            add(std::vector{std::string{path.path().front().id.name}});
          }
        }
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
          if (column.input) {
            add(*column.input);
          }
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

/// Appends the operands of the conjunction `expr` to `result`.
auto flatten_conjunction(ast::expression const& expr,
                         std::vector<Ref<ast::expression const>>& result)
  -> void {
  if (auto const* x = try_as<ast::binary_expr>(expr);
      x and x->op == ast::binary_op::and_) {
    flatten_conjunction(x->left, result);
    flatten_conjunction(x->right, result);
    return;
  }
  result.emplace_back(expr);
}

/// Returns the field path of `expr` without `?`, or `None`.
auto plain_path(ast::expression const& expr)
  -> Option<std::vector<std::string>> {
  TRY(auto path, ast::field_path::try_from(expr));
  auto result = std::vector<std::string>{};
  for (auto const& segment : path.path()) {
    if (segment.has_question_mark) {
      return None{};
    }
    result.push_back(segment.id.name);
  }
  if (result.empty()) {
    return None{};
  }
  return result;
}

/// Builds a `PipeQuery` stage by stage.
class PipeBuilder {
public:
  PipeBuilder(PipeQueryDialect const& dialect, PipeOptions const& options,
              std::vector<std::string> names)
    : dialect_{dialect}, options_{options}, names_{std::move(names)} {
  }

  auto add(ir::QueryStage const& stage) -> bool {
    return match(stage.kind, [&](auto const& x) {
      return apply(x);
    });
  }

  auto finish() && -> PipeQuery {
    auto fields = Option<LayoutFields>{};
    if (fields_) {
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
      },
      .aggregates = aggregates_,
      .counts = std::move(counts_),
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
      if (path.size() != 1 or std::ranges::contains(excluded_, path.front())) {
        return None{};
      }
      return path.front();
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
  /// which is `column` itself or an alias in `aliases`.
  auto nameable(std::string const& column, std::vector<PipeAlias>& aliases)
    -> std::string {
    if (dialect_->name_column(column)) {
      return column;
    }
    for (auto const& alias : aliases) {
      if (alias.input == column) {
        return alias.name;
      }
    }
    auto name = fresh("column");
    aliases.push_back({.name = name, .input = column});
    return name;
  }

  /// Narrows the time window by `conjunct`, if it compares the time column
  /// with a time literal.
  auto narrow(ast::expression const& conjunct) -> void {
    auto const* x = try_as<ast::binary_expr>(conjunct);
    if (not x) {
      return;
    }
    using enum ast::binary_op;
    auto op = x->op;
    auto const* field = &x->left;
    auto const* constant = try_as<ast::constant>(x->right);
    if (not constant) {
      constant = try_as<ast::constant>(x->left);
      field = &x->right;
      switch (op) {
        case lt:
          op = gt;
          break;
        case leq:
          op = geq;
          break;
        case gt:
          op = lt;
          break;
        case geq:
          op = leq;
          break;
        default:
          break;
      }
    }
    auto const* instant = constant ? try_as<time>(constant->value) : nullptr;
    if (not instant) {
      return;
    }
    auto path = plain_path(*field);
    if (not path) {
      return;
    }
    auto column = bind(*path);
    if (not column or *column != *options_->time_column) {
      return;
    }
    auto const lower = [&] {
      if (not window_.begin or *window_.begin < *instant) {
        window_.begin = *instant;
      }
    };
    auto const upper = [&] {
      if (not window_.end or *window_.end > *instant) {
        window_.end = *instant;
      }
    };
    switch (op) {
      case eq:
        lower();
        upper();
        break;
      case gt:
      case geq:
        lower();
        break;
      case lt:
      case leq:
        upper();
        break;
      default:
        break;
    }
  }

  auto apply(ir::QueryStage::Filter const& x) -> bool {
    auto conjuncts = std::vector<Ref<ast::expression const>>{};
    flatten_conjunction(x.predicate, conjuncts);
    if (narrowing_ and options_->time_column) {
      for (auto const& conjunct : conjuncts) {
        narrow(*conjunct);
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
      for (auto const& conjunct : conjuncts) {
        if (auto term = dialect_->base_term(*conjunct)) {
          base_terms_.push_back(std::move(*term));
          continue;
        }
        auto rendered = renderer().predicate(*conjunct);
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
    fields_ = std::move(output);
    excluded_.clear();
    return true;
  }

  auto apply(ir::QueryStage::Drop const& x) -> bool {
    leading_ = false;
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
    auto counts = std::vector<std::string>{};
    for (auto const& column : x.columns) {
      if (not column.function) {
        if (not column.input) {
          return false;
        }
        auto input = bind(*column.input);
        if (not input) {
          return false;
        }
        auto key = nameable(*input, aliases);
        if (not std::ranges::contains(keys, key)) {
          keys.push_back(key);
        }
        output.push_back({.path = column.output, .column = std::move(key)});
        continue;
      }
      auto arg = Option<std::string>{};
      if (column.input) {
        auto input = bind(*column.input);
        if (not input) {
          return false;
        }
        arg = nameable(*input, aliases);
      }
      auto call = dialect_->render_aggregate(
        *column.function, arg ? Option<std::string_view>{*arg} : None{});
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
        if (*column.function == "count") {
          counts.push_back(name);
        }
      }
      output.push_back({.path = column.output, .column = std::move(name)});
    }
    if (functions.empty()) {
      // A summary of group keys only, or of nothing, still needs a function.
      auto call = dialect_->render_aggregate("count", None{});
      if (not call) {
        return false;
      }
      functions.push_back({.name = fresh("count"), .call = std::move(*call)});
    }
    auto text = dialect_->aggregate_stage(aliases, functions, keys);
    if (not text) {
      return false;
    }
    stages_ += *text;
    fields_ = std::move(output);
    excluded_.clear();
    counts_.append_range(std::move(counts));
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
      if (not dialect_->name_column(*column)) {
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
  std::vector<std::string> counts_;
  TimeWindow window_;
  Option<std::string> schema_name_;
};

} // namespace

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
