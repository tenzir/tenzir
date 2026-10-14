//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/lambda.hpp"

#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/broadcast.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/list_array.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <span>
#include <string_view>
#include <utility>
#include <vector>

namespace tenzir::nova {

namespace {

using Path = std::span<ast::field_path::segment const>;

auto is_prefix(Path prefix, Path path) -> bool {
  return prefix.size() <= path.size()
         and std::ranges::equal(prefix, path.first(prefix.size()),
                                [](auto const& lhs, auto const& rhs) {
                                  return lhs.id.name == rhs.id.name;
                                });
}

/// Discovers the free fields of a lambda body, i.e. the field paths it reads
/// from the enclosing input rather than from its own parameters.
class CaptureFinder final : public ast::visitor<CaptureFinder> {
public:
  explicit CaptureFinder(std::vector<std::string> parameters)
    : bound_{std::move(parameters)} {
  }

  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  template <class T>
    requires concepts::one_of<T, ast::root_field, ast::field_access,
                              ast::index_expr>
  auto visit(T& x) -> void {
    if (auto path = ast::field_path::try_from(x)) {
      add(std::move(*path));
    } else {
      enter(x);
    }
  }

  auto visit(ast::this_& x) -> void {
    if (not captures_this_) {
      captures_this_ = ThisTag{x.get_location()};
    }
  }

  auto visit(ast::lambda_expr& x) -> void {
    auto size = bound_.size();
    for (auto const& parameter : x.params) {
      bound_.push_back(parameter.name);
    }
    visit(x.body);
    bound_.resize(size);
  }

  auto visit(ast::assignment& x) -> void {
    // Named argument labels are not input references.
    visit(x.right);
  }

  auto result() && -> LambdaCaptureStorage {
    if (captures_this_) {
      return *captures_this_;
    }
    return std::move(paths_);
  }

private:
  auto add(ast::field_path path) -> void {
    auto const segments = path.path();
    if (segments.empty()
        or std::ranges::find(bound_, segments.front().id.name)
             != bound_.end()) {
      return;
    }
    // A captured path subsumes the paths below it.
    if (std::ranges::any_of(paths_, [&](ast::field_path const& other) {
          return is_prefix(other.path(), segments);
        })) {
      return;
    }
    std::erase_if(paths_, [&](ast::field_path const& other) {
      return is_prefix(segments, other.path());
    });
    paths_.push_back(std::move(path));
  }

  std::vector<std::string> bound_;
  std::vector<ast::field_path> paths_;
  Option<ThisTag> captures_this_;
};

/// Whether a lambda body reads anything that `input` has, in which case it
/// sees the enclosing record.
auto reads_input(LambdaCaptures const& captures, Array<Record> const& input)
  -> bool {
  return match(
    captures,
    [](ThisTag) {
      return true;
    },
    [&](std::span<ast::field_path const> paths) {
      return std::ranges::any_of(paths, [&](ast::field_path const& path) {
        return input.field(path.path().front().id.name).is_some();
      });
    });
}

auto project(Array<Record> const& record, std::span<Path const> paths)
  -> Array<Record>;

/// Restricts the records in `column` to the fields that `paths` lead into.
/// Other values stay as they are, so that the body reports the same
/// diagnostics for them.
auto project(Array<Data> column, std::span<Path const> paths) -> Array<Data> {
  return std::move(column).map_alternative<Record>(
    [&](MaskedArray<Array<Record>> records) -> Array<Record> {
      return project(records.data, paths);
    });
}

/// Like above, for the non-empty `paths` relative to `record`. Records without
/// a field of a path keep their other captured fields.
auto project(Array<Record> const& record, std::span<Path const> paths)
  -> Array<Record> {
  auto fields
    = std::vector<std::pair<std::string_view, Array<Record>::MaskedArray>>{};
  auto below = std::vector<Path>{};
  for (auto const& path : paths) {
    auto const name = std::string_view{path.front().id.name};
    if (std::ranges::any_of(fields, [&](auto const& field) {
          return field.first == name;
        })) {
      continue;
    }
    auto field = record.field(name);
    if (not field) {
      continue;
    }
    below.clear();
    for (auto const& other : paths) {
      if (other.front().id.name == name) {
        below.push_back(other.subspan(1));
      }
    }
    // No path is a prefix of another, so one that ends here is the only one.
    if (not below.front().empty()) {
      field->data = project(std::move(field->data), below);
    }
    fields.emplace_back(name, std::move(*field));
  }
  if (fields.empty()) {
    return Array<Record>::make_empty(record.length());
  }
  return Array<Record>::from_fields(fields);
}

/// The record a lambda body sees in element space: the captured fields of
/// `input`, broadcast from each row selected by `rows` to its elements. A body
/// that reads `this` gets the whole record.
auto broadcast_captures(LambdaCaptures const& captures, Events const& input,
                        std::span<storage::Span const> spans,
                        storage::BitMap const& rows, storage::Index length)
  -> Array<Record> {
  return match(
    captures,
    [&](ThisTag) {
      return broadcast(input.data, spans, rows, length);
    },
    [&](std::span<ast::field_path const> captured) {
      if (captured.empty()) {
        return Array<Record>::make_empty(length);
      }
      auto paths = std::vector<Path>{};
      paths.reserve(captured.size());
      for (auto const& path : captured) {
        paths.push_back(path.path());
      }
      return broadcast(project(input.data, paths), spans, rows, length);
    });
}

} // namespace

auto LambdaArgument::make(ast::lambda_expr& lambda, InstantiateCtx ctx)
  -> failure_or<LambdaArgument> {
  if (not lambda.is_unary()) {
    diagnostic::error("expected unary lambda").primary(lambda).emit(ctx);
    return failure::promise();
  }
  auto captures = CaptureFinder{{lambda.param(0).name}};
  captures.visit(lambda.body);
  return LambdaArgument{std::addressof(lambda), std::move(captures).result()};
}

auto BinaryLambdaArgument::make(ast::lambda_expr& lambda, InstantiateCtx ctx)
  -> failure_or<BinaryLambdaArgument> {
  if (not lambda.is_binary()) {
    diagnostic::error("expected binary lambda").primary(lambda).emit(ctx);
    return failure::promise();
  }
  auto captures
    = CaptureFinder{std::vector{lambda.param(0).name, lambda.param(1).name}};
  captures.visit(lambda.body);
  return BinaryLambdaArgument{std::addressof(lambda),
                              std::move(captures).result()};
}

auto EvalFrame::eval(LambdaArgument const& lambda,
                     MaskedArray<Array<Data>> subject,
                     Events const& input) const -> Array<Data> {
  TENZIR_ASSERT(lambda);
  TENZIR_ASSERT(subject.data.length() == input.length());
  auto const length = subject.data.length();
  subject.present = std::move(subject.present) & input.mask;
  if (subject.present.true_count() == 0) {
    return Array<Data>{Array<Null>{storage::NullStorage{length}}};
  }
  // Like the legacy evaluator, captures retain the enclosing record; the
  // parameter overwrites its field below.
  auto record = reads_input(lambda.captures(), input.data)
                  ? input.data
                  : Array<Record>::make_empty(input.length());
  auto mask = subject.present;
  record = std::move(record).with_field_overwrite(lambda.param(),
                                                  std::move(subject));
  auto events = Events{std::move(record), std::move(mask), input.meta};
  // The body was prepared by the same evaluator, so its call sites resolve in
  // this run's function table; only the input changes.
  auto run = _::EvalRun{*run_->evaluator_, std::addressof(events), run_->ctx_};
  auto frame = EvalFrame{run, events.mask};
  return run.eval(lambda.body(), std::move(frame));
}

auto EvalFrame::eval(BinaryLambdaArgument const& lambda,
                     MaskedArray<Array<Data>> lhs, MaskedArray<Array<Data>> rhs,
                     Events const& input,
                     Option<Ref<diagnostic_handler>> dh) const -> Array<Data> {
  TENZIR_ASSERT(lambda);
  TENZIR_ASSERT_EQ(lhs.data.length(), input.length());
  TENZIR_ASSERT_EQ(rhs.data.length(), input.length());
  auto const length = lhs.data.length();
  auto present = std::move(lhs.present) & std::move(rhs.present) & input.mask;
  if (present.true_count() == 0) {
    return Array<Data>{Array<Null>{storage::NullStorage{length}}};
  }
  lhs.present = present;
  rhs.present = present;
  // Like the unary case, captures retain the enclosing record; the parameters
  // overwrite their fields below.
  auto record = reads_input(lambda.captures(), input.data)
                  ? input.data
                  : Array<Record>::make_empty(input.length());
  record
    = std::move(record).with_field_overwrite(lambda.param(0), std::move(lhs));
  record
    = std::move(record).with_field_overwrite(lambda.param(1), std::move(rhs));
  auto events = Events{std::move(record), std::move(present), input.meta};
  auto ctx = dh ? EvalCtx{**dh} : run_->ctx_;
  auto run = _::EvalRun{*run_->evaluator_, std::addressof(events), ctx};
  auto frame = EvalFrame{run, events.mask};
  return run.eval(lambda.body(), std::move(frame));
}

auto EvalFrame::eval(LambdaArgument const& lambda,
                     MaskedArray<Array<Data>> subject) const -> Array<Data> {
  TENZIR_ASSERT(lambda);
  auto const length = subject.data.length();
  if (subject.present.true_count() == 0) {
    return Array<Data>{Array<Null>{storage::NullStorage{length}}};
  }
  auto const capture_location = match(
    lambda.captures(),
    [](ThisTag capture) -> Option<location> {
      return capture.source;
    },
    [](std::span<ast::field_path const> paths) -> Option<location> {
      if (paths.empty()) {
        return None{};
      }
      return paths.front().get_location();
    });
  if (capture_location) {
    diagnostic::error("expected a constant expression")
      .primary(*capture_location)
      .emit(dh());
    return Array<Data>{Array<Null>{storage::NullStorage{length}}};
  }
  auto events
    = Events{Array<Record>::make_empty(subject.data.length()), subject.present,
             Events::Meta::make_empty(subject.data.length())};
  return eval(lambda, std::move(subject), events);
}

auto EvalFrame::eval_elements(LambdaArgument const& lambda,
                              MaskedArray<Array<List>> const& list,
                              Option<storage::BitMap const&> elements) const
  -> ElementResults {
  TENZIR_ASSERT(lambda);
  auto const primary = list.data.to_primary();
  auto layout = as<storage::ListStorage>(primary.storage());
  auto const rows = mask() & list.present;
  auto filter = elements.map([](storage::BitMap const& x) {
    return x;
  });
  if (filter) {
    TENZIR_ASSERT_EQ(filter->length(), layout.values().length());
  }
  auto spans = std::span<storage::Span const>{
    layout.spans().begin(), static_cast<std::size_t>(layout.length())};
  auto selected = broadcast_mask(spans, rows, layout.values().length());
  auto total = int64_t{0};
  storage::for_each_true(rows, [&](storage::Index row) {
    total += spans[row].end - spans[row].begin;
  });
  if (selected.true_count() != total) {
    // Rows share elements, but every row needs its own results, so each gets
    // its own copy of its elements, one row after another.
    TENZIR_ASSERT_LEQ(total, std::numeric_limits<storage::Index>::max());
    auto ranges = std::vector<storage::Span>{};
    auto owned = storage::DataOwner<storage::Span[]>::make_value(
      layout.length(), storage::Span{0, 0});
    auto offset = storage::Index{0};
    storage::for_each_true(rows, [&](storage::Index row) {
      auto const span = spans[row];
      owned[row] = {offset, offset + (span.end - span.begin)};
      offset += span.end - span.begin;
      ranges.push_back(span);
    });
    if (filter) {
      filter = concatenate_ranges(*filter, ranges);
    }
    layout = storage::ListStorage{std::move(owned),
                                  concatenate_ranges(layout.values(), ranges)};
    spans = {layout.spans().begin(), static_cast<std::size_t>(layout.length())};
    selected = storage::BitMap{offset, true};
  }
  // Element space, not row space: the results have one row per element of
  // the layout, never this frame's length.
  auto const length = layout.values().length();
  if (filter) {
    selected = std::move(selected) & *filter;
  }
  if (not selected.any()) {
    return {
      std::move(layout),
      Array<Data>{Array<Null>{storage::NullStorage{length}}},
    };
  }
  auto subject = MaskedArray<Array<Data>>{layout.values(), selected};
  auto const* input = this->input();
  if (not input) {
    auto results = eval(lambda, std::move(subject));
    return {std::move(layout), std::move(results)};
  }
  auto events = Events{
    broadcast_captures(lambda.captures(), *input, spans, rows, length),
    std::move(selected),
    Events::Meta{
      .name = broadcast(input->meta.name, spans, rows, length),
      .import_time = broadcast(input->meta.import_time, spans, rows, length),
      .internal = broadcast(input->meta.internal, spans, rows, length),
    },
  };
  auto results = eval(lambda, std::move(subject), events);
  return {std::move(layout), std::move(results)};
}

} // namespace tenzir::nova
