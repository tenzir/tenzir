//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova/lambda.hpp"

#include "tenzir/nova/bitmap_iteration.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/list_array.hpp"

#include <algorithm>
#include <unordered_set>
#include <utility>

namespace tenzir::nova {

namespace {

/// Discovers the free fields of a lambda body, i.e. the fields it reads from
/// the enclosing input rather than from its own parameters.
class Captures final : public ast::visitor<Captures> {
public:
  explicit Captures(std::string parameter) : bound_{std::move(parameter)} {
  }

  explicit Captures(std::vector<std::string> parameters)
    : bound_{std::move(parameters)} {
  }

  template <class T>
  auto visit(T& x) -> void {
    enter(x);
  }

  auto visit(ast::root_field& x) -> void {
    add(x.id);
  }

  auto visit(ast::this_&) -> void {
    // `this` reads the enclosing event as a whole, so no single field name
    // describes what the body needs.
    captures_this = true;
  }

  template <class T>
    requires concepts::one_of<T, ast::field_access, ast::index_expr>
  auto visit(T& x) -> void {
    if (auto path = ast::field_path::try_from(x)) {
      add(path->path().front().id);
    } else {
      enter(x);
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

  std::vector<ast::identifier> captures;
  bool captures_this = false;

private:
  auto add(ast::identifier const& id) -> void {
    if (std::ranges::find(bound_, id.name) == bound_.end()
        and seen_.insert(id.name).second) {
      captures.push_back(id);
    }
  }

  std::vector<std::string> bound_;
  std::unordered_set<std::string> seen_;
};

/// The element rows belonging to the rows of `shape` selected by `mask`.
auto expand_mask(storage::BitMap const& mask, storage::ListStorage const& shape)
  -> storage::BitMap {
  if (auto constant = mask.as_constant()) {
    return storage::BitMap{shape.values().length(), *constant};
  }
  auto result = storage::BitMap::Mutable{shape.values().length()};
  for (auto row : storage::true_bits(mask)) {
    auto const& span = shape.spans()[row];
    for (auto index = span.begin; index < span.end; ++index) {
      result.set(index, true);
    }
  }
  return std::move(result).finish();
}

/// Broadcasts a captured column from each row of `shape` to that row's
/// elements, so the body sees the enclosing row's value for every element.
auto expand_capture(MaskedArray<Array<Data>> field,
                    storage::ListStorage const& shape,
                    storage::BitMap const& mask) -> MaskedArray<Array<Data>> {
  auto active = std::move(field.present) & mask;
  auto builder = ArrayBuilder<Data>{};
  for (auto row : storage::true_bits(active)) {
    auto const& [begin, end] = shape.spans()[row];
    if (begin == end) {
      continue;
    }
    builder.skip_n(begin - builder.length());
    auto value = field.data.get(row);
    for (auto index = begin; index < end; ++index) {
      append_row(builder, value);
    }
  }
  builder.skip_n(shape.values().length() - builder.length());
  return {builder.finish(), expand_mask(active, shape)};
}

/// Broadcasts one metadata column from each row of `shape` to that row's
/// elements, mirroring `expand_capture` for the typed metadata arrays.
template <fundamental_type Tag>
auto expand_meta_column(Array<Tag> const& column,
                        storage::ListStorage const& shape) -> Array<Tag> {
  auto builder = ArrayBuilder<Tag>{};
  auto const rows = std::min(column.length(), shape.length());
  for (auto row = storage::Index{0}; row < rows; ++row) {
    auto const& [begin, end] = shape.spans()[row];
    if (begin == end) {
      continue;
    }
    builder.skip_n(begin - builder.length());
    auto const value = *column.get(row);
    for (auto index = begin; index < end; ++index) {
      builder.data(value);
    }
  }
  builder.skip_n(shape.values().length() - builder.length());
  return builder.finish();
}

/// Lifts row-space event metadata into the element space of `shape`, so a
/// lambda body evaluated per element still sees its enclosing event's origin.
auto expand_meta(Events::Meta const& meta, storage::ListStorage const& shape)
  -> Events::Meta {
  return Events::Meta{
    .name = expand_meta_column(meta.name, shape),
    .import_time = expand_meta_column(meta.import_time, shape),
    .internal = expand_meta_column(meta.internal, shape),
  };
}

} // namespace

auto LambdaArgument::make(ast::lambda_expr& lambda, InstantiateCtx ctx)
  -> failure_or<LambdaArgument> {
  if (not lambda.is_unary()) {
    diagnostic::error("expected unary lambda").primary(lambda).emit(ctx);
    return failure::promise();
  }
  auto captures = Captures{lambda.param(0).name};
  captures.visit(lambda.body);
  return LambdaArgument{std::addressof(lambda), std::move(captures.captures)};
}

auto BinaryLambdaArgument::make(ast::lambda_expr& lambda, InstantiateCtx ctx)
  -> failure_or<BinaryLambdaArgument> {
  if (not lambda.is_binary()) {
    diagnostic::error("expected binary lambda").primary(lambda).emit(ctx);
    return failure::promise();
  }
  auto captures
    = Captures{std::vector{lambda.param(0).name, lambda.param(1).name}};
  captures.visit(lambda.body);
  return BinaryLambdaArgument{std::addressof(lambda),
                              std::move(captures.captures),
                              captures.captures_this};
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
  auto record = Array<Record>::make_empty(input.length());
  for (auto const& capture : lambda.captures()) {
    if (input.data.field(capture.name)) {
      // Like the legacy evaluator, captures retain the enclosing record; the
      // parameter overwrites its field below.
      record = input.data;
      break;
    }
  }
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
  auto record = Array<Record>::make_empty(input.length());
  // Like the unary case, captures retain the enclosing record; the parameters
  // overwrite their fields below. A body that reads `this` needs the whole
  // row, which no capture name describes.
  if (lambda.captures_this()) {
    record = input.data;
  } else {
    for (auto const& capture : lambda.captures()) {
      if (input.data.field(capture.name)) {
        record = input.data;
        break;
      }
    }
  }
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
  if (not lambda.captures().empty()) {
    diagnostic::error("expected a constant expression")
      .primary(lambda.captures().front().location)
      .emit(dh());
    return Array<Data>{Array<Null>{storage::NullStorage{length}}};
  }
  auto events
    = Events{Array<Record>::make_empty(subject.data.length()), subject.present,
             Events::Meta::make_empty(subject.data.length())};
  return eval(lambda, std::move(subject), events);
}

auto EvalFrame::eval_elements(LambdaArgument const& lambda,
                              storage::ListStorage const& list) const
  -> Array<Data> {
  return eval_selected_elements(lambda, list, expand_mask(mask(), list));
}

auto EvalFrame::eval_elements(LambdaArgument const& lambda,
                              storage::ListStorage const& list,
                              storage::BitMap const& elements) const
  -> Array<Data> {
  TENZIR_ASSERT_EQ(elements.length(), list.values().length());
  return eval_selected_elements(lambda, list,
                                expand_mask(mask(), list) & elements);
}

auto EvalFrame::eval_selected_elements(LambdaArgument const& lambda,
                                       storage::ListStorage const& list,
                                       storage::BitMap elements) const
  -> Array<Data> {
  TENZIR_ASSERT(lambda);
  auto subject = MaskedArray<Array<Data>>{list.values(), elements};
  // Element space, not row space: the result has one row per list element,
  // so it is sized by `list.values()`, never by this frame's length.
  auto result
    = Array<Data>{Array<Null>{storage::NullStorage{elements.length()}}};
  if (elements.true_count() != 0) {
    auto const* input = this->input();
    if (input) {
      auto record = Array<Record>::make_empty(list.values().length());
      for (auto const& capture : lambda.captures()) {
        if (input->data.field(capture.name)) {
          auto expanded = expand_capture(
            {Array<Data>{input->data}, input->mask}, list, mask());
          auto records = expanded.data.get_alternative<Record>();
          TENZIR_ASSERT(records);
          record = std::move(records->data);
          break;
        }
      }
      auto events
        = Events{std::move(record), elements, expand_meta(input->meta, list)};
      result = eval(lambda, std::move(subject), events);
    } else {
      result = eval(lambda, std::move(subject));
    }
  }
  // The body produced a value for every element of `elements`, so the result
  // is already valid across it.
  return result;
}

} // namespace tenzir::nova
