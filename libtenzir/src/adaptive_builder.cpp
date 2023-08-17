//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/adaptive_builder.hpp"

#include "tenzir/detail/narrow.hpp"

namespace tenzir::experimental {

void field_ref::null() {
  die("TODO");
}

void field_ref::atom(int64_t value) {
  origin_->prepare<atom_builder<arrow::Int64Type>>(name_)->append(value);
}

auto field_ref::record() -> record_ref {
  return origin_->prepare<record_builder>(name_)->append();
}

auto field_ref::list() -> list_ref {
  return origin_->prepare<list_builder>(name_)->append();
}

void list_ref::null() {
  die("TODO");
}

void list_ref::atom(int64_t value) {
  origin_->prepare<atom_builder<arrow::Int64Type>>()->append(value);
}

auto list_ref::record() -> record_ref {
  return origin_->prepare<record_builder>()->append();
}

auto list_ref::list() -> list_ref {
  return origin_->prepare<list_builder>()->append();
}

template <class Builder>
auto list_builder::prepare() -> Builder* {
  (void)offsets_.Append(detail::narrow<int>(elements_->length()));
  if (auto cast = dynamic_cast<Builder*>(elements_.get())) {
    return cast;
  }
  if (auto* cast = dynamic_cast<union_builder*>(elements_.get())) {
    auto index = 0;
    for (auto& variant : cast->variants()) {
      if (auto* inner = dynamic_cast<Builder*>(variant.get())) {
        cast->begin_next(index);
        return inner;
      }
      index += 1;
    }
    index = cast->add_variant(std::make_unique<Builder>());
    cast->begin_next(index);
    return static_cast<Builder*>(cast->variants().back().get());
  }
  auto new_outer = std::make_unique<union_builder>(std::move(elements_));
  auto index = new_outer->add_variant(std::make_unique<Builder>());
  new_outer->begin_next(index);
  auto result = static_cast<Builder*>(new_outer->variants()[index].get());
  elements_ = std::move(new_outer);
  // TODO: Offsets remain valid, right?
  return result;
}

template <class Builder>
  requires(not std::same_as<Builder, null_builder>)
auto record_builder::prepare(std::string_view name) -> Builder* {
  // TODO: Overwriting / non existing?!
  auto it = fields_.find(name);
  if (it == fields_.end()) {
    // Field does not exist yet.
    it = fields_.try_emplace(std::string{name}, field_builders_.size()).first;
    field_builders_.push_back(std::make_unique<Builder>());
    auto result = static_cast<Builder*>(field_builders_.back().get());
    result->resize(length_ - 1);
    return result;
  }
  // The field exists. Try to find an existing, compatible builder.
  auto& builder = field_builders_[it->second];
  builder->resize(length_ - 1);
  if constexpr (std::same_as<Builder, null_builder>) {
    // TODO!!
    return builder.get();
  }
  if (auto* cast = dynamic_cast<Builder*>(builder.get())) {
    // The field itself is ...
    return cast;
  }
  if (auto* cast = dynamic_cast<union_builder*>(builder.get())) {
    // The field is a union, which potentially contains a list builder.
    auto index = 0;
    for (auto& variant : cast->variants()) {
      if (auto* inner = dynamic_cast<Builder*>(variant.get())) {
        // We found an existing list builder within this union.
        cast->begin_next(index);
        return inner;
      }
      index += 1;
    }
    // There is no list builder in this union yet.
    index = cast->add_variant(std::make_unique<Builder>());
    cast->begin_next(index);
    return static_cast<Builder*>(cast->variants().back().get());
  }
  // Otherwise, we have to upgrade this field into a union.
  auto new_outer = std::make_unique<union_builder>(std::move(builder));
  auto index = new_outer->add_variant(std::make_unique<Builder>());
  new_outer->begin_next(index);
  auto result = static_cast<Builder*>(new_outer->variants()[index].get());
  builder = std::move(new_outer);
  return result;
}

auto list_builder::record() -> record_builder* {
  add_offset(elements_->length());
  if (auto cast = dynamic_cast<record_builder*>(elements_.get())) {
    return cast;
  }
  if (auto cast = dynamic_cast<union_builder*>(elements_.get())) {
    auto index = 0;
    for (auto& variant : cast->variants()) {
      if (auto inner = dynamic_cast<record_builder*>(variant.get())) {
        cast->begin_next(index);
        return inner;
      }
      index += 1;
    }
  }
}

// TODO: This is the same as list_ref. Can perhaps unify?
class series_builder {
public:
  void null() {
    builder_->resize(builder_->length() + 1);
  }

  void atom(int64_t value) {
    prepare<atom_builder<arrow::Int64Type>>()->append(value);
  }

  auto record() -> record_ref {
    prepare<record_builder>()->append();
  }

  auto list() -> list_ref {
    prepare<list_builder>()->append();
  }

  auto finish() -> std::shared_ptr<arrow::Array> {
    return builder_->finish();
  }

private:
  template <class Builder>
  auto prepare() -> Builder* {
    if (auto cast = dynamic_cast<null_builder*>(builder_.get())) {
      auto length = builder_->length();
      builder_ = std::make_unique<Builder>();
      builder_->resize(length);
      return static_cast<Builder*>(builder_.get());
    }
    if (auto cast = dynamic_cast<Builder*>(builder_.get())) {
      return cast;
    }
    if (auto cast = dynamic_cast<union_builder*>(builder_.get())) {
      auto index = int8_t{0};
      for (auto& variant : cast->variants()) {
        if (auto inner = dynamic_cast<Builder*>(variant.get())) {
          cast->begin_next(index);
          return inner;
        }
        // TODO: Overflow?
        index += 1;
      }
      auto ptr = std::make_unique<Builder>();
      auto* inner = ptr.get();
      index = cast->add_variant(std::move(ptr));
      cast->begin_next(index);
      return inner;
    }
    auto builder = std::make_unique<union_builder>(std::move(builder_));
    auto variant = std::make_unique<Builder>();
    auto* ptr = variant.get();
    auto index = builder->add_variant(std::move(variant));
    builder->begin_next(index);
    return ptr;
  }

  std::unique_ptr<builder_base> builder_ = std::make_unique<null_builder>();
};

static void test() {
  {
    // <nothing>
    auto b = record_builder{};
  }
  {
    // {}
    auto b = record_builder{};
    b.append();
  }
  {
    // {a: null}
    auto b = record_builder{};
    auto r = b.append();
    r.null("a");
  }
  {
    // {}, {a: 42}, {}
    auto b = record_builder{};
    b.append();
    b.append().field("a").atom(42);
    b.append();
  }
  {
    // {a: 1, a: 2}, {a: 3}
    auto b = record_builder{};
    auto r = b.append();
    r.field("a").atom(1);
    r.field("a").atom(2);
    r = b.append();
    r.field("a").atom(3);
  }
  {
    // {a: [1, 2]}, {a: [], a: [{}]}
    auto b = record_builder{};
    auto l = b.append().list("a");
    l.atom(1);
    l.atom(2);
    auto r = b.append();
    r.list("a");
    r.list("a").record();
  }
  {
    // [1, 2], null, []
    auto b = list_builder{};
    auto l = b.append();
    l.atom(1);
    l.atom(2);
    b.append_null();
    b.append();
  }
  {
    // {a: 42}, [], null, 43
    auto b = series_builder{};
    b.record();
    b.record().field("a").atom(42);
    b.list();
    b.null();
    b.atom(43);
    (void)b.finish();
  }
}

} // namespace tenzir::experimental
