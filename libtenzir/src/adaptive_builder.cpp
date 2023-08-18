//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/adaptive_builder.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/detail/narrow.hpp"

#include <tenzir/logger.hpp>

#include <arrow/builder.h>
#include <arrow/type.h>
#include <tsl/robin_map.h>

#include <span>

namespace tenzir::experimental {

namespace {

template <class Builder>
auto resize_arrow_builder(Builder& builder, int64_t length) {
  auto current = builder.length();
  if (current < length) {
    (void)builder.AppendNulls(length - current);
  } else if (current > length) {
    auto result = std::shared_ptr<typename Builder::ArrayType>{};
    (void)builder.Finish(&result);
    (void)builder.AppendArraySlice(*result->data(), 0, length);
  }
}

} // namespace

namespace detail {

// TODO: Remove.
using tenzir::detail::narrow;

class typed_builder {
public:
  virtual ~typed_builder() = default;

  virtual auto finish() -> std::shared_ptr<arrow::Array> = 0;

  virtual auto type() -> std::shared_ptr<arrow::DataType> = 0;

  virtual auto length() -> int64_t = 0;

  /// @note If this removes elements, it can be very expensive.
  virtual void resize(int64_t length) = 0;
};

class list_builder final : public typed_builder {
  friend class tenzir::experimental::list_ref;

public:
  auto append() -> list_ref;

  void resize(int64_t length) override;

  // void add_offset(int32_t x) {
  //   (void)offsets_.Append(x);
  // }

  auto finish() -> std::shared_ptr<arrow::Array> override {
    (void)offsets_.Append(detail::narrow<int32_t>(elements_.length()));
    return arrow::ListArray::FromArrays(*offsets_.Finish().ValueOrDie(),
                                        *elements_.finish())
      .ValueOrDie();
  }

  auto type() -> std::shared_ptr<arrow::DataType> override {
    return std::make_shared<arrow::ListType>(elements_.type());
  }

  auto length() -> int64_t override {
    return offsets_.length();
  }

  // auto record() -> record_builder*;

private:
  /// This only stores *beginning* indices.
  arrow::Int32Builder offsets_;
  series_builder elements_;
};

class record_builder final : public typed_builder {
  friend class tenzir::experimental::field_ref;

public:
  auto append() -> record_ref {
    length_ += 1;
    return record_ref{this};
  }

  auto finish() -> std::shared_ptr<arrow::Array> override {
    auto children = std::vector<std::shared_ptr<arrow::Array>>{};
    children.reserve(builders_.size());
    for (auto& builder : builders_) {
      TENZIR_WARN("comparing {} and {}", builder.length(), length_);
      TENZIR_ASSERT_CHEAP(builder.length() <= length_);
      builder.resize(length_);
      children.push_back(builder.finish());
    }
    auto null_bitmap = std::shared_ptr<arrow::Buffer>{};
    if (nulls_.length() > 0) {
      TENZIR_WARN("adding nulls to struct");
      auto nulls = std::shared_ptr<arrow::BooleanArray>{};
      auto count = length_ - nulls_.length();
      TENZIR_ASSERT_CHEAP(count >= 0);
      TENZIR_ASSERT_CHEAP(nulls_.Reserve(count).ok());
      while (count > 0) {
        TENZIR_ASSERT_CHEAP(nulls_.Append(true).ok());
        count -= 1;
      }
      TENZIR_ASSERT_CHEAP(nulls_.Finish(&nulls).ok());
      TENZIR_ASSERT_CHEAP(nulls->data()->buffers.size() == 2);
      null_bitmap = nulls->data()->buffers[1]; // TODO: ?
      TENZIR_WARN("about to print hex of {} out of {}",
                  (void*)null_bitmap.get(), nulls->data()->buffers.size());
      TENZIR_WARN((*null_bitmap).ToHexString());
    }
    return std::make_shared<arrow::StructArray>(type(), length(), children,
                                                std::move(null_bitmap));
  }

  auto type() -> std::shared_ptr<arrow::DataType> override {
    return std::make_shared<arrow::StructType>(make_fields());
  }

  auto length() -> int64_t override {
    return length_;
  }

  void resize(int64_t length) override {
    TENZIR_WARN("resize: {} -> {}", length_, length);
    if (length < length_) {
      if (length < nulls_.length()) {
        auto nulls = std::shared_ptr<arrow::BooleanArray>{};
        (void)nulls_.Finish(&nulls);
        (void)nulls_.AppendArraySlice(*nulls->data(), 0, length);
      }
      for (auto& builder : builders_) {
        builder.resize(length);
      }
    } else if (length > length_) {
      // TODO
      auto true_count = length_ - nulls_.length();
      auto false_count = length - length_;
      (void)nulls_.Reserve(true_count + false_count);
      while (true_count > 0) {
        TENZIR_WARN("added a true");
        (void)nulls_.Append(true);
        true_count -= 1;
      }
      while (false_count > 0) {
        TENZIR_WARN("added a false");
        (void)nulls_.Append(false);
        false_count -= 1;
      }
    }
    length_ = length;
  }

private:
  auto make_fields() -> std::vector<std::shared_ptr<arrow::Field>> {
    auto fields = std::vector<std::shared_ptr<arrow::Field>>{};
    fields.resize(builders_.size());
    for (auto& [name, index] : fields_) {
      fields[index] = arrow::field(name, builders_[index].type());
    }
    return fields;
  }

  /// Prepares for overwriting.
  template <class Builder>
  auto prepare(std::string_view name) -> Builder*;

  tsl::robin_map<std::string, size_t, tenzir::detail::heterogeneous_string_hash,
                 tenzir::detail::heterogeneous_string_equal>
    fields_;

  /// Lazy field builders. If not long enough, field is absent.
  std::vector<series_builder> builders_;

  /// Lazy validity bitmap. If not long enough, value is present.
  arrow::BooleanBuilder nulls_;

  /// ...
  int64_t length_ = 0;
};

namespace {

class null_builder final : public detail::typed_builder {
public:
  auto finish() -> std::shared_ptr<arrow::Array> {
    auto builder = arrow::NullBuilder{};
    (void)builder.AppendNulls(length_);
    return builder.Finish().ValueOrDie();
  }

  auto type() -> std::shared_ptr<arrow::DataType> {
    return std::make_shared<arrow::NullType>();
  }

  auto length() -> int64_t {
    return length_;
  }

  void resize(int64_t length) {
    length_ = length;
  }

private:
  int64_t length_ = 0;
};

template <class T>
class atom_builder final : public detail::typed_builder {
public:
  auto finish() -> std::shared_ptr<arrow::Array> override {
    return inner_.Finish().ValueOrDie();
  }

  auto type() -> std::shared_ptr<arrow::DataType> override {
    return std::make_shared<T>();
  }

  auto length() -> int64_t override {
    return inner_.length();
  }

  void resize(int64_t length) override {
    resize_arrow_builder(inner_, length);
  }

  void append(arrow::TypeTraits<T>::CType value) {
    (void)inner_.Append(value);
  }

private:
  arrow::TypeTraits<T>::BuilderType inner_;
};

class union_builder final : public typed_builder {
public:
  union_builder() = default;

  explicit union_builder(std::unique_ptr<typed_builder> x) {
    // TODO: Does this actually append zeroes?
    (void)discriminants_.AppendEmptyValues(x->length());
    (void)offsets_.Reserve(x->length());
    for (auto i = 0; i < x->length(); ++i) {
      offsets_.UnsafeAppend(i);
    }
    variants_.push_back(std::move(x));
  }

  void begin_next(int8_t idx) {
    TENZIR_ASSERT_CHEAP(idx < variants_.size());
    (void)discriminants_.Append(idx);
    (void)offsets_.Append(detail::narrow<int32_t>(variants_[idx]->length()));
  }

  auto add_variant(std::unique_ptr<typed_builder> child) -> int8_t {
    TENZIR_ASSERT_CHEAP(child->length() == 0);
    variants_.push_back(std::move(child));
    return variants_.size() - 1;
  }

  auto finish() -> std::shared_ptr<arrow::Array> override {
    auto variants = std::vector<std::shared_ptr<arrow::Array>>{};
    variants.reserve(variants_.size());
    for (auto& variant : variants_) {
      // fields.push_back(arrow::field("", variant->type()));
      // types.push_back(detail::narrow<int8_t>(&variant - variants_.data()));
      variants.push_back(variant->finish());
    }

    // TODO: dereference -> copy?
    return std::static_pointer_cast<arrow::DenseUnionArray>(
      arrow::DenseUnionArray::Make(*discriminants_.Finish().ValueOrDie(),
                                   *offsets_.Finish().ValueOrDie(),
                                   std::move(variants))
        .ValueOrDie());
  }

  auto type() -> std::shared_ptr<arrow::DataType> override {
    auto fields = std::vector<std::shared_ptr<arrow::Field>>{};
    fields.reserve(variants_.size());
    auto types = std::vector<int8_t>{};
    types.reserve(variants_.size());
    for (auto& variant : variants_) {
      fields.push_back(arrow::field("", variant->type()));
      types.push_back(detail::narrow<int8_t>(&variant - variants_.data()));
    }
    return arrow::DenseUnionType::Make(std::move(fields), std::move(types))
      .ValueOrDie();
  }

  auto length() -> int64_t override {
    return discriminants_.length();
  }

  void resize(int64_t length) override {
    if (length != discriminants_.length()) {
      TENZIR_UNREACHABLE(); // TODO
    }
    // TODO: We can not append nulls here.
    // resize_numeric_builder(discriminants_, length);
    // resize_numeric_builder(offsets_, length);
    // for (auto& variant : variants_) {
    //   // TODO: Do we want to resize variants here?
    //   (void)variant;
    // }
  }

  auto variants() -> std::span<std::unique_ptr<typed_builder>> {
    return variants_;
  }

private:
  arrow::Int8Builder discriminants_;
  arrow::Int32Builder offsets_;
  std::vector<std::unique_ptr<typed_builder>> variants_;
};

} // namespace

} // namespace detail

void field_ref::null() {
  // We already incremented the length of the origin.
  origin_->resize(origin_->length());
}

void field_ref::atom(int64_t value) {
  origin_->prepare<detail::atom_builder<arrow::Int64Type>>(name_)->append(
    value);
}

auto field_ref::record() -> record_ref {
  return origin_->prepare<detail::record_builder>(name_)->append();
}

auto field_ref::list() -> list_ref {
  return origin_->prepare<detail::list_builder>(name_)->append();
}

void list_ref::null() {
  origin_->elements_.null();
}

void list_ref::atom(int64_t value) {
  origin_->elements_.atom(value);
}

auto list_ref::record() -> record_ref {
  return origin_->elements_.record();
}

auto list_ref::list() -> list_ref {
  return origin_->elements_.list();
}

series_builder::series_builder()
  : builder_{std::make_unique<detail::null_builder>()} {
}

void series_builder::null() {
  resize(length() + 1);
}

void series_builder::resize(int64_t length) {
  builder_->resize(length);
}

void series_builder::atom(int64_t value) {
  prepare<detail::atom_builder<arrow::Int64Type>>()->append(value);
}

auto series_builder::record() -> record_ref {
  return prepare<detail::record_builder>()->append();
}

auto series_builder::list() -> list_ref {
  return prepare<detail::list_builder>()->append();
}

auto series_builder::length() -> int64_t {
  return builder_->length();
}

auto series_builder::finish() -> std::shared_ptr<arrow::Array> {
  return builder_->finish();
}

template <class Builder>
auto series_builder::prepare() -> Builder* {
  if (auto cast = dynamic_cast<detail::null_builder*>(builder_.get())) {
    auto length = builder_->length();
    builder_ = std::make_unique<Builder>();
    builder_->resize(length);
    return static_cast<Builder*>(builder_.get());
  }
  if (auto cast = dynamic_cast<Builder*>(builder_.get())) {
    return cast;
  }
  if (auto cast = dynamic_cast<detail::union_builder*>(builder_.get())) {
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
  auto builder = std::make_unique<detail::union_builder>(std::move(builder_));
  auto variant = std::make_unique<Builder>();
  auto* ptr = variant.get();
  auto index = builder->add_variant(std::move(variant));
  builder->begin_next(index);
  builder_ = std::move(builder);
  return ptr;
}

template <class Builder>
auto detail::record_builder::prepare(std::string_view name) -> Builder* {
#if 1
  auto it = fields_.find(name);
  if (it == fields_.end()) {
    // Field does not exist yet.
    it = fields_.try_emplace(std::string{name}, builders_.size()).first;
    auto new_builder = std::make_unique<Builder>();
    auto pointer = new_builder.get();
    builders_.emplace_back(std::move(new_builder));
    pointer->resize(length_ - 1);
    return pointer;
  }
  auto& builder = builders_[it->second];
  builder.resize(length_ - 1);
  return builder.prepare<Builder>();
#else
  // TODO: Overwriting / non existing?!
  auto it = fields_.find(name);
  if (it == fields_.end()) {
    // Field does not exist yet.
    it = fields_.try_emplace(std::string{name}, builders_.size()).first;
    builders_.push_back(std::make_unique<Builder>());
    auto result = static_cast<Builder*>(builders_.back().get());
    result->resize(length_ - 1);
    return result;
  }
  // The field exists. Try to find an existing, compatible builder.
  auto& builder = builders_[it->second];
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
#endif
}

// auto list_builder::record() -> record_builder* {
//   add_offset(elements_->length());
//   if (auto cast = dynamic_cast<record_builder*>(elements_.get())) {
//     return cast;
//   }
//   if (auto cast = dynamic_cast<union_builder*>(elements_.get())) {
//     auto index = 0;
//     for (auto& variant : cast->variants()) {
//       if (auto inner = dynamic_cast<record_builder*>(variant.get())) {
//         cast->begin_next(index);
//         return inner;
//       }
//       index += 1;
//     }
//   }
// }

auto series_builder::type() -> std::shared_ptr<arrow::DataType> {
  return builder_->type();
}

static void test() {
  {
    // <nothing>
    auto b = series_builder{};
  }
  {
    // {}
    auto b = series_builder{};
    b.record();
  }
  {
    // {a: null}
    auto b = series_builder{};
    b.record().field("a").null();
  }
  {
    // {}, {a: 42}, {}
    auto b = series_builder{};
    b.record();
    b.record().field("a").atom(42);
    b.record();
  }
  {
    // {a: 1, a: 2}, {a: 3}
    auto b = series_builder{};
    auto r = b.record();
    r.field("a").atom(1);
    r.field("a").atom(2);
    r = b.record();
    r.field("a").atom(3);
  }
  {
    // {a: [1, 2]}, {a: [], a: [{}]}
    auto b = series_builder{};
    auto l = b.record().field("a");
    l.atom(1);
    l.atom(2);
    auto r = b.record();
    r.field("a").list();
    r.field("a").list().record();
  }
  {
    // [1, 2], null, []
    auto b = series_builder{};
    auto l = b.list();
    l.atom(1);
    l.atom(2);
    b.null();
    b.list();
  }
  {
    // {a: 42}, [], null, 43
    auto b = series_builder{};
    b.record().field("a").atom(42);
    b.list();
    b.null();
    b.atom(43);
  }
}

auto detail::list_builder::append() -> list_ref {
  (void)offsets_.Append(detail::narrow<int32_t>(elements_.length()));
  return list_ref{this};
}

void detail::list_builder::resize(int64_t length) {
  auto current = this->length();
  if (length < current) {
    // TODO: Write this differently?
    auto result = std::shared_ptr<arrow::Int32Array>{};
    (void)offsets_.Finish(&result);
    (void)offsets_.AppendArraySlice(*result->data(), 0, length);
    // Get ending offset of last element.
    elements_.resize(result->Value(length));
  } else if (length > current) {
    // TODO: Optimize.
    // TODO: Is this correct?
    auto count = length - offsets_.length();
    auto offset = elements_.length();
    while (count > 0) {
      // TODO: The builder does not seem to expose the right methods to do this.
      // FIXME: This may not be allowed?
      auto validity = false;
      (void)offsets_.AppendValues(&offset, &offset + 1, &validity);
      count -= 1;
    }
  }
}

series_builder::series_builder(std::unique_ptr<detail::typed_builder> builder)
  : builder_{std::move(builder)} {
}

series_builder::~series_builder() = default;

} // namespace tenzir::experimental
