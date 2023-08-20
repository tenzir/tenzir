//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/series_builder.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/detail/narrow.hpp"

#include <tenzir/logger.hpp>

#include <arrow/builder.h>
#include <arrow/type.h>
#include <sys/_types/_int32_t.h>
#include <tsl/robin_map.h>

#include <span>

namespace tenzir::experimental {

namespace {

template <class Builder>
void resize_arrow_builder(Builder& builder, int64_t length) {
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
using tenzir::detail::heterogeneous_string_hashmap;
using tenzir::detail::narrow;

class typed_builder {
public:
  virtual ~typed_builder() = default;

  virtual auto finish() -> std::shared_ptr<arrow::Array> = 0;

  virtual auto type() -> std::shared_ptr<arrow::DataType> = 0;

  virtual auto length() -> int64_t = 0;

  /// @note If this removes elements, it can be very expensive.
  virtual void resize(int64_t new_length) = 0;
};

namespace {

class null_builder final : public detail::typed_builder {
public:
  auto finish() -> std::shared_ptr<arrow::Array> override {
    auto builder = arrow::NullBuilder{};
    (void)builder.AppendNulls(length_);
    return builder.Finish().ValueOrDie();
  }

  auto type() -> std::shared_ptr<arrow::DataType> override {
    return std::make_shared<arrow::NullType>();
  }

  auto length() -> int64_t override {
    return length_;
  }

  void resize(int64_t new_length) override {
    length_ = new_length;
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

  void resize(int64_t new_length) override {
    resize_arrow_builder(inner_, new_length);
  }

  void append(arrow::TypeTraits<T>::CType value) {
    (void)inner_.Append(value);
  }

private:
  arrow::TypeTraits<T>::BuilderType inner_;
};

class union_builder final : public typed_builder {
public:
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
    TENZIR_ASSERT_CHEAP(idx >= 0);
    TENZIR_ASSERT_CHEAP(static_cast<size_t>(idx) < variants_.size());
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
      variants.push_back(variant->finish());
    }
    // TODO: Check dereference.
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
    TENZIR_ASSERT_CHEAP(discriminants_.length() == offsets_.length());
    if (length < discriminants_.length()) {
      // TODO: Do we leave the old data in the variants?
      resize_arrow_builder(discriminants_, length);
      resize_arrow_builder(offsets_, length);
    } else {
      // A union itself does not have a validity map. But we know that there
      // exists at least one variant, which we can append nulls for.
      TENZIR_ASSERT_CHEAP(not variants_.empty());
      auto count = length - discriminants_.length();
      (void)discriminants_.Reserve(count);
      (void)offsets_.Reserve(count);
      auto variant_start = variants_[0]->length();
      variants_[0]->resize(variant_start + count);
      for (auto i = 0; i < count; ++i) {
        (void)discriminants_.UnsafeAppend(0);
        (void)offsets_.UnsafeAppend(detail::narrow<int32_t>(variant_start + i));
      }
    }
  }

  auto variants() -> std::span<std::unique_ptr<typed_builder>> {
    return variants_;
  }

private:
  // TODO: Use `TypedBufferBuilder` instead?
  arrow::Int8Builder discriminants_;
  arrow::Int32Builder offsets_;
  std::vector<std::unique_ptr<typed_builder>> variants_;
};

} // namespace

class list_builder final : public typed_builder {
  friend class tenzir::experimental::list_ref;

public:
  auto append() -> list_ref;

  void resize(int64_t new_length) override;

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
      TENZIR_ASSERT_CHEAP(builder.length() <= length_);
      builder.resize(length_);
      children.push_back(builder.finish());
    }
    auto null_bitmap = std::shared_ptr<arrow::Buffer>{};
    if (valid_.length() > 0) {
      auto nulls = std::shared_ptr<arrow::BooleanArray>{};
      auto count = length_ - valid_.length();
      TENZIR_ASSERT_CHEAP(count >= 0);
      TENZIR_ASSERT_CHEAP(valid_.Reserve(count).ok());
      while (count > 0) {
        TENZIR_ASSERT_CHEAP(valid_.Append(true).ok());
        count -= 1;
      }
      TENZIR_ASSERT_CHEAP(valid_.Finish(&nulls).ok());
      TENZIR_ASSERT_CHEAP(nulls->data()->buffers.size() == 2);
      null_bitmap = nulls->data()->buffers[1]; // TODO: Check this.
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

  void resize(int64_t new_length) override {
    if (new_length < length_) {
      if (new_length < valid_.length()) {
        auto nulls = std::shared_ptr<arrow::BooleanArray>{};
        (void)valid_.Finish(&nulls);
        (void)valid_.AppendArraySlice(*nulls->data(), 0, new_length);
      }
      for (auto& builder : builders_) {
        builder.resize(new_length);
      }
    } else if (new_length > length_) {
      // TODO: Check and optimize.
      auto true_count = length_ - valid_.length();
      auto false_count = new_length - length_;
      (void)valid_.Reserve(true_count + false_count);
      while (true_count > 0) {
        valid_.UnsafeAppend(true);
        true_count -= 1;
      }
      while (false_count > 0) {
        valid_.UnsafeAppend(false);
        false_count -= 1;
      }
    }
    length_ = new_length;
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

  // TODO: Do we want to make `null_builder -> typed_builder*`?
  /// Prepares field for overwriting (i.e., erases value if already set).
  template <class Builder>
  auto prepare(std::string_view name) -> Builder*;

  auto builder(std::string_view name) -> series_builder* {
    auto it = fields_.find(name);
    if (it == fields_.end()) {
      return nullptr;
    }
    return &builders_[it->second];
  }

  /// @pre Field does not exist yet.
  template <class Builder>
  auto insert_new_field(std::string name) -> Builder*;

  /// ...
  detail::heterogeneous_string_hashmap<size_t> fields_;

  /// Missing values shall be considered null.
  std::vector<series_builder> builders_;

  /// Missing values shall be considered true.
  arrow::BooleanBuilder valid_;

  /// ...
  int64_t length_ = 0;
};

} // namespace detail

void field_ref::null() {
  // Note: We already incremented the length of the origin.
  if (auto field = origin_->builder(name_)) {
    // TODO: Can this be inefficient?
    field->resize(origin_->length() - 1);
    field->resize(origin_->length());
  } else {
    // TODO: Resize? Probably not.
    origin_->insert_new_field<detail::null_builder>(std::string{name_});
  }
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

auto field_ref::builder() -> series_builder* {
  return origin_->builder(name_);
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

void series_builder::reset() {
  builder_ = std::make_unique<detail::null_builder>();
}

template <class Builder>
auto series_builder::prepare() -> Builder* {
  static_assert(not std::same_as<Builder, detail::null_builder>);
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
  if (auto cast = dynamic_cast<detail::null_builder*>(builder_.get())) {
    auto length = builder_->length();
    builder_ = std::make_unique<Builder>();
    builder_->resize(length);
    return static_cast<Builder*>(builder_.get());
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
  static_assert(not std::same_as<Builder, detail::null_builder>);
  auto it = fields_.find(name);
  if (it == fields_.end()) {
    return insert_new_field<Builder>(std::string{name});
  }
  auto& builder = builders_[it->second];
  builder.resize(length_ - 1);
  return builder.prepare<Builder>();
}

template <class Builder>
auto detail::record_builder::insert_new_field(std::string name) -> Builder* {
  auto [it, inserted] = fields_.try_emplace(std::move(name), builders_.size());
  TENZIR_ASSERT_CHEAP(inserted);
  auto builder = std::make_unique<Builder>();
  auto pointer = builder.get();
  builders_.emplace_back(std::move(builder));
  pointer->resize(length_ - 1);
  return pointer;
}

auto series_builder::type() -> std::shared_ptr<arrow::DataType> {
  return builder_->type();
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
    // TODO: Optimize and check that this is correct.
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
