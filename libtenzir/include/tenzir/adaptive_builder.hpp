//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/detail/heterogeneous_string_hash.hpp"
#include "tenzir/legacy_type.hpp"
#include "tenzir/view.hpp"
#include "tsl/robin_map.h"

#include <arrow/api.h>

#include <memory>
#include <string_view>

namespace tenzir::experimental {

template <class T>
class atom_builder;
class builder_base;
class field_ref;
class list_builder;
class list_ref;
class record_builder;
class record_ref;
class union_builder;

/// Methods overwrite the field.
class field_ref {
public:
  field_ref(record_builder* origin, std::string_view name)
    : origin_{origin}, name_{name} {
  }

  void null();

  void atom(int64_t value);

  auto record() -> record_ref;

  auto list() -> list_ref;

private:
  record_builder* origin_;
  std::string_view name_;
};

/// Method has no immediate effect.
class record_ref {
public:
  explicit record_ref(record_builder* origin) : origin_{origin} {
  }

  auto field(std::string_view name) -> field_ref {
    return field_ref{origin_, name};
  }

private:
  record_builder* origin_;
};

/// Methods append to the list.
class list_ref {
public:
  explicit list_ref(list_builder* origin) : origin_{origin} {
  }

  void null();

  void atom(int64_t value);

  auto record() -> record_ref;

  auto list() -> list_ref;

private:
  list_builder* origin_;
};

class builder_base {
public:
  virtual ~builder_base() = default;

  virtual auto finish() -> std::shared_ptr<arrow::Array>;

  virtual auto type() -> std::shared_ptr<arrow::DataType>;

  virtual auto length() -> int64_t;

  /// @note If this removes elements, it can be very expensive.
  virtual void resize(int64_t length);
};

class null_builder final : public builder_base {
public:
private:
  arrow::NullBuilder builder_;
};

template <class T>
class atom_builder final : public builder_base {
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

  void append(arrow::TypeTraits<T>::CType value) {
    (void)inner_.Append(value);
  }

private:
  arrow::TypeTraits<T>::BuilderType inner_;
};

class list_builder final : public builder_base {
  friend class list_ref;

public:
  auto append() -> list_ref {
    (void)offsets_.Append(elements_->length());
    return list_ref{this};
  }

  void fill(int64_t length) override {
    TENZIR_ASSERT_CHEAP(offsets_.length() <= length);
    // TODO
    auto count = length - offsets_.length();
    auto offset = elements_->length();
    // TODO: Optimize.
    // TODO: Is this correct?
    while (count > 0) {
      // TODO: The builder does not seem to expose the right methods to do this.
      // FIXME: This may not be allowed?
      auto validity = false;
      (void)offsets_.AppendValues(&offset, &offset + 1, &validity);
      count -= 1;
    }
  }

  void add_offset(int32_t x) {
    (void)offsets_.Append(x);
  }

  auto values() -> std::unique_ptr<builder_base>& {
    return elements_;
  }

  auto finish() -> std::shared_ptr<arrow::Array> override {
    return arrow::ListArray::FromArrays(*offsets_.Finish().ValueOrDie(),
                                        *elements_->finish())
      .ValueOrDie();
  }

  auto type() -> std::shared_ptr<arrow::DataType> override {
    return std::make_shared<arrow::ListType>(elements_->type());
  }

  auto length() -> int override {
    return offsets_.length();
  }

  auto record() -> record_builder*;

private:
  template <class Builder>
  auto prepare() -> Builder*;

  arrow::Int32Builder offsets_;
  std::unique_ptr<builder_base> elements_;
};

class union_builder final : public builder_base {
public:
  union_builder() = default;

  explicit union_builder(std::unique_ptr<builder_base> x) {
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

  auto add_variant(std::unique_ptr<builder_base> child) -> int8_t {
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

  auto variants() -> std::span<std::unique_ptr<builder_base>> {
    return variants_;
  }

private:
  arrow::Int8Builder discriminants_;
  arrow::Int32Builder offsets_;
  std::vector<std::unique_ptr<builder_base>> variants_;
};

class record_builder final : public builder_base {
  friend class field_ref;

public:
  auto append() -> record_ref {
    length_ += 1;
    return record_ref{};
  }

  auto finish() -> std::shared_ptr<arrow::Array> override {
    auto children = std::vector<std::shared_ptr<arrow::Array>>{};
    children.reserve(field_builders_.size());
    for (auto& field_builder : field_builders_) {
      children.push_back(field_builder->finish());
    }
    return arrow::StructArray::Make(children, make_fields()).ValueOrDie();
  }

  auto type() -> std::shared_ptr<arrow::DataType> override {
    return std::make_shared<arrow::StructType>(make_fields());
  }

  auto length() -> int64_t override {
    return length_;
  }

private:
  auto make_fields() -> std::vector<std::shared_ptr<arrow::Field>> {
    auto fields = std::vector<std::shared_ptr<arrow::Field>>{};
    fields.resize(field_builders_.size());
    for (auto& [name, index] : fields_) {
      fields[index] = arrow::field(name, field_builders_[index]->type());
    }
    return fields;
  }

  /// Prepares for overwriting.
  template <class Builder>
    requires(not std::same_as<Builder, null_builder>)
  auto prepare(std::string_view name) -> Builder*;

  tsl::robin_map<std::string, size_t, detail::heterogeneous_string_hash,
                 detail::heterogeneous_string_equal>
    fields_;

  std::vector<std::unique_ptr<builder_base>> field_builders_;

  int64_t length_ = 0;
};

class adaptive_row {
public:
  auto list_field(std::string_view name) -> list_builder* {
    auto sb = struct_builder{};
  }

  void primitive_field(std::string_view name, int64_t data) {
    auto sb = struct_builder{};
    auto field_index = 0;
    for (auto& field : b->type()->fields()) {
      if (field->name() == name) {
        // ---- Field already exists!
        if (field->type()->Equals(arrow::Int64Type{})) {
          // ----- type is equal -----
          // TODO: Test if it is a union type!
          // We can reuse the builder.
          auto builder = static_cast<arrow::Int64Builder*>(b->child(0));
          auto not_appended_yet = true;
          if (not_appended_yet) {
            (void)builder->Append(data);
          } else {
            auto array = std::shared_ptr<arrow::Int64Array>{};
            (void)builder->Finish(&array);
            (void)builder->AppendArraySlice(*array->data(), 0,
                                            array->length() - 1);
            (void)builder->Append(data);
          }
        } else if (auto ptr = std::dynamic_pointer_cast<arrow::DenseUnionType>(
                     field->type())) {
          // ----- type is contained in union -----
          die("TODO");
        } else {
          // ----- type is not equal and not contained in union -----
          // => Make a union out of the field!
          // TODO: Try some conversions first (e.g. signedness)?
          // TODO: Do not use uint64.
          auto union_builder
            = std::make_shared<union_builder>(b->child_builder(field_index));
          auto new_builder = std::make_shared<arrow::Int64Builder>();
          auto new_slot = union_builder->add_variant(new_builder);
          union_builder->add_next(new_slot);
          (void)new_builder->Append(data);
          // TODO: Actually replace field!
          auto new_struct_type = std::shared_ptr<arrow::DataType>{};
          auto field_builders
            = std::vector<std::shared_ptr<arrow::ArrayBuilder>>{};
          field_builders.push_back(std::move(union_builder));
          arrow::StructBuilder{new_struct_type, arrow::default_memory_pool(),
                               std::move(field_builders)};
          return;
        }
      }
      field_index += 1;
    }
    // ---- Field does not exist yet => create it
    auto field_builder = std::make_unique<arrow_builder<arrow::Int64Builder>>();
    (void)field_builder->inner.Append(data);
    sb.field(std::string{name}) = std::move(field_builder);
  }
};

// class adaptive_builder {
// public:
//   adaptive_builder()
//     : builder_{std::make_shared<arrow::StructType>(
//                  std::vector<std::shared_ptr<arrow::Field>>{}),
//                arrow::default_memory_pool(),
//                {}} {
//   }

//   auto push_row() -> adaptive_row {
//     builder_.Append();
//   }

//   auto finish() -> table_slice {
//   }

// private:
//   arrow::StructBuilder builder_;
// };

} // namespace tenzir::experimental
