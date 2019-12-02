/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#include "vast/arrow_table_slice_builder.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"

#include <caf/make_copy_on_write.hpp>
#include <caf/make_counted.hpp>

#include <arrow/api.h>

#include <memory>

using namespace vast;

namespace vast {

// -- column builder (cb) implementations --------------------------------------

namespace {

template <class VastType, class ArrowType>
struct column_builder_trait_base : arrow::TypeTraits<ArrowType> {
  using data_type = typename type_traits<VastType>::data_type;
  using view_type = view<data_type>;
  using meta_type = VastType;
};

template <class VastType, class ArrowType>
struct primitive_column_builder_trait_base
  : column_builder_trait_base<VastType, ArrowType> {
  using super = column_builder_trait_base<VastType, ArrowType>;

  static auto make_arrow_type() {
    return super::type_singleton();
  }

  static bool
  append(typename super::BuilderType& builder, typename super::view_type x) {
    return builder.Append(x).ok();
  }
};

template <class T>
struct column_builder_trait;

#define PRIMITIVE_COLUMN_BUILDER_TRAIT(VastType, ArrowType)                    \
  template <>                                                                  \
  struct column_builder_trait<VastType>                                        \
    : primitive_column_builder_trait_base<VastType, ArrowType> {}

PRIMITIVE_COLUMN_BUILDER_TRAIT(bool_type, arrow::BooleanType);
PRIMITIVE_COLUMN_BUILDER_TRAIT(integer_type, arrow::Int64Type);
PRIMITIVE_COLUMN_BUILDER_TRAIT(count_type, arrow::UInt64Type);
PRIMITIVE_COLUMN_BUILDER_TRAIT(real_type, arrow::DoubleType);

#undef PRIMITIVE_COLUMN_BUILDER_TRAIT

template <>
struct column_builder_trait<time_type>
  : column_builder_trait_base<time_type, arrow::TimestampType> {
  using super = column_builder_trait_base<time_type, arrow::TimestampType>;

  static auto make_arrow_type() {
    return arrow::timestamp(arrow::TimeUnit::NANO);
  }

  static bool
  append(typename super::BuilderType& builder, typename super::view_type x) {
    return builder.Append(x.time_since_epoch().count()).ok();
  }
};

// Arrow does not have a duration type. There is TIME32/TIME64, but they
// represent the time of day, i.e., nano- or milliseconds since midnight.
// Hence, we fall back to storing the duration is 64-bit integer.
template <>
struct column_builder_trait<duration_type>
  : column_builder_trait_base<duration_type, arrow::Int64Type> {
  using super = column_builder_trait_base<duration_type, arrow::Int64Type>;

  static auto make_arrow_type() {
    return super::type_singleton();
  }

  static bool
  append(typename super::BuilderType& builder, typename super::view_type x) {
    return builder.Append(x.count()).ok();
  }
};

template <>
struct column_builder_trait<string_type>
  : column_builder_trait_base<string_type, arrow::StringType> {
  using super = column_builder_trait_base<string_type, arrow::StringType>;

  static auto make_arrow_type() {
    return super::type_singleton();
  }

  static bool
  append(typename super::BuilderType& builder, typename super::view_type x) {
    auto str = arrow::util::string_view(x.data(), x.size());
    return builder.Append(str).ok();
  }
};

template <>
struct column_builder_trait<pattern_type>
  : column_builder_trait_base<pattern_type, arrow::StringType> {
  using super = column_builder_trait_base<pattern_type, arrow::StringType>;

  static auto make_arrow_type() {
    return super::type_singleton();
  }

  static bool
  append(typename super::BuilderType& builder, typename super::view_type x) {
    auto str = arrow::util::string_view(x.string().data(), x.string().size());
    return builder.Append(str).ok();
  }
};

template <>
struct column_builder_trait<enumeration_type>
  : column_builder_trait_base<enumeration_type, arrow::UInt64Type> {
  using super = column_builder_trait_base<enumeration_type, arrow::UInt64Type>;

  static auto make_arrow_type() {
    return super::type_singleton();
  }

  static bool
  append(typename super::BuilderType& builder, typename super::view_type x) {
    return builder.Append(x).ok();
  }
};

template <>
struct column_builder_trait<none_type> : arrow::TypeTraits<arrow::NullType> {
  // -- member types -----------------------------------------------------------

  using super = arrow::TypeTraits<arrow::NullType>;

  using data_type = caf::none_t;

  using view_type = view<data_type>;

  using meta_type = none_type;

  // -- static member functions ------------------------------------------------

  static auto make_arrow_type() {
    return super::type_singleton();
  }

  static bool append(typename super::BuilderType& builder, view_type) {
    return builder.AppendNull().ok();
  }
};

template <>
struct column_builder_trait<address_type>
  : arrow::TypeTraits<arrow::FixedSizeBinaryType> {
  // -- member types -----------------------------------------------------------

  using super = arrow::TypeTraits<arrow::FixedSizeBinaryType>;

  using data_type = address;

  using view_type = view<data_type>;

  using meta_type = address_type;

  // -- static member functions ------------------------------------------------

  static auto make_arrow_type() {
    return std::make_shared<arrow::FixedSizeBinaryType>(16);
  }

  static bool append(typename super::BuilderType& builder, view_type x) {
    return builder.Append(x.data()).ok();
  }
};

template <>
struct column_builder_trait<subnet_type>
  : arrow::TypeTraits<arrow::FixedSizeBinaryType> {
  // -- member types -----------------------------------------------------------

  using super = arrow::TypeTraits<arrow::FixedSizeBinaryType>;

  using data_type = subnet;

  using view_type = view<data_type>;

  using meta_type = subnet_type;

  // -- static member functions ------------------------------------------------

  static auto make_arrow_type() {
    return std::make_shared<arrow::FixedSizeBinaryType>(17);
  }

  static bool append(typename super::BuilderType& builder, view_type x) {
    std::array<uint8_t, 17> data;
    auto& src = x.network().data();
    std::copy(src.begin(), src.end(), data.begin());
    data[16] = x.length();
    return builder.Append(data).ok();
  }
};

template <>
struct column_builder_trait<port_type>
  : arrow::TypeTraits<arrow::FixedSizeBinaryType> {
  // -- member types -----------------------------------------------------------

  using super = arrow::TypeTraits<arrow::FixedSizeBinaryType>;

  using data_type = port;

  using view_type = view<data_type>;

  using meta_type = address_type;

  // -- static member functions ------------------------------------------------

  static auto make_arrow_type() {
    return std::make_shared<arrow::FixedSizeBinaryType>(3);
  }

  static bool append(typename super::BuilderType& builder, view_type x) {
    // We store ports as uint16 (little endian) for the port itself plus an
    // uint8 for the type.
    uint16_t n = x.number();
    auto n_ptr = reinterpret_cast<uint8_t*>(&n);
    std::array<uint8_t, 3> data{n_ptr[0], n_ptr[1],
                                static_cast<uint8_t>(x.type())};
    return builder.Append(data).ok();
  }
};

template <class Trait>
class column_builder_impl final
  : public arrow_table_slice_builder::column_builder {
public:
  using arrow_builder_type = typename Trait::BuilderType;

  template <class T = Trait>
  column_builder_impl(
    std::enable_if_t<T::is_parameter_free, arrow::MemoryPool*> pool) {
    reset(pool);
  }

  template <class T = Trait>
  column_builder_impl(
    std::enable_if_t<!T::is_parameter_free, arrow::MemoryPool*> pool) {
    reset(T::make_arrow_type(), pool);
  }

  bool add(data_view x) override {
    if (auto xptr = caf::get_if<typename Trait::view_type>(&x))
      return Trait::append(*arrow_builder_, *xptr);
    else if (caf::holds_alternative<view<caf::none_t>>(x))
      return arrow_builder_->AppendNull().ok();
    else
      return false;
  }

  std::shared_ptr<arrow::Array> finish() override {
    std::shared_ptr<arrow::Array> result;
    if (!arrow_builder_->Finish(&result).ok())
      throw std::logic_error("builder.Finish failed");
    return result;
  }

  std::shared_ptr<arrow::ArrayBuilder> arrow_builder() const override {
    return arrow_builder_;
  }

private:
  template <class... Ts>
  void reset(Ts&&... xs) {
    arrow_builder_
      = std::make_shared<arrow_builder_type>(std::forward<Ts>(xs)...);
  }

  std::shared_ptr<arrow_builder_type> arrow_builder_;
};

template <class SequenceType>
class sequence_column_builder
  : public arrow_table_slice_builder::column_builder {
public:
  using arrow_builder_type = arrow::ListBuilder;

  using data_type = typename type_traits<SequenceType>::data_type;

  sequence_column_builder(arrow::MemoryPool* pool,
                          arrow_table_slice_builder::column_builder_ptr nested)
    : nested_(std::move(nested)) {
    reset(pool, nested_->arrow_builder());
  }

  bool add(data_view x) override {
    if (caf::holds_alternative<view<caf::none_t>>(x))
      return arrow_builder_->AppendNull().ok();
    if (!arrow_builder_->Append().ok())
      return false;
    if (auto xptr = caf::get_if<view<data_type>>(&x)) {
      auto n = (*xptr)->size();
      for (size_t i = 0; i < n; ++i)
        if (!nested_->add((*xptr)->at(i)))
          return false;
      return true;
    } else {
      return false;
    }
  }

  std::shared_ptr<arrow::Array> finish() override {
    std::shared_ptr<arrow::Array> result;
    if (!arrow_builder_->Finish(&result).ok())
      throw std::logic_error("builder.Finish failed");
    return result;
  }

  std::shared_ptr<arrow::ArrayBuilder> arrow_builder() const override {
    return arrow_builder_;
  }

private:
  template <class... Ts>
  void reset(Ts&&... xs) {
    arrow_builder_
      = std::make_shared<arrow_builder_type>(std::forward<Ts>(xs)...);
  }

  std::shared_ptr<arrow::ListBuilder> arrow_builder_;

  arrow_table_slice_builder::column_builder_ptr nested_;
};

template <class VastType>
using column_builder_impl_t
  = column_builder_impl<column_builder_trait<VastType>>;

class map_column_builder : public arrow_table_slice_builder::column_builder {
public:
  // There is no MapBuilder in Arrow. A map is simply a list of structs
  // (key-value pairs).
  using arrow_builder_type = arrow::ListBuilder;

  using data_type = view<map>;

  using column_builder_ptr = arrow_table_slice_builder::column_builder_ptr;

  map_column_builder(arrow::MemoryPool* pool,
                     std::shared_ptr<arrow::DataType> struct_type,
                     column_builder_ptr key_builder,
                     column_builder_ptr val_builder)
    : key_builder_(std::move(key_builder)),
      val_builder_(std::move(val_builder)) {
    std::vector fields{key_builder_->arrow_builder(),
                       val_builder_->arrow_builder()};
    kvp_builder_ = std::make_shared<arrow::StructBuilder>(struct_type, pool,
                                                          std::move(fields));
    list_builder_ = std::make_shared<arrow::ListBuilder>(pool, kvp_builder_);
  }

  bool add(data_view x) override {
    if (caf::holds_alternative<view<caf::none_t>>(x))
      return list_builder_->AppendNull().ok();
    if (!list_builder_->Append().ok())
      return false;
    if (auto xptr = caf::get_if<data_type>(&x)) {
      for (auto kvp : **xptr)
        if (!kvp_builder_->Append().ok() || !key_builder_->add(kvp.first)
            || !val_builder_->add(kvp.second))
          return false;
      return true;
    }
    return false;
  }

  std::shared_ptr<arrow::Array> finish() override {
    std::shared_ptr<arrow::Array> result;
    if (!list_builder_->Finish(&result).ok())
      throw std::logic_error("builder.Finish failed");
    return result;
  }

  std::shared_ptr<arrow::ArrayBuilder> arrow_builder() const override {
    return list_builder_;
  }

private:
  std::shared_ptr<arrow::StructBuilder> kvp_builder_;
  std::shared_ptr<arrow_builder_type> list_builder_;

  column_builder_ptr key_builder_;
  column_builder_ptr val_builder_;
};

} // namespace

// -- table slice builder implementation ---------------------------------------

arrow_table_slice_builder::column_builder::~column_builder() {
  // nop
}

caf::atom_value arrow_table_slice_builder::get_implementation_id() noexcept {
  return arrow_table_slice::class_id;
}

table_slice_builder_ptr arrow_table_slice_builder::make(record_type layout) {
  return caf::make_counted<arrow_table_slice_builder>(std::move(layout));
}
arrow_table_slice_builder::column_builder_ptr
arrow_table_slice_builder::make_column_builder(const type& t,
                                               arrow::MemoryPool* pool) {
  auto f = detail::overload(
    [=](const auto& x) -> column_builder_ptr {
      using type = std::decay_t<decltype(x)>;
      if constexpr (detail::is_any_v<type, vector_type, set_type>) {
        auto nested = make_column_builder(x.value_type, pool);
        auto ptr = new sequence_column_builder<type>(pool, std::move(nested));
        return column_builder_ptr{ptr};
      } else {
        auto ptr = new column_builder_impl_t<std::decay_t<decltype(x)>>(pool);
        return column_builder_ptr{ptr};
      }
    },
    [=](const map_type& x) -> column_builder_ptr {
      auto key_builder = make_column_builder(x.key_type, pool);
      auto value_builder = make_column_builder(x.value_type, pool);
      record_type fields{{"key", x.key_type}, {"value", x.value_type}};
      auto ptr = new map_column_builder(pool, make_arrow_type(fields),
                                        std::move(key_builder),
                                        std::move(value_builder));
      return column_builder_ptr{ptr};
    },
    [=](const record_type&) -> column_builder_ptr {
      throw std::logic_error("expected flat layout!");
    },
    [=](const alias_type& x) -> column_builder_ptr {
      return make_column_builder(x.value_type, pool);
    });
  return caf::visit(f, t);
}

std::shared_ptr<arrow::Schema>
arrow_table_slice_builder::make_arrow_schema(const record_type& t) {
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  arrow_fields.reserve(t.fields.size());
  for (auto& field : t.fields) {
    auto field_ptr = arrow::field(field.name, make_arrow_type(field.type));
    arrow_fields.emplace_back(std::move(field_ptr));
  }
  return std::make_shared<arrow::Schema>(arrow_fields);
}

std::shared_ptr<arrow::DataType>
arrow_table_slice_builder::make_arrow_type(const type& t) {
  using data_type_ptr = std::shared_ptr<arrow::DataType>;
  auto f = detail::overload(
    [=](const auto& x) -> data_type_ptr {
      using trait = column_builder_trait<std::decay_t<decltype(x)>>;
      return trait::make_arrow_type();
    },
    [=](const vector_type& x) -> data_type_ptr {
      return arrow::list(make_arrow_type(x.value_type));
    },
    [=](const set_type& x) -> data_type_ptr {
      return arrow::list(make_arrow_type(x.value_type));
    },
    [=](const map_type& x) -> data_type_ptr {
      // A map in arrow is a list of structs holding key/value pairs.
      std::vector fields{arrow::field("key", make_arrow_type(x.key_type)),
                         arrow::field("value", make_arrow_type(x.value_type))};
      return arrow::list(arrow::struct_(fields));
    },
    [=](const record_type& x) -> data_type_ptr {
      std::vector<std::shared_ptr<arrow::Field>> fields;
      for (auto& field : x.fields) {
        auto ptr = arrow::field(field.name, make_arrow_type(field.type));
        fields.emplace_back(std::move(ptr));
      }
      return arrow::struct_(fields);
    },
    [=](const alias_type& x) -> data_type_ptr {
      return make_arrow_type(x.value_type);
    });
  return caf::visit(f, t);
}

arrow_table_slice_builder::arrow_table_slice_builder(record_type layout)
  : super{std::move(layout)}, col_{0}, rows_{0} {
  VAST_ASSERT(this->layout().fields.size() > 0);
  builders_.reserve(this->layout().fields.size());
  auto pool = arrow::default_memory_pool();
  for (auto& field : this->layout().fields)
    builders_.emplace_back(make_column_builder(field.type, pool));
}

arrow_table_slice_builder::~arrow_table_slice_builder() {
  // nop
}

bool arrow_table_slice_builder::add_impl(data_view x) {
  if (!builders_[col_]->add(x))
    return false;
  if (++col_ == layout().fields.size()) {
    ++rows_;
    col_ = 0;
  }
  return true;
}

table_slice_ptr arrow_table_slice_builder::finish() {
  // Sanity check.
  if (col_ != 0)
    return nullptr;
  // Generate Arrow schema from layout.
  auto schema = make_arrow_schema(layout());
  // Collect Arrow arrays for the record batch.
  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(builders_.size());
  for (auto& builder : builders_)
    columns.emplace_back(builder->finish());
  // Done. Build record batch and table slice.
  auto batch = arrow::RecordBatch::Make(schema, rows_, columns);
  table_slice_header hdr{layout(), rows_, 0};
  rows_ = 0;
  return caf::make_copy_on_write<arrow_table_slice>(std::move(hdr),
                                                    std::move(batch));
}

size_t arrow_table_slice_builder::rows() const noexcept {
  return rows_;
}

caf::atom_value arrow_table_slice_builder::implementation_id() const noexcept {
  return get_implementation_id();
}

} // namespace vast
