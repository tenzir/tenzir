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
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/die.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/util/config.h>

namespace vast {

// -- column builder implementations ------------------------------------------

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

  using meta_type = port_type;

  // -- static member functions ------------------------------------------------

  static auto make_arrow_type() {
    return std::make_shared<arrow::FixedSizeBinaryType>(3);
  }

  static bool append(typename super::BuilderType& builder, view_type x) {
    // We store ports as uint16 (little endian) for the port itself plus an
    // uint8 for the type.
    uint16_t n = detail::to_network_order(x.number());
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
      die("failed to finish Arrow column builder");
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
                          std::unique_ptr<column_builder> nested)
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
      die("failed to finish Arrow column builder");
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

  std::unique_ptr<column_builder> nested_;
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

  map_column_builder(arrow::MemoryPool* pool,
                     std::shared_ptr<arrow::DataType> struct_type,
                     std::unique_ptr<column_builder> key_builder,
                     std::unique_ptr<column_builder> val_builder)
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
      die("failed to finish Arrow column builder");
    return result;
  }

  std::shared_ptr<arrow::ArrayBuilder> arrow_builder() const override {
    return list_builder_;
  }

private:
  std::shared_ptr<arrow::StructBuilder> kvp_builder_;
  std::shared_ptr<arrow_builder_type> list_builder_;

  std::unique_ptr<column_builder> key_builder_;
  std::unique_ptr<column_builder> val_builder_;
};

} // namespace

// -- member types -------------------------------------------------------------

arrow_table_slice_builder::column_builder::~column_builder() noexcept {
  // nop
}

std::unique_ptr<arrow_table_slice_builder::column_builder>
arrow_table_slice_builder::column_builder::make(const type& t,
                                                arrow::MemoryPool* pool) {
  auto f = detail::overload{
    [=](const auto& x) -> std::unique_ptr<column_builder> {
      using type = std::decay_t<decltype(x)>;
      if constexpr (std::is_same_v<type, list_type>) {
        auto nested = column_builder::make(x.value_type, pool);
        return std::make_unique<sequence_column_builder<type>>(
          pool, std::move(nested));
      } else {
        return std::make_unique<column_builder_impl_t<std::decay_t<decltype(x)>>>(
          pool);
      }
    },
    [=](const map_type& x) -> std::unique_ptr<column_builder> {
      auto key_builder = column_builder::make(x.key_type, pool);
      auto value_builder = column_builder::make(x.value_type, pool);
      record_type fields{{"key", x.key_type}, {"value", x.value_type}};
      return std::make_unique<map_column_builder>(pool, make_arrow_type(fields),
                                                  std::move(key_builder),
                                                  std::move(value_builder));
    },
    [=](const record_type&) -> std::unique_ptr<column_builder> {
      die("expected flat layout!");
    },
    [=](const alias_type& x) -> std::unique_ptr<column_builder> {
      return column_builder::make(x.value_type, pool);
    },
  };
  return caf::visit(f, t);
}

// -- constructors, destructors, and assignment operators ----------------------

table_slice_builder_ptr
arrow_table_slice_builder::make(record_type layout,
                                size_t initial_buffer_size) {
  return table_slice_builder_ptr{
    new arrow_table_slice_builder{std::move(layout), initial_buffer_size},
    false};
}

arrow_table_slice_builder::~arrow_table_slice_builder() noexcept {
  // nop
}

// -- properties ---------------------------------------------------------------

table_slice arrow_table_slice_builder::finish(
  [[maybe_unused]] span<const byte> serialized_layout) {
  // Sanity check.
  if (column_ != 0)
    return {};
  // Pack layout.
  auto layout_buffer
    = serialized_layout.empty()
        ? *fbs::serialize_bytes(builder_, layout())
        : builder_.CreateVector(
          reinterpret_cast<const unsigned char*>(serialized_layout.data()),
          serialized_layout.size());
  // Pack schema.
#if ARROW_VERSION_MAJOR >= 2
  auto flat_schema = arrow::ipc::SerializeSchema(*schema_).ValueOrDie();
#else
  auto flat_schema
    = arrow::ipc::SerializeSchema(*schema_, nullptr).ValueOrDie();
#endif
  auto schema_buffer
    = builder_.CreateVector(flat_schema->data(), flat_schema->size());
  // Pack record batch.
  auto columns = std::vector<std::shared_ptr<arrow::Array>>{};
  columns.reserve(column_builders_.size());
  for (auto&& builder : column_builders_)
    columns.emplace_back(builder->finish());
  auto record_batch
    = arrow::RecordBatch::Make(schema_, rows_, std::move(columns));
  auto flat_record_batch
    = arrow::ipc::SerializeRecordBatch(*record_batch,
                                       arrow::ipc::IpcWriteOptions::Defaults())
        .ValueOrDie();
  auto record_batch_buffer = builder_.CreateVector(flat_record_batch->data(),
                                                   flat_record_batch->size());
  // Create Arrow-encoded table slices.
  auto arrow_table_slice_buffer = fbs::table_slice::arrow::Createv0(
    builder_, layout_buffer, schema_buffer, record_batch_buffer);
  // Create and finish table slice.
  auto table_slice_buffer
    = fbs::CreateTableSlice(builder_, fbs::table_slice::TableSlice::arrow_v0,
                            arrow_table_slice_buffer.Union());
  fbs::FinishTableSliceBuffer(builder_, table_slice_buffer);
  // Reset the builder state.
  rows_ = {};
  // Create the table slice from the chunk.
  auto chunk = fbs::release(builder_);
  return table_slice{std::move(chunk), table_slice::verify::no, layout()};
}

size_t arrow_table_slice_builder::rows() const noexcept {
  return rows_;
}

caf::atom_value arrow_table_slice_builder::implementation_id() const noexcept {
  return caf::atom("arrow");
}

void arrow_table_slice_builder::reserve([[maybe_unused]] size_t num_rows) {
  // nop
}

// -- implementation details ---------------------------------------------------

arrow_table_slice_builder::arrow_table_slice_builder(record_type layout,
                                                     size_t initial_buffer_size)
  : table_slice_builder{std::move(layout)},
    schema_{make_arrow_schema(this->layout())},
    builder_{initial_buffer_size} {
  VAST_ASSERT(schema_);
  VAST_ASSERT(schema_->num_fields()
              == detail::narrow_cast<int>(this->layout().fields.size()));
  column_builders_.reserve(this->layout().fields.size());
  auto pool = arrow::default_memory_pool();
  for (auto& field : this->layout().fields)
    column_builders_.emplace_back(column_builder::make(field.type, pool));
}

bool arrow_table_slice_builder::add_impl(data_view x) {
  if (!column_builders_[column_]->add(x))
    return false;
  if (++column_ == layout().fields.size()) {
    ++rows_;
    column_ = 0;
  }
  return true;
}

// -- utility functions --------------------------------------------------------

std::shared_ptr<arrow::Schema> make_arrow_schema(const record_type& t) {
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  arrow_fields.reserve(t.fields.size());
  for (auto& field : t.fields) {
    auto field_ptr = arrow::field(field.name, make_arrow_type(field.type));
    arrow_fields.emplace_back(std::move(field_ptr));
  }
  auto metadata = arrow::key_value_metadata({{"name", t.name()}});
  return std::make_shared<arrow::Schema>(arrow_fields, metadata);
}

std::shared_ptr<arrow::DataType> make_arrow_type(const type& t) {
  using data_type_ptr = std::shared_ptr<arrow::DataType>;
  auto f = detail::overload{
    [=](const auto& x) -> data_type_ptr {
      using trait = column_builder_trait<std::decay_t<decltype(x)>>;
      return trait::make_arrow_type();
    },
    [=](const list_type& x) -> data_type_ptr {
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
    },
  };
  return caf::visit(f, t);
}

} // namespace vast
