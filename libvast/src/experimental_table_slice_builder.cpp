//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/experimental_table_slice_builder.hpp"

#include "vast/arrow_extension_types.hpp"
#include "vast/config.hpp"
#include "vast/detail/byte_swap.hpp"
#include "vast/detail/narrow.hpp"
#include "vast/detail/overload.hpp"
#include "vast/die.hpp"
#include "vast/experimental_table_slice.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/logger.hpp"
#include "vast/type.hpp"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/writer.h>

namespace vast {

// -- column builder implementations ------------------------------------------

namespace {

template <class VastType, class ArrowType>
struct column_builder_trait_base : arrow::TypeTraits<ArrowType> {
  using data_type = type_to_data_t<VastType>;
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
PRIMITIVE_COLUMN_BUILDER_TRAIT(count_type, arrow::UInt64Type);
PRIMITIVE_COLUMN_BUILDER_TRAIT(real_type, arrow::DoubleType);

#undef PRIMITIVE_COLUMN_BUILDER_TRAIT

template <>
struct column_builder_trait<integer_type>
  : column_builder_trait_base<integer_type, arrow::Int64Type> {
  using super = column_builder_trait_base<integer_type, arrow::Int64Type>;

  static auto make_arrow_type() {
    return super::type_singleton();
  }

  static bool
  append(typename super::BuilderType& builder, typename super::view_type x) {
    return builder.Append(x.value).ok();
  }
};

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

template <>
struct column_builder_trait<duration_type>
  : column_builder_trait_base<duration_type, arrow::DurationType> {
  using super = column_builder_trait_base<duration_type, arrow::DurationType>;

  static auto make_arrow_type() {
    return arrow::duration(arrow::TimeUnit::NANO);
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

class enum_column_builder
  : public experimental_table_slice_builder::column_builder {
public:
  // arrow::StringDictionaryBuilder would be appropriate, but requires data
  // to be appended as string, however the table slice only receives the uint8
  using arrow_builder_type = arrow::Int16Builder;

  explicit enum_column_builder(enumeration_type enum_type)
    : enum_type_{std::move(enum_type)},
      arr_builder_{std::make_shared<arrow::Int16Builder>()} {
  }

  bool add(data_view x) override {
    if (auto* xptr = caf::get_if<view<enumeration>>(&x))
      return arr_builder_->Append(*xptr).ok();
    if (caf::holds_alternative<view<caf::none_t>>(x))
      return arr_builder_->AppendNull().ok();
    return false;
  }

  std::shared_ptr<arrow::Array> finish() override {
    if (auto index_array = arr_builder_->Finish(); index_array.ok())
      return std::make_shared<arrow::DictionaryArray>(
        arrow::dictionary(arrow::int16(), arrow::utf8()),
        index_array.ValueUnsafe(), make_field_array());
    die("failed to finish Arrow enum builders");
  }

  [[nodiscard]] std::shared_ptr<arrow::ArrayBuilder>
  arrow_builder() const override {
    return arr_builder_;
  }

private:
  enumeration_type enum_type_;
  std::shared_ptr<arrow::Int16Builder> arr_builder_;

  [[nodiscard]] std::shared_ptr<arrow::Array> make_field_array() const {
    arrow::StringBuilder string_builder{};
    for (const auto& f : enum_type_.fields())
      if (!string_builder.Append(std::string{f.name}).ok())
        die("failed to build Arrow enum field array");
    if (auto array = string_builder.Finish(); array.ok()) {
      return *array;
    }
    die("failed to finish Arrow enum field array");
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
    auto bytes = as_bytes(x);
    auto ptr = reinterpret_cast<const char*>(bytes.data());
    auto str = arrow::util::string_view{ptr, bytes.size()};
    return builder.Append(str).ok();
  }
};

template <class Trait>
class column_builder_impl final
  : public experimental_table_slice_builder::column_builder {
public:
  using arrow_builder_type = typename Trait::BuilderType;

  explicit column_builder_impl(arrow::MemoryPool* pool) {
    if constexpr (Trait::is_parameter_free)
      reset(pool);
    else
      reset(Trait::make_arrow_type(), pool);
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

  [[nodiscard]] std::shared_ptr<arrow::ArrayBuilder>
  arrow_builder() const override {
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

class list_column_builder
  : public experimental_table_slice_builder::column_builder {
public:
  using arrow_builder_type = arrow::ListBuilder;

  using data_type = type_to_data_t<list_type>;

  list_column_builder(arrow::MemoryPool* pool,
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

  [[nodiscard]] std::shared_ptr<arrow::ArrayBuilder>
  arrow_builder() const override {
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

class map_column_builder
  : public experimental_table_slice_builder::column_builder {
public:
  using arrow_builder_type = arrow::MapBuilder;

  map_column_builder(arrow::MemoryPool* pool,
                     std::unique_ptr<column_builder> key_builder,
                     std::unique_ptr<column_builder> item_builder)
    : key_builder_(std::move(key_builder)),
      item_builder_(std::move(item_builder)) {
    map_builder_ = std::make_shared<arrow::MapBuilder>(
      pool, key_builder_->arrow_builder(), item_builder_->arrow_builder());
  }

  bool add(data_view x) override {
    if (caf::holds_alternative<view<caf::none_t>>(x))
      return map_builder_->AppendNull().ok();
    VAST_ASSERT(caf::holds_alternative<view<map>>(x));
    auto m = caf::get<view<map>>(x);
    if (!map_builder_->Append().ok())
      return false;
    for (auto entry : *m) {
      if (!key_builder_->add(entry.first) || !item_builder_->add(entry.second))
        return false;
    }
    return true;
  }

  std::shared_ptr<arrow::Array> finish() override {
    auto res = map_builder_->Finish();
    return res.ValueOrDie();
  }

  [[nodiscard]] std::shared_ptr<arrow::ArrayBuilder>
  arrow_builder() const override {
    return map_builder_;
  }

private:
  std::shared_ptr<arrow::MapBuilder> map_builder_;

  std::unique_ptr<column_builder> key_builder_;
  std::unique_ptr<column_builder> item_builder_;
};

class record_column_builder
  : public experimental_table_slice_builder::column_builder {
public:
  using data_type = view<record>;

  record_column_builder(
    arrow::MemoryPool* pool, std::shared_ptr<arrow::DataType> struct_type,
    std::vector<std::unique_ptr<column_builder>>&& field_builders)
    : field_builders_{std::move(field_builders)} {
    auto fields = std::vector<std::shared_ptr<arrow::ArrayBuilder>>{};
    fields.reserve(field_builders_.size());
    for (auto& field_builder : field_builders_) {
      auto underlying_builder = field_builder->arrow_builder();
      VAST_ASSERT(underlying_builder);
      fields.push_back(std::move(underlying_builder));
    }
    struct_builder_ = std::make_shared<arrow::StructBuilder>(struct_type, pool,
                                                             std::move(fields));
  }

  bool add(data_view x) override {
    if (caf::holds_alternative<view<caf::none_t>>(x)) {
      auto status = struct_builder_->AppendNull();
      return status.ok();
    }
    // Verify that we're actually holding a record.
    auto* xptr = caf::get_if<data_type>(&x);
    if (!xptr)
      return false;
    if (auto status = struct_builder_->Append(); !status.ok())
      return false;
    const auto& r = **xptr;
    VAST_ASSERT(r.size() == field_builders_.size(), "record size mismatch");
    for (size_t i = 0; i < r.size(); ++i) {
      VAST_ASSERT(struct_builder_->type()->field(i)->name() == r.at(i).first,
                  "field name mismatch");
      if (!field_builders_[i]->add(r.at(i).second))
        return false;
    }
    return true;
  }

  std::shared_ptr<arrow::Array> finish() override {
    std::shared_ptr<arrow::Array> result;
    if (!struct_builder_->Finish(&result).ok())
      die("failed to finish Arrow column builder");
    return result;
  }

  [[nodiscard]] std::shared_ptr<arrow::ArrayBuilder>
  arrow_builder() const override {
    return struct_builder_;
  }

private:
  std::shared_ptr<arrow::StructBuilder> struct_builder_;

  std::vector<std::unique_ptr<column_builder>> field_builders_;
};

class subnet_column_builder
  : public experimental_table_slice_builder::column_builder {
public:
  using data_type = view<subnet>;
  using view_type = view<data_type>;

  subnet_column_builder(arrow::MemoryPool* pool)
    : length_builder_(std::make_shared<arrow::UInt8Builder>()),
      address_builder_(std::make_shared<arrow::FixedSizeBinaryBuilder>(
        address_extension_type::arrow_type, pool)) {
    std::vector<std::shared_ptr<arrow::ArrayBuilder>> fields{length_builder_,
                                                             address_builder_};
    subnet_builder_ = std::make_shared<arrow::StructBuilder>(
      subnet_extension_type::arrow_type, pool, fields);
  }

  bool add(data_view data) override {
    if (caf::holds_alternative<view<caf::none_t>>(data))
      return subnet_builder_->AppendNull().ok();
    if (auto* dataptr = caf::get_if<view_type>(&data)) {
      const auto* addr_ptr
        = reinterpret_cast<const char*>(as_bytes(dataptr->network()).data());
      return subnet_builder_->Append().ok()
             && length_builder_->Append(dataptr->length()).ok()
             && address_builder_->Append(addr_ptr).ok();
    }
    return false;
  }

  std::shared_ptr<arrow::Array> finish() override {
    std::shared_ptr<arrow::Array> result;
    if (!subnet_builder_->Finish(&result).ok())
      die("failed to finish Arrow subnet column builder");
    return result;
  }

  [[nodiscard]] std::shared_ptr<arrow::ArrayBuilder>
  arrow_builder() const override {
    return subnet_builder_;
  }

private:
  std::shared_ptr<arrow::UInt8Builder> length_builder_;
  std::shared_ptr<arrow::FixedSizeBinaryBuilder> address_builder_;
  std::shared_ptr<arrow::StructBuilder> subnet_builder_;
};

} // namespace

// -- member types -------------------------------------------------------------

experimental_table_slice_builder::column_builder::~column_builder() noexcept {
  // nop
}

std::unique_ptr<experimental_table_slice_builder::column_builder>
experimental_table_slice_builder::column_builder::make(
  const type& t, arrow::MemoryPool* pool) {
  auto f = detail::overload{
    [=](const auto& x) -> std::unique_ptr<column_builder> {
      return std::make_unique<column_builder_impl_t<std::decay_t<decltype(x)>>>(
        pool);
    },
    [=](const list_type& x) -> std::unique_ptr<column_builder> {
      auto nested = column_builder::make(x.value_type(), pool);
      return std::make_unique<list_column_builder>(pool, std::move(nested));
    },
    [=](const map_type& x) -> std::unique_ptr<column_builder> {
      auto key_builder = column_builder::make(x.key_type(), pool);
      auto value_builder = column_builder::make(x.value_type(), pool);
      record_type fields{{"key", x.key_type()}, {"value", x.value_type()}};
      return std::make_unique<map_column_builder>(pool, std::move(key_builder),
                                                  std::move(value_builder));
    },
    [&](const subnet_type&) -> std::unique_ptr<column_builder> {
      return std::make_unique<subnet_column_builder>(pool);
    },
    [&](const enumeration_type& x) -> std::unique_ptr<column_builder> {
      return std::make_unique<enum_column_builder>(x);
    },
    [=](const record_type& x) -> std::unique_ptr<column_builder> {
      auto field_builders = std::vector<std::unique_ptr<column_builder>>{};
      field_builders.reserve(x.num_fields());
      for (const auto& field : x.fields())
        field_builders.push_back(column_builder::make(field.type, pool));
      return std::make_unique<record_column_builder>(
        pool, make_experimental_type(type{x}), std::move(field_builders));
    },
  };
  return caf::visit(f, t);
}

// -- constructors, destructors, and assignment operators ----------------------

table_slice_builder_ptr
experimental_table_slice_builder::make(type layout,
                                       size_t initial_buffer_size) {
  return table_slice_builder_ptr{new experimental_table_slice_builder{
                                   std::move(layout), initial_buffer_size},
                                 false};
}

experimental_table_slice_builder::~experimental_table_slice_builder() noexcept {
  // nop
}

// -- properties ---------------------------------------------------------------

size_t experimental_table_slice_builder::columns() const noexcept {
  auto result = schema_->num_fields();
  VAST_ASSERT(result >= 0);
  return detail::narrow_cast<size_t>(result);
}

table_slice experimental_table_slice_builder::finish() {
  // Sanity check: If this triggers, the calls to add() did not match the number
  // of fields in the layout.
  VAST_ASSERT(column_ == 0);
  // Pack layout.
  const auto layout_bytes = as_bytes(layout());
  auto layout_buffer = builder_.CreateVector(
    reinterpret_cast<const uint8_t*>(layout_bytes.data()), layout_bytes.size());
  // Pack schema.
  // Pack record batch.
  auto columns = std::vector<std::shared_ptr<arrow::Array>>{};
  columns.reserve(column_builders_.size());
  for (auto&& builder : column_builders_)
    columns.emplace_back(builder->finish());
  auto record_batch
    = arrow::RecordBatch::Make(schema_, rows_, std::move(columns));
  auto ipc_ostream = arrow::io::BufferOutputStream::Create().ValueOrDie();
  auto stream_writer
    = arrow::ipc::MakeStreamWriter(ipc_ostream, schema_).ValueOrDie();
  auto status = stream_writer->WriteRecordBatch(*record_batch);
  if (!status.ok())
    VAST_ERROR("failed to write record batch: {}", status);
  auto arrow_ipc_buffer = ipc_ostream->Finish().ValueOrDie();
  auto fbs_ipc_buffer
    = builder_.CreateVector(arrow_ipc_buffer->data(), arrow_ipc_buffer->size());
  // Create Arrow-encoded table slices. We need to set the import time to
  // something other than 0, as it cannot be modified otherwise. We then later
  // reset it to the clock's epoch.
  constexpr int64_t stub_ns_since_epoch = 1337;
  auto arrow_table_slice_buffer = fbs::table_slice::arrow::Createexperimental(
    builder_, layout_buffer, fbs_ipc_buffer, stub_ns_since_epoch);
  // Create and finish table slice.
  auto table_slice_buffer
    = fbs::CreateTableSlice(builder_,
                            fbs::table_slice::TableSlice::arrow_experimental,
                            arrow_table_slice_buffer.Union());
  fbs::FinishTableSliceBuffer(builder_, table_slice_buffer);
  // Reset the builder state.
  rows_ = {};
  // Create the table slice from the chunk.
  auto chunk = fbs::release(builder_);
  auto result = table_slice{std::move(chunk), table_slice::verify::no};
  result.import_time(time{});
  return result;
}

table_slice experimental_table_slice_builder::create(
  const std::shared_ptr<arrow::RecordBatch>& record_batch, const type& layout,
  size_t initial_buffer_size) {
  VAST_ASSERT(record_batch->schema()->Equals(make_experimental_schema(layout)),
              "record layout doesn't match record batch schema");
  auto builder = flatbuffers::FlatBufferBuilder{initial_buffer_size};
  // Pack layout.
  const auto layout_bytes = as_bytes(layout);
  auto layout_buffer = builder.CreateVector(
    reinterpret_cast<const uint8_t*>(layout_bytes.data()), layout_bytes.size());
  // Pack record batch.
  auto flat_record_batch
    = arrow::ipc::SerializeRecordBatch(*record_batch,
                                       arrow::ipc::IpcWriteOptions::Defaults())
        .ValueOrDie();
  auto record_batch_buffer = builder.CreateVector(flat_record_batch->data(),
                                                  flat_record_batch->size());
  // Create Arrow-encoded table slices. We need to set the import time to
  // something other than 0, as it cannot be modified otherwise. We then later
  // reset it to the clock's epoch.
  constexpr int64_t stub_ns_since_epoch = 1337;
  auto arrow_table_slice_buffer = fbs::table_slice::arrow::Createexperimental(
    builder, layout_buffer, record_batch_buffer, stub_ns_since_epoch);
  // Create and finish table slice.
  auto table_slice_buffer
    = fbs::CreateTableSlice(builder,
                            fbs::table_slice::TableSlice::arrow_experimental,
                            arrow_table_slice_buffer.Union());
  fbs::FinishTableSliceBuffer(builder, table_slice_buffer);
  // Create the table slice from the chunk.
  auto chunk = fbs::release(builder);
  auto result = table_slice{std::move(chunk), table_slice::verify::no};
  result.import_time(time{});
  return result;
}

size_t experimental_table_slice_builder::rows() const noexcept {
  return rows_;
}

table_slice_encoding
experimental_table_slice_builder::implementation_id() const noexcept {
  return table_slice_encoding::experimental;
}

void experimental_table_slice_builder::reserve(
  [[maybe_unused]] size_t num_rows) {
  // nop
}

// -- implementation details ---------------------------------------------------

experimental_table_slice_builder::experimental_table_slice_builder(
  type layout, size_t initial_buffer_size)
  : table_slice_builder{std::move(layout)},
    schema_{make_experimental_schema(this->layout())},
    builder_{initial_buffer_size} {
  VAST_ASSERT(schema_);
  const auto& rt = caf::get<record_type>(this->layout());
  VAST_ASSERT(schema_->num_fields()
              == detail::narrow_cast<int>(rt.num_leaves()));
  column_builders_.reserve(columns());
  auto* pool = arrow::default_memory_pool();
  for (const auto& [field, _] : rt.leaves())
    column_builders_.emplace_back(column_builder::make(field.type, pool));
}

bool experimental_table_slice_builder::add_impl(data_view x) {
  if (!column_builders_[column_]->add(x))
    return false;
  if (++column_ == columns()) {
    ++rows_;
    column_ = 0;
  }
  return true;
}

// -- utility functions --------------------------------------------------------

std::shared_ptr<arrow::Schema> make_experimental_schema(const type& t) {
  VAST_ASSERT(caf::holds_alternative<record_type>(t));
  const auto& rt = flatten(caf::get<record_type>(t));
  std::vector<std::shared_ptr<arrow::Field>> arrow_fields;
  arrow_fields.reserve(rt.num_leaves());
  for (const auto& field : rt.fields())
    arrow_fields.emplace_back(make_experimental_field(field));
  auto metadata = arrow::key_value_metadata({{"name", std::string{t.name()}}});
  return std::make_shared<arrow::Schema>(arrow_fields, metadata);
}

std::shared_ptr<arrow::Field>
make_experimental_field(const record_type::field_view& field) {
  const auto& arrow_type = make_experimental_type(field.type);
  return arrow::field(std::string{field.name}, arrow_type);
}

std::shared_ptr<arrow::DataType> make_experimental_type(const type& t) {
  using data_type_ptr = std::shared_ptr<arrow::DataType>;
  auto f = detail::overload{
    [](const auto& x) -> data_type_ptr {
      using trait = column_builder_trait<std::decay_t<decltype(x)>>;
      return trait::make_arrow_type();
    },
    [](const enumeration_type& x) {
      return make_arrow_enum(x);
    },
    [](const address_type&) {
      return make_arrow_address();
    },
    [](const subnet_type&) {
      return make_arrow_subnet();
    },
    [](const pattern_type&) {
      return make_arrow_pattern();
    },
    [](const list_type& x) -> data_type_ptr {
      return arrow::list(make_experimental_type(x.value_type()));
    },
    [](const map_type& x) -> data_type_ptr {
      return arrow::map(make_experimental_type(x.key_type()),
                        make_experimental_type(x.value_type()));
    },
    [](const record_type& x) -> data_type_ptr {
      std::vector<std::shared_ptr<arrow::Field>> fields;
      fields.reserve(x.num_fields());
      for (const auto& field : x.fields()) {
        auto ptr = arrow::field(std::string{field.name},
                                make_experimental_type(field.type));
        fields.emplace_back(std::move(ptr));
      }
      return arrow::struct_(fields);
    },
  };
  return caf::visit(f, t);
}

// NOLINTNEXTLINE(misc-no-recursion)
type make_vast_type(const arrow::DataType& arrow_type) {
  switch (arrow_type.id()) {
    case arrow::Type::NA:
      return type{none_type{}};
    case arrow::Type::BOOL:
      return type{bool_type{}};
    case arrow::Type::INT64:
      return type{integer_type{}};
    case arrow::Type::UINT64:
      return type{count_type{}};
    case arrow::Type::DOUBLE:
      return type{real_type{}};
    case arrow::Type::DURATION: {
      const auto& t = static_cast<const arrow::DurationType&>(arrow_type);
      if (t.unit() != arrow::TimeUnit::NANO)
        die(fmt::format("unhandled Arrow type: Duration[{}]", t.unit()));
      return type{duration_type{}};
    }
    case arrow::Type::STRING:
      return type{string_type{}};
    case arrow::Type::TIMESTAMP: {
      const auto& t = static_cast<const arrow::TimestampType&>(arrow_type);
      if (t.unit() != arrow::TimeUnit::NANO)
        die(fmt::format("unhandled Arrow type: Timestamp[{}]", t.unit()));
      return type{time_type{}};
    }
    case arrow::Type::FIXED_SIZE_BINARY: {
      const auto& t
        = static_cast<const arrow::FixedSizeBinaryType&>(arrow_type);
      switch (auto width = t.byte_width(); width) {
        case 17:
          return type{subnet_type{}};
        default:
          die(fmt::format("unhandled Arrow type: FIXEDBINARY[{}]", width));
      }
      return type{time_type{}};
    }
    case arrow::Type::MAP: {
      const auto& t = static_cast<const arrow::MapType&>(arrow_type);
      return type{map_type{
        make_vast_type(*t.key_type()),
        make_vast_type(*t.item_type()),
      }};
    }
    case arrow::Type::LIST: {
      const auto& t = static_cast<const arrow::ListType&>(arrow_type);
      const auto& embedded_type = make_vast_type(*t.value_type());
      return type{list_type{embedded_type}};
    }
    case arrow::Type::STRUCT: {
      std::vector<record_type::field_view> field_types;
      field_types.reserve(arrow_type.num_fields());
      for (const auto& f : arrow_type.fields())
        field_types.emplace_back(f->name(), make_vast_type(*f->type()));
      return type{record_type{field_types}};
    }
    case arrow::Type::EXTENSION: {
      const auto& t = static_cast<const arrow::ExtensionType&>(arrow_type);
      if (t.extension_name() == "vast.enum") {
        const auto& et = static_cast<const enum_extension_type&>(arrow_type);
        return type{et.get_enum_type()};
      }
      if (t.extension_name() == address_extension_type::id)
        return type{address_type{}};
      if (t.extension_name() == subnet_extension_type::id)
        return type{subnet_type{}};
      if (t.extension_name() == pattern_extension_type::id)
        return type{pattern_type{}};
      die(
        fmt::format("unhandled Arrow extension type: {}", t.extension_name()));
    }
    default:
      die(fmt::format("unhandled Arrow type: {}", arrow_type.ToString()));
  }
}

type make_vast_type(const arrow::Schema& arrow_schema) {
  std::vector<record_type::field_view> field_types;
  field_types.reserve(arrow_schema.num_fields());
  for (const auto& f : arrow_schema.fields())
    field_types.emplace_back(f->name(), make_vast_type(*f->type()));
  return type{record_type{field_types}};
}

} // namespace vast
