//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/table_slice.hpp"

#include "tenzir/arrow_table_slice.hpp"
#include "tenzir/arrow_utils.hpp"
#include "tenzir/bitmap_algorithms.hpp"
#include "tenzir/chunk.hpp"
#include "tenzir/collect.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/default_formatter.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/detail/zip_iterator.hpp"
#include "tenzir/expression.hpp"
#include "tenzir/fbs/table_slice.hpp"
#include "tenzir/ids.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/series.hpp"
#include "tenzir/table_slice_builder.hpp"
#include "tenzir/tql2/ast.hpp"
#include "tenzir/type.hpp"
#include "tenzir/value_index.hpp"

#include <arrow/record_batch.h>

#include <cstddef>
#include <ranges>
#include <span>

namespace tenzir {

// -- utility functions --------------------------------------------------------

namespace {

/// Visits a FlatBuffers table slice to dispatch to its specific encoding.
/// @param visitor A callable object to dispatch to.
/// @param x The FlatBuffers root type for table slices.
/// @note The handler for invalid table slices takes no arguments. If none is
/// specified, visig aborts when the table slice encoding is invalid.
template <class Visitor>
auto visit(Visitor&& visitor, const fbs::TableSlice* x) noexcept(
  std::conjunction_v<
    // Check whether the handlers for all other table slice encodings are
    // noexcept-specified. When adding a new encoding, add it here as well.
    std::is_nothrow_invocable<Visitor>,
    std::is_nothrow_invocable<Visitor, const fbs::table_slice::arrow::v2&>>) {
  if (!x)
    return std::invoke(std::forward<Visitor>(visitor));
  switch (x->table_slice_type()) {
    case fbs::table_slice::TableSlice::NONE:
      return std::invoke(std::forward<Visitor>(visitor));
    case fbs::table_slice::TableSlice::msgpack_v0:
    case fbs::table_slice::TableSlice::msgpack_v1:
    case fbs::table_slice::TableSlice::arrow_v0:
    case fbs::table_slice::TableSlice::arrow_v1:
      die("outdated table slice encoding");
    case fbs::table_slice::TableSlice::arrow_v2:
      return std::invoke(std::forward<Visitor>(visitor),
                         *x->table_slice_as_arrow_v2());
  }
  // GCC-8 fails to recognize that this can never be reached, so we just call a
  // [[noreturn]] function.
  die("unhandled table slice encoding");
}

/// Get a pointer to the `tenzir.fbs.TableSlice` inside the chunk.
/// @param chunk The chunk to look at.
const fbs::TableSlice* as_flatbuffer(const chunk_ptr& chunk) noexcept {
  if (!chunk)
    return nullptr;
  return fbs::GetTableSlice(chunk->data());
}

/// Verifies that the chunk contains a valid `tenzir.fbs.TableSlice` FlatBuffers
/// table and returns the `chunk` itself, or returns `nullptr`.
/// @param chunk The chunk to verify.
/// @param verify Whether to verify the chunk.
/// @note This is a no-op if `verify == table_slice::verify::no`.
chunk_ptr
verified_or_none(chunk_ptr&& chunk, enum table_slice::verify verify) noexcept {
  if (verify == table_slice::verify::yes && chunk) {
    const auto* const data = reinterpret_cast<const uint8_t*>(chunk->data());
    auto verifier = flatbuffers::Verifier{data, chunk->size()};
    if (!verifier.template VerifyBuffer<fbs::TableSlice>())
      chunk = {};
  }
  return std::move(chunk);
}

/// A helper utility for accessing the state of a table slice.
/// @param encoded The encoding-specific FlatBuffers table.
/// @param state The encoding-specific runtime state of the table slice.
template <class Slice, class State>
constexpr auto&
state([[maybe_unused]] Slice&& encoded, State&& state) noexcept {
  return std::forward<State>(state).arrow_v2;
}

} // namespace

// -- constructors, destructors, and assignment operators ----------------------

table_slice::table_slice() noexcept = default;

table_slice::table_slice(chunk_ptr&& chunk, enum verify verify,
                         const std::shared_ptr<arrow::RecordBatch>& batch,
                         type schema) noexcept
  : chunk_{verified_or_none(std::move(chunk), verify)} {
  TENZIR_ASSERT(!chunk_ || chunk_->unique());
  if (chunk_) {
    ++num_instances_;
    auto f = detail::overload{
      []() noexcept {
        die("invalid table slice encoding");
      },
      [&](const auto& encoded) noexcept {
        auto& state_ptr = state(encoded, state_);
        auto state = std::make_unique<std::decay_t<decltype(*state_ptr)>>(
          encoded, chunk_, batch, std::move(schema));
        state_ptr = state.get();
        const auto bytes = as_bytes(chunk_);
        // We create a second chunk with an intentionally decoupled reference
        // count here that we bind to the lifetime of the original chunk. This
        // avoids cyclic references between the table slice and its
        // encoding-specific state.
        chunk_
          = chunk::make(bytes, [state = std::move(state),
                                chunk = std::move(chunk_)]() mutable noexcept {
              --num_instances_;
              // We manually call the destructors in proper order here, as the
              // state (and thus the contained chunk that actually owns the
              // memory we decoupled) must be destroyed last and the destruction
              // order for lambda captures is undefined.
              chunk = {};
              state = {};
            });
      },
    };
    visit(f, as_flatbuffer(chunk_));
  }
}

table_slice::table_slice(const fbs::FlatTableSlice& flat_slice,
                         const chunk_ptr& parent_chunk,
                         enum verify verify) noexcept
  : table_slice(parent_chunk->slice(as_bytes(*flat_slice.data())), verify) {
  // nop
}

table_slice::table_slice(const std::shared_ptr<arrow::RecordBatch>& record_batch,
                         type schema, enum serialize serialize) {
  *this
    = table_slice_builder::create(record_batch, std::move(schema), serialize);
}

table_slice::table_slice(const table_slice& other) noexcept = default;

table_slice& table_slice::operator=(const table_slice& rhs) noexcept {
  if (this == &rhs)
    return *this;
  chunk_ = rhs.chunk_;
  offset_ = rhs.offset_;
  state_ = rhs.state_;
  return *this;
}

table_slice::table_slice(table_slice&& other) noexcept
  : chunk_{std::exchange(other.chunk_, {})},
    offset_{std::exchange(other.offset_, invalid_id)},
    state_{std::exchange(other.state_, {})} {
  // nop
}

table_slice& table_slice::operator=(table_slice&& rhs) noexcept {
  chunk_ = std::exchange(rhs.chunk_, {});
  offset_ = std::exchange(rhs.offset_, invalid_id);
  state_ = std::exchange(rhs.state_, {});
  return *this;
}

table_slice::~table_slice() noexcept = default;

table_slice table_slice::unshare() const noexcept {
  auto result = table_slice{chunk::copy(chunk_), verify::no};
  result.offset_ = offset_;
  return result;
}

// -- operators ----------------------------------------------------------------

// TODO: Dispatch to optimized implementations if the encodings are the same.
bool operator==(const table_slice& lhs, const table_slice& rhs) noexcept {
  if (!lhs.chunk_ && !rhs.chunk_)
    return true;
  constexpr auto check_metadata = true;
  return to_record_batch(lhs)->Equals(*to_record_batch(rhs), check_metadata);
}

bool operator!=(const table_slice& lhs, const table_slice& rhs) noexcept {
  return !(lhs == rhs);
}

// -- properties ---------------------------------------------------------------

const type& table_slice::schema() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      static const auto schema = type{};
      return &schema;
    },
    [&](const auto& encoded) noexcept {
      return &state(encoded, state_)->schema();
    },
  };
  return *visit(f, as_flatbuffer(chunk_));
}

table_slice::size_type table_slice::rows() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      return size_type{};
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->rows();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

table_slice::size_type table_slice::columns() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      return size_type{};
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->columns();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

id table_slice::offset() const noexcept {
  return offset_;
}

void table_slice::offset(id offset) noexcept {
  offset_ = offset;
}

time table_slice::import_time() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      return time{};
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->import_time();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

void table_slice::import_time(time import_time) noexcept {
  if (import_time == this->import_time())
    return;
  modify_state([&](auto& state) {
    state.import_time(import_time);
  });
}

template <class F>
void table_slice::modify_state(F&& f) {
  // We work around the uniqueness requirement for the const_cast below by
  // creating a new table slice here that points to the same data as the current
  // table slice. This implies that the table slice is no longer in one
  // contiguous buffer.
  if (chunk_ && !chunk_->unique())
    *this = table_slice{to_record_batch(*this), schema()};
  auto g = detail::overload{
    []() noexcept {
      die("cannot assign import time to invalid table slice");
    },
    [&](const auto& encoded) noexcept {
      auto& mutable_state
        = const_cast<std::add_lvalue_reference_t<std::remove_const_t<
          std::remove_reference_t<decltype(*state(encoded, state_))>>>>(
          *state(encoded, state_));
      f(mutable_state);
    },
  };
  visit(g, as_flatbuffer(chunk_));
}

bool table_slice::is_serialized() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      return true;
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->is_serialized();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

size_t table_slice::instances() noexcept {
  return num_instances_;
}

// -- data access --------------------------------------------------------------

auto table_slice::values() const -> generator<view<record>> {
  auto path = tenzir::offset{};
  auto [type, array] = path.get(*this);
  const auto as_series = series{std::move(type), std::move(array)};
  for (auto&& value : as_series.values<record_type>()) {
    TENZIR_ASSERT_EXPENSIVE(value);
    co_yield std::move(*value);
  }
}

auto table_slice::values(const struct offset& path) const
  -> generator<view<data>> {
  auto [type, array] = path.get(*this);
  const auto as_series = series{std::move(type), std::move(array)};
  return as_series.values();
}

void table_slice::append_column_to_index(table_slice::size_type column,
                                         value_index& index) const {
  TENZIR_ASSERT(offset() != invalid_id);
  auto f = detail::overload{
    []() noexcept {
      die("cannot append column of invalid table slice to index");
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)
        ->append_column_to_index(offset(), column, index);
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

data_view table_slice::at(table_slice::size_type row,
                          table_slice::size_type column) const {
  TENZIR_ASSERT(row < rows());
  TENZIR_ASSERT(column < columns());
  auto f = detail::overload{
    [&]() noexcept -> data_view {
      die("cannot access data of invalid table slice");
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->at(row, column);
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

data_view table_slice::at(table_slice::size_type row,
                          table_slice::size_type column, const type& t) const {
  TENZIR_ASSERT(row < rows());
  TENZIR_ASSERT(column < columns());
  auto f = detail::overload{
    [&]() noexcept -> data_view {
      die("cannot access data of invalid table slice");
    },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->at(row, column, t);
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

std::shared_ptr<arrow::RecordBatch> to_record_batch(const table_slice& slice) {
  auto f = detail::overload{
    []() noexcept -> std::shared_ptr<arrow::RecordBatch> {
      die("cannot access record batch of invalid table slice");
    },
    [&](const auto& encoded) noexcept -> std::shared_ptr<arrow::RecordBatch> {
      // The following does not work on all compilers, hence the ugly
      // decay+decltype workaround:
      //   if constexpr (state(encoding, slice.state_)->encoding
      //                 == table_slice_encoding::arrow) { ... }
      return state(encoded, slice.state_)->record_batch();
    },
  };
  return visit(f, as_flatbuffer(slice.chunk_));
}

// -- concepts -----------------------------------------------------------------

std::span<const std::byte> as_bytes(const table_slice& slice) noexcept {
  TENZIR_ASSERT(slice.is_serialized());
  return as_bytes(slice.chunk_);
}

// -- operations ---------------------------------------------------------------

table_slice concatenate(std::vector<table_slice> slices) {
  slices.erase(std::remove_if(slices.begin(), slices.end(),
                              [](const auto& slice) {
                                return slice.rows() == 0;
                              }),
               slices.end());
  if (slices.empty())
    return {};
  if (slices.size() == 1)
    return std::move(slices[0]);
  auto schema = slices[0].schema();
  TENZIR_ASSERT_EXPENSIVE(std::all_of(slices.begin(), slices.end(),
                                      [&](const auto& slice) {
                                        return slice.schema() == schema;
                                      }),
                          "concatenate requires slices to be homogeneous");
  auto builder = caf::get<record_type>(schema).make_arrow_builder(
    arrow::default_memory_pool());
  auto arrow_schema = schema.to_arrow_schema();
  const auto resize_result
    = builder->Resize(detail::narrow_cast<int64_t>(rows(slices)));
  TENZIR_ASSERT(resize_result.ok(), resize_result.ToString().c_str());

  for (const auto& slice : slices) {
    auto batch = to_record_batch(slice);
    auto status = append_array(*builder, caf::get<record_type>(schema),
                               *batch->ToStructArray().ValueOrDie());
    TENZIR_ASSERT(status.ok());
  }
  const auto rows = builder->length();
  if (rows == 0)
    return {};
  const auto array = builder->Finish().ValueOrDie();
  auto batch = arrow::RecordBatch::Make(
    std::move(arrow_schema), rows,
    caf::get<type_to_arrow_array_t<record_type>>(*array).fields());
  auto result = table_slice{batch, schema};
  result.offset(slices[0].offset());
  result.import_time(slices[0].import_time());
  return result;
}

generator<table_slice>
select(const table_slice& slice, expression expr, const ids& hints) {
  if (slice.rows() == 0) {
    co_return;
  }
  const auto offset = slice.offset() == invalid_id ? 0 : slice.offset();
  auto slice_ids = make_ids({{offset, offset + slice.rows()}});
  auto selection = slice_ids;
  if (!hints.empty())
    selection &= hints;
  // Do no rows qualify?
  if (!any(selection))
    co_return;
  // Evaluate the filter expression.
  if (!caf::holds_alternative<caf::none_t>(expr)) {
    // Tailor the expression to the type; this is required for using the
    // evaluate function, which expects field and type extractors to be resolved
    // already.
    auto tailored_expr = tailor(expr, slice.schema());
    if (!tailored_expr)
      co_return;
    selection = evaluate(*tailored_expr, slice, selection);
    // Do no rows qualify?
    if (!any(selection))
      co_return;
  }
  // Do all rows qualify?
  if (rank(selection) == slice.rows()) {
    co_yield slice;
    co_return;
  }
  // Start slicing and dicing.
  auto batch = to_record_batch(slice);
  for (const auto [first, last] : select_runs(selection)) {
    co_yield subslice(slice, first - offset, last - offset);
  }
}

table_slice head(table_slice slice, size_t num_rows) {
  return subslice(
    slice, 0,
    std::min(slice.rows(),
             detail::narrow_cast<table_slice::size_type>(num_rows)));
}

table_slice tail(table_slice slice, size_t num_rows) {
  return subslice(
    slice,
    slice.rows()
      - std::min(slice.rows(),
                 detail::narrow_cast<table_slice::size_type>(num_rows)),
    slice.rows());
}

std::pair<table_slice, table_slice>
split(const table_slice& slice, size_t partition_point) {
  if (slice.rows() == 0) {
    return {{}, {}};
  }
  if (partition_point == 0)
    return {{}, slice};
  if (partition_point >= slice.rows())
    return {slice, {}};
  return {
    head(slice, partition_point),
    tail(slice, slice.rows() - partition_point),
  };
}

auto split(std::vector<table_slice> events, uint64_t partition_point)
  -> std::pair<std::vector<table_slice>, std::vector<table_slice>> {
  auto it = events.begin();
  for (; it != events.end(); ++it) {
    if (partition_point == it->rows()) {
      return {
        {events.begin(), it + 1},
        {it + 1, events.end()},
      };
    }
    if (partition_point < it->rows()) {
      auto lhs = std::vector<table_slice>{};
      auto rhs = std::vector<table_slice>{};
      lhs.reserve(std::distance(events.begin(), it + 1));
      rhs.reserve(std::distance(it, events.end()));
      lhs.insert(lhs.end(), std::make_move_iterator(events.begin()),
                 std::make_move_iterator(it));
      auto [split_lhs, split_rhs] = split(*it, partition_point);
      lhs.push_back(std::move(split_lhs));
      rhs.push_back(std::move(split_rhs));
      rhs.insert(rhs.end(), std::make_move_iterator(it + 1),
                 std::make_move_iterator(events.end()));
      return {
        std::move(lhs),
        std::move(rhs),
      };
    }
    partition_point -= it->rows();
  }
  return {
    std::move(events),
    {},
  };
}

auto subslice(const table_slice& slice, size_t begin, size_t end)
  -> table_slice {
  TENZIR_ASSERT(begin <= end);
  TENZIR_ASSERT(end <= slice.rows());
  if (begin == 0 && end == slice.rows()) {
    return slice;
  }
  if (begin == end) {
    return {};
  }
  auto offset = slice.offset();
  auto batch = to_record_batch(slice);
  auto sub_slice = table_slice{
    batch->Slice(detail::narrow_cast<int64_t>(begin),
                 detail::narrow_cast<int64_t>(end - begin)),
    slice.schema(),
  };
  sub_slice.offset(offset + begin);
  sub_slice.import_time(slice.import_time());
  return sub_slice;
}

uint64_t rows(const std::vector<table_slice>& slices) {
  auto result = uint64_t{0};
  for (const auto& slice : slices)
    result += slice.rows();
  return result;
}

std::optional<table_slice>
filter(const table_slice& slice, expression expr, const ids& hints) {
  if (slice.rows() == 0) {
    return {};
  }
  auto selected = collect(select(slice, std::move(expr), hints));
  if (selected.empty())
    return {};
  return concatenate(std::move(selected));
}

std::optional<table_slice>
filter(const table_slice& slice, const expression& expr) {
  return filter(slice, expr, ids{});
}

std::optional<table_slice> filter(const table_slice& slice, const ids& hints) {
  return filter(slice, expression{}, hints);
}

uint64_t count_matching(const table_slice& slice, const expression& expr,
                        const ids& hints) {
  if (slice.rows() == 0) {
    return 0;
  }
  const auto offset = slice.offset() == invalid_id ? 0 : slice.offset();
  if (expr == expression{}) {
    auto result = uint64_t{};
    for (auto id : select(hints)) {
      if (id < offset)
        continue;
      if (id >= offset + slice.rows())
        break;
      ++result;
    }
    return result;
  }
  // Tailor the expression to the type; this is required for using the
  // evaluate function, which expects field and type extractors to be resolved
  // already.
  auto tailored_expr = tailor(expr, slice.schema());
  if (!tailored_expr)
    return 0;
  return rank(evaluate(expr, slice, hints));
}

table_slice resolve_enumerations(table_slice slice) {
  if (slice.rows() == 0) {
    return slice;
  }
  auto type = caf::get<record_type>(slice.schema());
  // Resolve enumeration types, if there are any.
  auto transformations = std::vector<indexed_transformation>{};
  for (const auto& [field, index] : type.leaves()) {
    if (!caf::holds_alternative<enumeration_type>(field.type))
      continue;
    static auto transformation =
      [](struct record_type::field field,
         std::shared_ptr<arrow::Array> array) noexcept
      -> std::vector<
        std::pair<struct record_type::field, std::shared_ptr<arrow::Array>>> {
      const auto& et = caf::get<enumeration_type>(field.type);
      auto new_type = tenzir::type{string_type{}};
      new_type.assign_metadata(field.type);
      auto builder
        = string_type::make_arrow_builder(arrow::default_memory_pool());
      for (const auto& value : values(
             et, caf::get<type_to_arrow_array_t<enumeration_type>>(*array))) {
        if (!value) {
          const auto append_result = builder->AppendNull();
          TENZIR_ASSERT_EXPENSIVE(append_result.ok(),
                                  append_result.ToString().c_str());
          continue;
        }
        const auto append_result
          = append_builder(string_type{}, *builder, et.field(*value));
        TENZIR_ASSERT_EXPENSIVE(append_result.ok(),
                                append_result.ToString().c_str());
      }
      return {{
        {field.name, new_type},
        builder->Finish().ValueOrDie(),
      }};
    };
    transformations.push_back({index, transformation});
  }
  return transform_columns(slice, transformations);
}

auto resolve_meta_extractor(const table_slice& slice, const meta_extractor& ex)
  -> data {
  if (slice.rows() == 0) {
    return {};
  }
  switch (ex.kind) {
    case meta_extractor::schema: {
      return std::string{slice.schema().name()};
    }
    case meta_extractor::schema_id: {
      return slice.schema().make_fingerprint();
    }
    case meta_extractor::import_time: {
      const auto import_time = slice.import_time();
      if (import_time == time{}) {
        // The table slice API returns the epoch timestamp if no import
        // time was set, so we convert that to null.
        return {};
      }
      return import_time;
    }
    case meta_extractor::internal: {
      return slice.schema().attribute("internal").has_value();
    }
  }
  die("unhandled meta extractor kind");
}

auto resolve_operand(const table_slice& slice, const operand& op)
  -> std::pair<type, std::shared_ptr<arrow::Array>> {
  if (slice.rows() == 0) {
    return {};
  }
  const auto batch = to_record_batch(slice);
  const auto& layout = caf::get<record_type>(slice.schema());
  auto inferred_type = type{};
  auto array = std::shared_ptr<arrow::Array>{};
  // Helper function that binds a fixed value.
  auto bind_value = [&](const data& value) {
    auto tmp_inferred_type = type::infer(value);
    if (not tmp_inferred_type) {
      return;
    }
    inferred_type = *tmp_inferred_type;
    if (not inferred_type) {
      inferred_type = type{null_type{}};
      auto builder
        = null_type::make_arrow_builder(arrow::default_memory_pool());
      const auto append_result = builder->AppendNulls(batch->num_rows());
      TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
      array = builder->Finish().ValueOrDie();
      return;
    }
    auto g = [&]<concrete_type Type>(const Type& inferred_type) {
      auto builder
        = inferred_type.make_arrow_builder(arrow::default_memory_pool());
      for (int i = 0; i < batch->num_rows(); ++i) {
        const auto append_result
          = append_builder(inferred_type, *builder,
                           make_view(caf::get<type_to_data_t<Type>>(value)));
        TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
      }
      array = builder->Finish().ValueOrDie();
    };
    caf::visit(g, inferred_type);
  };
  // Helper function that binds an existing array.
  auto bind_array = [&](const offset& index) {
    inferred_type = layout.field(index).type;
    array = index.get(*batch);
  };
  auto f = detail::overload{
    [&](const data& value) {
      bind_value(value);
    },
    [&](const field_extractor& ex) {
      if (auto index = slice.schema().resolve_key_or_concept_once(ex.field)) {
        bind_array(*index);
        return;
      }
      bind_value({});
    },
    [&](const type_extractor& ex) {
      for (const auto& [field, index] : layout.leaves()) {
        bool match = field.type == ex.type;
        if (not match) {
          for (auto name : field.type.names()) {
            if (name == ex.type.name()) {
              match = true;
              break;
            }
          }
        }
        if (match) {
          bind_array(index);
          return;
        }
      }
      bind_value({});
    },
    [&](const meta_extractor& ex) {
      bind_value(resolve_meta_extractor(slice, ex));
    },
    [&](const data_extractor& ex) {
      bind_array(layout.resolve_flat_index(ex.column));
    },
  };
  caf::visit(f, op);
  return {
    std::move(inferred_type),
    std::move(array),
  };
}

namespace {

/// Combines offsets such that starting with the first array the list offsets
/// are replaced with the values of the next list offsets array, repeated until
/// all list offsets arrays have been combined into one. This allows for
/// flattening lists in Arrow's data model.
auto combine_offsets(
  const std::vector<std::shared_ptr<arrow::Array>>& list_offsets)
  -> std::shared_ptr<arrow::Array> {
  TENZIR_ASSERT(not list_offsets.empty());
  auto it = list_offsets.begin();
  auto result = *it++;
  auto builder = arrow::Int32Builder{};
  for (; it < list_offsets.end(); ++it) {
    for (const auto& index : static_cast<const arrow::Int32Array&>(*result)) {
      TENZIR_ASSERT(index);
      const auto& next = static_cast<const arrow::Int32Array&>(**it);
      TENZIR_ASSERT(not next.IsNull(*index));
      auto append_result = builder.Append(next.Value(*index));
      TENZIR_ASSERT(append_result.ok(), append_result.ToString().c_str());
    }
    result = builder.Finish().ValueOrDie();
  }
  return result;
}

auto make_flatten_transformation(
  std::string_view separator, const std::string& name_prefix,
  std::vector<std::shared_ptr<arrow::Array>> list_offsets)
  -> indexed_transformation::function_type;

auto flatten_record(
  std::string_view separator, std::string_view name_prefix,
  const std::vector<std::shared_ptr<arrow::Array>>& list_offsets,
  struct record_type::field field, const std::shared_ptr<arrow::Array>& array)
  -> indexed_transformation::result_type;

auto flatten_list(std::string_view separator, std::string_view name_prefix,
                  std::vector<std::shared_ptr<arrow::Array>> list_offsets,
                  struct record_type::field field,
                  const std::shared_ptr<arrow::Array>& array)
  -> indexed_transformation::result_type {
  const auto& lt = caf::get<list_type>(field.type);
  auto list_array
    = std::static_pointer_cast<type_to_arrow_array_t<list_type>>(array);
  list_offsets.push_back(list_array->offsets());
  auto f = detail::overload{
    [&]<concrete_type Type>(
      const Type&) -> indexed_transformation::result_type {
      auto result = indexed_transformation::result_type{};
      if (list_offsets.empty()) {
        result.push_back({
          {
            fmt::format("{}{}", name_prefix, field.name),
            field.type,
          },
          array,
        });
      } else {
        auto combined_offsets = combine_offsets(list_offsets);
        result.push_back({
          {
            fmt::format("{}{}", name_prefix, field.name),
            field.type,
          },
          arrow::ListArray::FromArrays(*combined_offsets, *list_array->values())
            .ValueOrDie(),
        });
      }
      return result;
    },
    [&](const list_type& lt) -> indexed_transformation::result_type {
      return flatten_list(separator, name_prefix, std::move(list_offsets),
                          {field.name, lt}, list_array->values());
    },
    [&](const record_type& rt) -> indexed_transformation::result_type {
      return flatten_record(separator, name_prefix, list_offsets,
                            {field.name, rt}, list_array->values());
    },
  };
  return caf::visit(f, lt.value_type());
}

auto flatten_record(
  std::string_view separator, std::string_view name_prefix,
  const std::vector<std::shared_ptr<arrow::Array>>& list_offsets,
  struct record_type::field field, const std::shared_ptr<arrow::Array>& array)
  -> indexed_transformation::result_type {
  const auto& rt = caf::get<record_type>(field.type);
  auto struct_array
    = std::static_pointer_cast<type_to_arrow_array_t<record_type>>(array);
  const auto next_name_prefix
    = fmt::format("{}{}{}", name_prefix, field.name, separator);
  auto transformations = std::vector<indexed_transformation>{};
  for (size_t i = 0; i < rt.num_fields(); ++i) {
    transformations.push_back(
      {offset{i},
       make_flatten_transformation(separator, next_name_prefix, list_offsets)});
  }
  auto [output_type, output_struct_array]
    = transform_columns(field.type, struct_array, transformations);
  TENZIR_ASSERT(output_type);
  TENZIR_ASSERT(output_struct_array);
  auto result = indexed_transformation::result_type{};
  result.reserve(output_struct_array->num_fields());
  const auto& output_rt = caf::get<record_type>(output_type);
  for (int i = 0; i < output_struct_array->num_fields(); ++i) {
    const auto field_view = output_rt.field(i);
    result.push_back({
      {std::string{field_view.name}, field_view.type},
      output_struct_array->field(i),
    });
  }
  return result;
}

auto make_flatten_transformation(
  std::string_view separator, const std::string& name_prefix,
  std::vector<std::shared_ptr<arrow::Array>> list_offsets)
  -> indexed_transformation::function_type {
  return [=](struct record_type::field field,
             std::shared_ptr<arrow::Array> array)
           -> indexed_transformation::result_type {
    auto f = detail::overload{
      [&]<concrete_type Type>(
        const Type&) -> indexed_transformation::result_type {
        // Return unchanged, but use prefix, and wrap in a list if we need to.
        if (list_offsets.empty()) {
          return {
            {
              {
                fmt::format("{}{}", name_prefix, field.name),
                field.type,
              },
              array,
            },
          };
        }
        // FIXME: This transformation changes the length.
        auto combined_offsets = combine_offsets(list_offsets);
        return {
          {
            {
              fmt::format("{}{}", name_prefix, field.name),
              type{list_type{field.type}},
            },
            arrow::ListArray::FromArrays(*combined_offsets, *array).ValueOrDie(),
          },
        };
      },
      [&](const list_type&) -> indexed_transformation::result_type {
        return flatten_list(separator, name_prefix, list_offsets,
                            std::move(field), array);
      },
      [&](const record_type&) -> indexed_transformation::result_type {
        return flatten_record(separator, name_prefix, list_offsets,
                              std::move(field), array);
      },
    };
    auto result = caf::visit(f, field.type);
    return result;
  };
}

auto make_rename_transformation(std::string new_name)
  -> indexed_transformation::function_type {
  return [new_name = std::move(new_name)](struct record_type::field field,
                                          std::shared_ptr<arrow::Array> array)
           -> std::vector<std::pair<struct record_type::field,
                                    std::shared_ptr<arrow::Array>>> {
    return {
      {
        {
          new_name,
          field.type,
        },
        array,
      },
    };
  };
}

} // namespace

auto flatten(table_slice slice, std::string_view separator) -> flatten_result {
  if (slice.rows() == 0 or slice.columns() == 0) {
    return {std::move(slice), {}};
  }
  // We cannot use arrow::StructArray::Flatten here because that does not
  // work recursively, see apache/arrow#20683. Hence, we roll our own version
  // here.
  auto renamed_fields = std::vector<std::string>{};
  auto transformations = std::vector<indexed_transformation>{};
  auto num_fields = caf::get<record_type>(slice.schema()).num_fields();
  transformations.reserve(num_fields);
  for (size_t i = 0; i < num_fields; ++i) {
    transformations.push_back(
      {offset{i}, make_flatten_transformation(separator, "", {})});
  }
  slice = transform_columns(slice, transformations);
  // Flattening cannot fail.
  TENZIR_ASSERT(slice.rows() > 0);
  // The slice may contain duplicate field name here, so we perform an
  // additional transformation to rename them in case we detect any.
  transformations.clear();
  const auto& layout = caf::get<record_type>(slice.schema());
  TENZIR_ASSERT_EXPENSIVE(layout.num_fields() == layout.num_leaves());
  for (const auto& leaf : layout.leaves()) {
    size_t num_occurences = 0;
    if (std::any_of(transformations.begin(), transformations.end(),
                    [&](const auto& t) {
                      return t.index == leaf.index;
                    }))
      continue;
    for (const auto& index : layout.resolve_key_suffix(leaf.field.name)) {
      // For historical reasons, resolve_key_suffix also suffix matches for dots
      // within a field name. That's pretty stupid, and it can lead to wrong
      // conflicts being detected here after flattening, so we check again if
      // the full name actually clashes.
      if (leaf.field.name != layout.field(index).name) {
        continue;
      }
      if (index <= leaf.index) {
        continue;
      }
      while (true) {
        ++num_occurences;
        if (num_occurences > 0) {
          auto new_name = fmt::format("{}_{}", leaf.field.name, num_occurences);
          if (layout.resolve_key(new_name).has_value()) {
            // The new field name also already exists, so we just continue
            // incrementing until we find a non-conflicting name.
            continue;
          }
          renamed_fields.push_back(
            fmt::format("{} -> {}", leaf.field.name, new_name));
          transformations.push_back(
            {index, make_rename_transformation(std::move(new_name))});
        }
        break;
      }
    }
  }
  TENZIR_ASSERT_EXPENSIVE(
    std::is_sorted(transformations.begin(), transformations.end()));
  slice = transform_columns(slice, transformations);
  // Renaming cannot fail.
  TENZIR_ASSERT(slice.rows() > 0);
  return {
    std::move(slice),
    std::move(renamed_fields),
  };
}

namespace {

struct unflatten_entry;

/// Basically a mutable `arrow::StructArray`.
struct unflatten_record {
  unflatten_record(int64_t length, std::shared_ptr<arrow::Buffer> null_bitmap);

  int64_t length;
  std::shared_ptr<arrow::Buffer> null_bitmap;
  detail::stable_map<std::string_view, unflatten_entry> fields;
};

struct unflatten_entry
  : variant<std::shared_ptr<arrow::Array>, unflatten_record> {
  using variant::variant;
};

unflatten_record::unflatten_record(int64_t length,
                                   std::shared_ptr<arrow::Buffer> null_bitmap)
  : length{length}, null_bitmap{std::move(null_bitmap)} {
}

auto realize(unflatten_record&& record) -> std::shared_ptr<arrow::StructArray>;

auto realize(unflatten_entry&& entry) -> std::shared_ptr<arrow::Array> {
  return entry.match<std::shared_ptr<arrow::Array>>(
    [](std::shared_ptr<arrow::Array>& array) {
      TENZIR_ASSERT(array);
      return std::move(array);
    },
    [](unflatten_record& record) {
      return realize(std::move(record));
    });
}

auto realize(unflatten_record&& record) -> std::shared_ptr<arrow::StructArray> {
  auto names = std::vector<std::string>{};
  auto arrays = std::vector<std::shared_ptr<arrow::Array>>{};
  for (auto& [name, entry] : record.fields) {
    names.emplace_back(name);
    arrays.push_back(realize(std::move(entry)));
  }
  return make_struct_array(record.length, record.null_bitmap, std::move(names),
                           arrays);
}

auto bitmap_or(std::shared_ptr<arrow::Buffer> x,
               std::shared_ptr<arrow::Buffer> y)
  -> std::shared_ptr<arrow::Buffer> {
  if (not x || not y) {
    return nullptr;
  }
  auto size = std::min(x->size(), y->size());
  auto z = check(arrow::AllocateBuffer(size));
  auto x_ptr = x->data();
  auto y_ptr = y->data();
  auto z_ptr = z->mutable_data();
  for (auto i = int64_t{0}; i < size; ++i) {
    z_ptr[i] = x_ptr[i] | y_ptr[i]; // NOLINT
  }
  return z;
}

void unflatten_into(unflatten_entry& entry, std::shared_ptr<arrow::Array> array,
                    std::string_view sep);

void unflatten_into(unflatten_entry& root, const arrow::StructArray& array,
                    std::string_view sep) {
  if (not std::holds_alternative<unflatten_record>(root)) {
    root.emplace<unflatten_record>(array.length(), array.null_bitmap());
  }
  auto names = array.struct_type()->fields()
               | std::views::transform(&arrow::Field::name);
  // We need to flatten the null bitmap here because it can happen that the
  // fields are saved to a record that is made non-null by another entry.
  auto fields = check(array.Flatten());
  for (auto [name, data] : detail::zip_equal(names, fields)) {
    auto segments
      = name | std::views::split(sep)
        | std::views::transform([](auto subrange) {
            return std::string_view{subrange.data(), subrange.size()};
          });
    auto current = &root;
    auto handle_segment = [&](std::string_view segment) {
      auto record = std::get_if<unflatten_record>(current);
      if (record) {
        record->null_bitmap
          = bitmap_or(record->null_bitmap, array.null_bitmap());
      } else {
        record = &current->emplace<unflatten_record>(array.length(),
                                                     array.null_bitmap());
      }
      current = &record->fields[segment];
    };
    // We have to work around the fact that `std::views::split` does not yield
    // anything if `name` is empty.
    if (segments.empty()) {
      handle_segment("");
    } else {
      for (auto segment : segments) {
        handle_segment(segment);
      }
    }
    unflatten_into(*current, data, sep);
  }
}

auto unflatten(const arrow::ListArray& array, std::string_view sep)
  -> std::shared_ptr<arrow::ListArray>;

void unflatten_into(unflatten_entry& entry, std::shared_ptr<arrow::Array> array,
                    std::string_view sep) {
  if (auto record = caf::get_if<arrow::StructArray>(&*array)) {
    unflatten_into(entry, *record, sep);
  } else if (auto list = caf::get_if<arrow::ListArray>(&*array)) {
    entry = unflatten(*list, sep);
  } else {
    entry = std::move(array);
  }
}

auto unflatten(std::shared_ptr<arrow::Array> array, std::string_view sep)
  -> std::shared_ptr<arrow::Array>;

auto unflatten(const arrow::StructArray& array, std::string_view sep)
  -> std::shared_ptr<arrow::StructArray> {
  // We unflatten records by recursively building up an `unflatten_record`,
  // which is basically a mutable `arrow::StructArray`.
  auto root = unflatten_entry{unflatten_record{array.length(), nullptr}};
  unflatten_into(root, array, sep);
  auto record = std::get_if<unflatten_record>(&root);
  TENZIR_ASSERT(record);
  return realize(std::move(*record));
}

auto unflatten(const arrow::ListArray& array, std::string_view sep)
  -> std::shared_ptr<arrow::ListArray> {
  // Unflattening a list simply means unflattening its values.
  auto values = unflatten(array.values(), sep);
  return check(arrow::ListArray::FromArrays(
    *array.offsets(), *values, arrow::default_memory_pool(),
    array.null_bitmap(), array.data()->null_count));
}

auto unflatten(std::shared_ptr<arrow::Array> array, std::string_view sep)
  -> std::shared_ptr<arrow::Array> {
  // We only unflatten records, but records can be contained in lists.
  if (auto record = caf::get_if<arrow::StructArray>(&*array)) {
    return unflatten(*record, sep);
  }
  if (auto list = caf::get_if<arrow::ListArray>(&*array)) {
    return unflatten(*list, sep);
  }
  return array;
}

} // namespace

auto unflatten(const table_slice& slice, std::string_view sep) -> table_slice {
  if (slice.rows() == 0) {
    return slice;
  }
  auto array = check(to_record_batch(slice)->ToStructArray());
  auto result = unflatten(array, sep);
  auto cast = std::dynamic_pointer_cast<arrow::StructArray>(std::move(result));
  TENZIR_ASSERT(cast);
  auto schema = type{slice.schema().name(), type::from_arrow(*cast->type())};
  auto batch = arrow::RecordBatch::Make(schema.to_arrow_schema(),
                                        cast->length(), cast->fields());
  auto out = table_slice{batch, std::move(schema)};
  out.import_time(slice.import_time());
  out.offset(slice.offset());
  return out;
}

} // namespace tenzir
