//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/table_slice.hpp"

#include "vast/chunk.hpp"
#include "vast/defaults.hpp"
#include "vast/detail/assert.hpp"
#include "vast/detail/overload.hpp"
#include "vast/detail/string.hpp"
#include "vast/error.hpp"
#include "vast/expression.hpp"
#include "vast/fbs/table_slice.hpp"
#include "vast/fbs/utils.hpp"
#include "vast/ids.hpp"
#include "vast/logger.hpp"
#include "vast/msgpack_table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/value_index.hpp"

#include <cstddef>

#if VAST_ENABLE_ARROW
#  include "vast/arrow_table_slice.hpp"
#endif // VAST_ENABLE_ARROW

namespace vast {

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
    // NOTE: GCC does not quite respect the C++ standard and instantiates
    // 'Visitor' with the encoded table, which is why we need to take extra care
    // of builds without VAST_ENABLE_ARROW here.
    std::is_nothrow_invocable<Visitor>,
#if VAST_ENABLE_ARROW
    std::is_nothrow_invocable<Visitor, const fbs::table_slice::arrow::v0&>,
#endif // VAST_ENABLE_ARROW
    std::is_nothrow_invocable<Visitor, const fbs::table_slice::msgpack::v0&>>) {
  if (!x)
    return std::invoke(std::forward<Visitor>(visitor));
  switch (x->table_slice_type()) {
    case fbs::table_slice::TableSlice::NONE:
      return std::invoke(std::forward<Visitor>(visitor));
    case fbs::table_slice::TableSlice::arrow_v0:
#if VAST_ENABLE_ARROW
      return std::invoke(std::forward<Visitor>(visitor),
                         *x->table_slice_as_arrow_v0());
#else
      static std::once_flag flag;
      std::call_once(flag, [] {
        VAST_ERROR("database contains Arrow-encoded table slice but "
                   "this version of VAST does not support Apache "
                   "Arrow; "
                   "data may be missing from exports");
      });
      return std::invoke(std::forward<Visitor>(visitor));
#endif
    case fbs::table_slice::TableSlice::msgpack_v0:
      return std::invoke(std::forward<Visitor>(visitor),
                         *x->table_slice_as_msgpack_v0());
  }
  // GCC-8 fails to recognize that this can never be reached, so we just call a
  // [[noreturn]] function.
  die("unhandled table slice encoding");
}

/// Get a pointer to the `vast.fbs.TableSlice` inside the chunk.
/// @param chunk The chunk to look at.
const fbs::TableSlice* as_flatbuffer(const chunk_ptr& chunk) noexcept {
  if (!chunk)
    return nullptr;
  return fbs::GetTableSlice(chunk->data());
}

/// Verifies that the chunk contains a valid `vast.fbs.TableSlice` FlatBuffers
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

/// A helper utility for converting table slice encoding to the corresponding
/// builder id.
/// @param encoding The table slice encoding to map.
table_slice_encoding builder_id(enum table_slice_encoding encoding) {
  return encoding;
}

/// A helper utility for accessing the state of a table slice.
/// @param encoded The encoding-specific FlatBuffers table.
/// @param state The encoding-specific runtime state of the table slice.
template <class Slice, class State>
constexpr auto&
state([[maybe_unused]] Slice&& encoded, State&& state) noexcept {
  using slice_type = std::decay_t<Slice>;
  if constexpr (std::is_same_v<slice_type, fbs::table_slice::arrow::v0>) {
    return std::forward<State>(state).arrow_v0;
  } else if constexpr (std::is_same_v<slice_type,
                                      fbs::table_slice::msgpack::v0>) {
    return std::forward<State>(state).msgpack_v0;
  } else {
    static_assert(detail::always_false_v<slice_type>, "cannot access table "
                                                      "slice state");
  }
}

} // namespace

// -- constructors, destructors, and assignment operators ----------------------

table_slice::table_slice() noexcept = default;

table_slice::table_slice(chunk_ptr&& chunk, enum verify verify) noexcept
  : chunk_{verified_or_none(std::move(chunk), verify)} {
  if (chunk_ && chunk_->unique()) {
    ++num_instances_;
    auto f = detail::overload{
      []() noexcept { die("invalid table slice encoding"); },
      [&](const auto& encoded) noexcept {
        auto& state_ptr = state(encoded, state_);
        auto state
          = std::make_unique<std::decay_t<decltype(*state_ptr)>>(encoded);
        state_ptr = state.get();
        chunk_->add_deletion_step(
          [state = std::move(state)]() noexcept { --num_instances_; });
      },
    };
    visit(f, as_flatbuffer(chunk_));
  }
}

table_slice::table_slice(chunk_ptr&& chunk, enum verify verify,
                         record_type layout) noexcept
  : chunk_{verified_or_none(std::move(chunk), verify)} {
  if (chunk_ && chunk_->unique()) {
    ++num_instances_;
    auto f = detail::overload{
      []() noexcept { die("invalid table slice encoding"); },
      [&](const auto& encoded) noexcept {
        auto& state_ptr = state(encoded, state_);
        auto state = std::make_unique<std::decay_t<decltype(*state_ptr)>>(
          encoded, std::move(layout));
        state_ptr = state.get();
        chunk_->add_deletion_step(
          [state = std::move(state)]() noexcept { --num_instances_; });
      },
    };
    visit(f, as_flatbuffer(chunk_));
  }
}

table_slice::table_slice(const fbs::FlatTableSlice& flat_slice,
                         const chunk_ptr& parent_chunk,
                         enum verify verify) noexcept {
  const auto* const flat_slice_begin
    = reinterpret_cast<const std::byte*>(flat_slice.data()->data());
  const auto flat_slice_size = flat_slice.data()->size();
  VAST_ASSERT(flat_slice_begin >= parent_chunk->data());
  VAST_ASSERT(std::next(flat_slice_begin, flat_slice_size)
              <= std::next(parent_chunk->data(), parent_chunk->size()));
  auto chunk = parent_chunk->slice(flat_slice_begin - parent_chunk->data(),
                                   flat_slice_size);
  // Delegate the sliced chunk to the constructor.
  *this = table_slice{std::move(chunk), verify};
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

// -- operators ----------------------------------------------------------------

// TODO: Dispatch to optimized implementations if the encodings are the same.
bool operator==(const table_slice& lhs, const table_slice& rhs) noexcept {
  // Check whether the slices point to the same chunk of data.
  if (lhs.chunk_ == rhs.chunk_)
    return true;
  // Check whether the slices have different sizes or layouts.
  if (lhs.rows() != rhs.rows() || lhs.columns() != rhs.columns()
      || lhs.layout() != rhs.layout())
    return false;
  // Check whether the slices contain different data.
  auto flat_layout = flatten(lhs.layout());
  for (size_t row = 0; row < lhs.rows(); ++row)
    for (size_t col = 0; col < flat_layout.fields.size(); ++col)
      if (lhs.at(row, col, flat_layout.fields[col].type)
          != rhs.at(row, col, flat_layout.fields[col].type))
        return false;
  return true;
}

bool operator!=(const table_slice& lhs, const table_slice& rhs) noexcept {
  return !(lhs == rhs);
}

// -- properties ---------------------------------------------------------------

enum table_slice_encoding table_slice::encoding() const noexcept {
  auto f = detail::overload{
    []() noexcept { return table_slice_encoding::none; },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->encoding;
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

const record_type& table_slice::layout() const noexcept {
  auto f = detail::overload{
    []() noexcept {
      static const auto empty_layout = record_type{};
      return &empty_layout;
    },
    [&](const auto& encoded) noexcept {
      return &state(encoded, state_)->layout();
    },
  };
  return *visit(f, as_flatbuffer(chunk_));
}

table_slice::size_type table_slice::rows() const noexcept {
  auto f = detail::overload{
    []() noexcept { return size_type{}; },
    [&](const auto& encoded) noexcept {
      return state(encoded, state_)->rows();
    },
  };
  return visit(f, as_flatbuffer(chunk_));
}

table_slice::size_type table_slice::columns() const noexcept {
  auto f = detail::overload{
    []() noexcept { return size_type{}; },
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

int table_slice::instances() noexcept {
  return num_instances_;
}

// -- data access --------------------------------------------------------------

void table_slice::append_column_to_index(table_slice::size_type column,
                                         value_index& index) const {
  VAST_ASSERT(offset() != invalid_id);
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
  VAST_ASSERT(row < rows());
  VAST_ASSERT(column < columns());
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
  if (const auto* alias = caf::get_if<alias_type>(&t))
    return at(row, column, alias->value_type);
  VAST_ASSERT(row < rows());
  VAST_ASSERT(column < columns());
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

#if VAST_ENABLE_ARROW

std::shared_ptr<arrow::RecordBatch> as_record_batch(const table_slice& slice) {
  auto f = detail::overload{
    []() noexcept -> std::shared_ptr<arrow::RecordBatch> {
      die("cannot access record batch of invalid table slice");
    },
    [&](const auto& encoded) noexcept -> std::shared_ptr<arrow::RecordBatch> {
      // The following does not work on all compilers, hence the ugly
      // decay+decltype workaround:
      //   if constexpr (state(encoding, slice.state_)->encoding
      //                 == table_slice_encoding::arrow) { ... }
      if constexpr (std::decay_t<decltype(*state(encoded, slice.state_))>::encoding
                    == table_slice_encoding::arrow) {
        // Get the record batch first, then create a copy that shares the
        // lifetime with the chunk and the original record batch. Capturing the
        // chunk guarantees that the table slice is valid as long as the
        // returned record batch is valid, and capturing the batch ensures that
        // guarantee for the underlying Arrow Buffer object.
        auto batch = state(encoded, slice.state_)->record_batch();
        const auto data = batch.get();
        auto result = std::shared_ptr<arrow::RecordBatch>{
          data,
          [batch = std::move(batch), slice](arrow::RecordBatch*) noexcept {
            static_cast<void>(batch);
            static_cast<void>(slice);
          }};
        return result;
      } else {
        // Rebuild the slice as an Arrow-encoded table slice.
        auto copy = rebuild(slice, table_slice_encoding::arrow);
        return as_record_batch(copy);
      }
    },
  };
  return visit(f, as_flatbuffer(slice.chunk_));
}

#endif // VAST_ENABLE_ARROW

// -- concepts -----------------------------------------------------------------

span<const std::byte> as_bytes(const table_slice& slice) noexcept {
  return as_bytes(slice.chunk_);
}

// -- operations ---------------------------------------------------------------

table_slice
rebuild(table_slice slice, enum table_slice_encoding encoding) noexcept {
  auto f = detail::overload{
    [&]() noexcept -> table_slice { return {}; },
    [&](const auto& encoded) noexcept -> table_slice {
      if (encoding == state(encoded, slice.state_)->encoding
          && state(encoded, slice.state_)->is_latest_version)
        return std::move(slice);
      auto builder = factory<table_slice_builder>::make(builder_id(encoding),
                                                        slice.layout());
      if (!builder)
        return table_slice{};
      auto flat_layout = flatten(slice.layout());
      for (table_slice::size_type row = 0; row < slice.rows(); ++row)
        for (table_slice::size_type column = 0;
             column < flat_layout.fields.size(); ++column)
          if (!builder->add(
                slice.at(row, column, flat_layout.fields[column].type)))
            return {};
      auto result = builder->finish();
      result.offset(slice.offset());
      return result;
    },
  };
  return visit(f, as_flatbuffer(slice.chunk_));
}

void select(std::vector<table_slice>& result, const table_slice& slice,
            const ids& selection) {
  VAST_ASSERT(slice.encoding() != table_slice_encoding::none);
  auto xs_ids = make_ids({{slice.offset(), slice.offset() + slice.rows()}});
  auto intersection = selection & xs_ids;
  auto intersection_rank = rank(intersection);
  // Do no rows qualify?
  if (intersection_rank == 0)
    return;
  // Do all rows qualify?
  if (rank(xs_ids) == intersection_rank) {
    result.emplace_back(slice);
    return;
  }
  // Get the desired encoding, and the already serialized layout.
  auto f = detail::overload{
    []() noexcept -> std::pair<table_slice_encoding, span<const std::byte>> {
      die("cannot select from an invalid table slice");
    },
    [&](const auto& encoded) noexcept {
      return std::pair{
        builder_id(state(encoded, slice.state_)->encoding),
        span{reinterpret_cast<const std::byte*>(encoded.layout()->data()),
             encoded.layout()->size()}};
    },
  };
  table_slice_encoding implementation_id;
  span<const std::byte> serialized_layout = {};
  std::tie(implementation_id, serialized_layout)
    = visit(f, as_flatbuffer(slice.chunk_));
  // Start slicing and dicing.
  auto builder
    = factory<table_slice_builder>::make(implementation_id, slice.layout());
  if (builder == nullptr) {
    VAST_ERROR("{} failed to get a table slice builder for {}", __func__,
               implementation_id);
    return;
  }
  id last_offset = slice.offset();
  auto push_slice = [&] {
    if (builder->rows() == 0)
      return;
    auto new_slice = builder->finish(serialized_layout);
    if (new_slice.encoding() == table_slice_encoding::none) {
      VAST_WARN("{} got an empty slice", __func__);
      return;
    }
    new_slice.offset(last_offset);
    result.emplace_back(std::move(new_slice));
  };
  auto flat_layout = flatten(slice.layout());
  auto last_id = last_offset - 1;
  for (auto id : select(intersection)) {
    // Finish last slice when hitting non-consecutive IDs.
    if (last_id + 1 != id) {
      push_slice();
      last_offset = id;
      last_id = id;
    } else {
      ++last_id;
    }
    VAST_ASSERT(id >= slice.offset());
    auto row = id - slice.offset();
    VAST_ASSERT(row < slice.rows());
    for (size_t column = 0; column < flat_layout.fields.size(); ++column) {
      auto cell_value = slice.at(row, column, flat_layout.fields[column].type);
      if (!builder->add(cell_value)) {
        VAST_ERROR("{} failed to add data at column {} in row {} to the "
                   "builder: {}",
                   __func__, column, row, cell_value);
        return;
      }
    }
  }
  push_slice();
}

std::vector<table_slice>
select(const table_slice& slice, const ids& selection) {
  std::vector<table_slice> result;
  select(result, slice, selection);
  return result;
}

table_slice truncate(const table_slice& slice, size_t num_rows) {
  VAST_ASSERT(slice.encoding() != table_slice_encoding::none);
  VAST_ASSERT(num_rows > 0);
  if (slice.rows() <= num_rows)
    return slice;
  auto selection = make_ids({{slice.offset(), slice.offset() + num_rows}});
  auto xs = select(slice, selection);
  VAST_ASSERT(xs.size() == 1);
  return std::move(xs.back());
}

std::pair<table_slice, table_slice>
split(const table_slice& slice, size_t partition_point) {
  VAST_ASSERT(slice.encoding() != table_slice_encoding::none);
  if (partition_point == 0)
    return {{}, slice};
  if (partition_point >= slice.rows())
    return {slice, {}};
  auto first = slice.offset();
  auto mid = first + partition_point;
  auto last = first + slice.rows();
  // Create first table slice.
  auto xs = select(slice, make_ids({{first, mid}}));
  VAST_ASSERT(xs.size() == 1);
  // Create second table slice.
  select(xs, slice, make_ids({{mid, last}}));
  VAST_ASSERT(xs.size() == 2);
  return {std::move(xs.front()), std::move(xs.back())};
}

uint64_t rows(const std::vector<table_slice>& slices) {
  auto result = uint64_t{0};
  for (const auto& slice : slices)
    result += slice.rows();
  return result;
}

namespace {

struct row_evaluator {
  row_evaluator(const table_slice& slice, size_t row)
    : slice_{slice}, row_{row} {
    // nop
  }

  template <class T>
  bool operator()(const data& d, const T& x) {
    return (*this)(x, d);
  }

  template <class T, class U>
  bool operator()(const T&, const U&) {
    return false;
  }

  bool operator()(caf::none_t) {
    return false;
  }

  bool operator()(const conjunction& c) {
    return std::all_of(c.begin(), c.end(),
                       [&](const auto& op) { return caf::visit(*this, op); });
  }

  bool operator()(const disjunction& d) {
    return std::any_of(d.begin(), d.end(),
                       [&](const auto& op) { return caf::visit(*this, op); });
  }

  bool operator()(const negation& n) {
    return !caf::visit(*this, n.expr());
  }

  bool operator()(const predicate& p) {
    op_ = p.op;
    return caf::visit(*this, p.lhs, p.rhs);
  }

  bool operator()(const meta_extractor& e, const data& d) {
    // TODO: Transform this AST node into a constant-time lookup node (e.g.,
    // data_extractor). It's not necessary to iterate over the schema for
    // every row; this should happen upfront.
    auto&& layout = slice_.layout();
    // TODO: type and field queries don't produce false positives in the
    // partition. Is there actually any reason to do the check here?
    if (e.kind == meta_extractor::type)
      return evaluate(layout.name(), op_, d);
    if (e.kind == meta_extractor::field) {
      const auto* s = caf::get_if<std::string>(&d);
      if (!s) {
        VAST_WARN("#field can only compare with string");
        return false;
      }
      auto result = false;
      auto neg = is_negated(op_);
      // auto abs_op = neg ? negate(op_) : op_;
      for (const auto& field : record_type::each{layout}) {
        auto fqn = layout.name() + "." + field.key();
        if (detail::ends_with(fqn, *s)) {
          result = true;
          break;
        }
      }
      return neg ? !result : result;
    }
    return false;
  }

  bool operator()(const type_extractor&, const data&) {
    die("type extractor should have been resolved at this point");
  }

  bool operator()(const field_extractor&, const data&) {
    die("field extractor should have been resolved at this point");
  }

  bool operator()(const data_extractor& e, const data& d) {
    auto col = slice_.layout().flat_index_at(e.offset);
    VAST_ASSERT(col);
    auto lhs = to_canonical(e.type, slice_.at(row_, *col, e.type));
    auto rhs = make_data_view(d);
    return evaluate_view(lhs, op_, rhs);
  }

  const table_slice& slice_;
  size_t row_;
  relational_operator op_ = {};
};

} // namespace

ids evaluate(const expression& expr, const table_slice& slice) {
  // TODO: switch to a column-based evaluation strategy where it makes sense.
  ids result;
  result.append(false, slice.offset());
  for (size_t row = 0; row != slice.rows(); ++row) {
    auto x = caf::visit(row_evaluator{slice, row}, expr);
    result.append_bit(x);
  }
  return result;
}

std::optional<table_slice>
filter(const table_slice& slice, const expression& expr, const ids& hints) {
  VAST_ASSERT(slice.encoding() != table_slice_encoding::none);
  const auto offset = slice.offset();
  auto slice_ids = make_ids({{offset, offset + slice.rows()}});
  auto selection = slice_ids;
  if (!hints.empty()) {
    selection &= hints;
    if (selection.empty())
      return std::nullopt;
  }
  if (expr == expression{}) {
    if (slice_ids == selection)
      return slice;
  }
  // Get the desired encoding, and the already serialized layout.
  auto f = detail::overload{
    []() noexcept -> std::pair<table_slice_encoding, span<const std::byte>> {
      die("cannot filter an invalid table slice");
    },
    [&](const auto& encoded) noexcept {
      return std::pair{
        builder_id(state(encoded, slice.state_)->encoding),
        span{reinterpret_cast<const std::byte*>(encoded.layout()->data()),
             encoded.layout()->size()}};
    },
  };
  table_slice_encoding implementation_id;
  span<const std::byte> serialized_layout = {};
  std::tie(implementation_id, serialized_layout)
    = visit(f, as_flatbuffer(slice.chunk_));
  // Start slicing and dicing.
  auto builder
    = factory<table_slice_builder>::make(implementation_id, slice.layout());
  VAST_ASSERT(builder);
  auto flat_layout = flatten(slice.layout());
  auto check = [&](row_evaluator eval) {
    if (expr == expression{})
      return true;
    return caf::visit(eval, expr);
  };
  for (auto id : select(selection)) {
    VAST_ASSERT(id >= offset);
    auto row = id - offset;
    VAST_ASSERT(row < slice.rows());
    if (check(row_evaluator{slice, row})) {
      for (size_t column = 0; column < flat_layout.fields.size(); ++column) {
        auto cell_value
          = slice.at(row, column, flat_layout.fields[column].type);
        auto ret = builder->add(cell_value);
        VAST_ASSERT(ret);
      }
    }
  }
  if (builder->rows() == 0)
    return std::nullopt;
  auto new_slice = builder->finish(serialized_layout);
  VAST_ASSERT(new_slice.encoding() != table_slice_encoding::none);
  new_slice.offset(offset);
  return new_slice;
}

std::optional<table_slice>
filter(const table_slice& slice, const expression& expr) {
  return filter(slice, expr, ids{});
}

std::optional<table_slice> filter(const table_slice& slice, const ids& hints) {
  return filter(slice, expression{}, hints);
}

} // namespace vast
