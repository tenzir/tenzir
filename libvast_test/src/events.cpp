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

#include "fixtures/events.hpp"

#include "vast/default_table_slice_builder.hpp"
#include "vast/defaults.hpp"
#include "vast/format/bgpdump.hpp"
#include "vast/format/test.hpp"
#include "vast/format/zeek.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/table_slice_builder_factory.hpp"
#include "vast/table_slice_factory.hpp"
#include "vast/to_events.hpp"
#include "vast/type.hpp"

#include "vast/concept/printable/to_string.hpp"
#include "vast/concept/printable/vast/data.hpp"
#include "vast/concept/printable/vast/event.hpp"

// Pull in the auto-generated serialized table slices.

namespace artifacts::logs::zeek {

extern char conn_buf[];

extern size_t conn_buf_size;

} // namespace artifacts::logs::zeek

namespace fixtures {

namespace {

// 2000-01-01 (GMT), just to not use 0 here.
constexpr timestamp epoch = timestamp{timespan{946684800}};

std::vector<event> make_ascending_integers(size_t count) {
  std::vector<event> result;
  type layout{record_type{{"value", integer_type{}}}};
  layout.name("test::int");
  for (size_t i = 0; i < count; ++i) {
    result.emplace_back(event::make(vector{static_cast<integer>(i)}, layout));
    result.back().timestamp(epoch + std::chrono::seconds(i + 100));
  }
  return result;
}

std::vector<event> make_alternating_integers(size_t count) {
  std::vector<event> result;
  type layout{record_type{{"value", integer_type{}}}};
  layout.name("test::int");
  for (size_t i = 0; i < count; ++i) {
    result.emplace_back(event::make(vector{static_cast<integer>(i % 2)},
                                    layout));
    result.back().timestamp(epoch + std::chrono::seconds(i + 100));
  }
  return result;
}

/// insert item into a sorted vector
/// @precondition is_sorted(vec)
/// @postcondition is_sorted(vec)
template <typename T, typename Pred>
auto insert_sorted(std::vector<T>& vec, const T& item, Pred pred) {
  return vec.insert(std::upper_bound(vec.begin(), vec.end(), item, pred), item);
}

template <class Reader>
static std::vector<event> extract(Reader&& reader) {
  std::vector<table_slice_ptr> slices;
  auto add_slice = [&](table_slice_ptr ptr) {
    slices.emplace_back(std::move(ptr));
  };
  auto [err, produced] = reader.read(std::numeric_limits<size_t>::max(),
                                     defaults::system::table_slice_size,
                                     add_slice);
  if (err != caf::none && err != ec::end_of_input)
    FAIL("reader returned an error: " << to_string(err));
  std::vector<event> result;
  result.reserve(produced);
  for (auto& slice : slices)
    to_events(result, *slice);
  if (result.size() != produced)
    FAIL("to_events() failed fill the vector");
  return result;
}

template <class Reader>
static std::vector<event> inhale(const char* filename) {
  auto input = std::make_unique<std::ifstream>(filename);
  Reader reader{defaults::system::table_slice_type, std::move(input)};
  return extract(reader);
}

} // namespace <anonymous>

size_t events::slice_size = 8;

std::vector<event> events::zeek_conn_log;
std::vector<event> events::zeek_dns_log;
std::vector<event> events::zeek_http_log;
std::vector<event> events::bgpdump_txt;
std::vector<event> events::random;

std::vector<table_slice_ptr> events::zeek_conn_log_slices;
std::vector<table_slice_ptr> events::zeek_dns_log_slices;
std::vector<table_slice_ptr> events::zeek_http_log_slices;
std::vector<table_slice_ptr> events::bgpdump_txt_slices;
// std::vector<table_slice_ptr> events::random_slices;

std::vector<table_slice_ptr> events::zeek_full_conn_log_slices;

std::vector<event> events::ascending_integers;
std::vector<table_slice_ptr> events::ascending_integers_slices;

std::vector<event> events::alternating_integers;
std::vector<table_slice_ptr> events::alternating_integers_slices;

record_type events::zeek_conn_log_layout() {
  return zeek_conn_log_slices[0]->layout();
}

std::vector<table_slice_ptr>
events::copy(std::vector<table_slice_ptr> xs) {
  std::vector<table_slice_ptr> result;
  result.reserve(xs.size());
  for (auto& x : xs)
    result.emplace_back(x->copy());
  return result;
}

/// A wrapper around a table_slice_builder_ptr that assigns ids to each
/// added event.
class id_assigning_builder {
public:
  explicit id_assigning_builder(table_slice_builder_ptr b) : inner_{b} {
    // nop
  }

  /// Adds an event to the table slice and assigns an id.
  bool add(event& e) {
    if (!inner_->recursive_add(e.data(), e.type()))
      FAIL("builder->recursive_add() failed");
    e.id(id_++);
    return true;
  }

  auto rows() const {
    return inner_->rows();
  }

  bool start_slice(size_t offset) {
    if (rows() != 0)
      return false;
    offset_ = offset;
    id_ = offset;
    return true;
  }

  /// Finish the slice and set its offset.
  table_slice_ptr finish() {
    auto slice = inner_->finish();
    slice.unshared().offset(offset_);
    return slice;
  }

private:
  table_slice_builder_ptr inner_;
  size_t offset_ = 0;
  size_t id_ = 0;
};

/// Tries to access the builder for `layout`.
class builders {
public:
  using map_type = std::map<std::string, id_assigning_builder>;

  id_assigning_builder* get(const type& layout) {
    auto i = builders_.find(layout.name());
    if (i != builders_.end())
      return &i->second;
    return caf::visit(
      detail::overload(
        [&](const record_type& rt) -> id_assigning_builder* {
          id_assigning_builder tmp{default_table_slice_builder::make(rt)};
          return &(builders_.emplace(rt.name(), std::move(tmp)).first->second);
        },
        [&](const auto&) -> id_assigning_builder* {
          FAIL("layout is not a record type");
          return nullptr;
        }),
      layout);
  }

  map_type& all() {
    return builders_;
  }

private:
  map_type builders_;
};

events::events() {
  static bool initialized = false;
  if (initialized)
    return;
  factory<table_slice>::initialize();
  factory<table_slice_builder>::initialize();
  initialized = true;
  MESSAGE("inhaling unit test suite events");
  zeek_conn_log = inhale<format::zeek::reader>(zeek::small_conn);
  REQUIRE_EQUAL(zeek_conn_log.size(), 20u);
  zeek_dns_log = inhale<format::zeek::reader>(zeek::dns);
  REQUIRE_EQUAL(zeek_dns_log.size(), 32u);
  zeek_http_log = inhale<format::zeek::reader>(zeek::http);
  REQUIRE_EQUAL(zeek_http_log.size(), 40u);
  bgpdump_txt = inhale<format::bgpdump::reader>(bgpdump::updates20180124);
  REQUIRE_EQUAL(bgpdump_txt.size(), 100u);
  vast::format::test::reader rd{defaults::system::table_slice_type, 42, 1000};
  random = extract(rd);
  REQUIRE_EQUAL(random.size(), 1000u);
  ascending_integers = make_ascending_integers(250);
  alternating_integers = make_alternating_integers(250);
  auto allocate_id_block = [i = id{0}](size_t size) mutable {
    auto first = i;
    i += size;
    return first;
  };
  MESSAGE("building slices of " << slice_size << " events each");
  auto assign_ids_and_slice_up =
    [&](std::vector<event>& src) {
      VAST_ASSERT(src.size() > 0);
      VAST_ASSERT(caf::holds_alternative<record_type>(src[0].type()));
      std::vector<table_slice_ptr> slices;
      builders bs;
      auto finish_slice = [&](auto& builder) {
        auto pred = [](const auto& lhs, const auto& rhs) {
          return lhs->offset() < rhs->offset();
        };
        insert_sorted(slices, builder.finish(), pred);
      };
      for (auto& e : src) {
        auto bptr = bs.get(e.type());
        if (bptr->rows() == 0)
          bptr->start_slice(allocate_id_block(slice_size));
        bptr->add(e);
        if (bptr->rows() == slice_size)
          finish_slice(*bptr);
      }
      for (auto& i : bs.all()) {
        auto builder = i.second;
        if (builder.rows() > 0)
          finish_slice(builder);
      }
      return slices;
    };
  zeek_conn_log_slices = assign_ids_and_slice_up(zeek_conn_log);
  zeek_dns_log_slices = assign_ids_and_slice_up(zeek_dns_log);
  allocate_id_block(1000); // cause an artificial gap in the ID sequence
  zeek_http_log_slices = assign_ids_and_slice_up(zeek_http_log);
  bgpdump_txt_slices = assign_ids_and_slice_up(bgpdump_txt);
  // random_slices = slice_up(random);
  ascending_integers_slices = assign_ids_and_slice_up(ascending_integers);
  alternating_integers_slices = assign_ids_and_slice_up(alternating_integers);
  auto sort_by_id = [](std::vector<event>& v) {
    auto pred = [](const auto& lhs, const auto& rhs) {
      return lhs.id() < rhs.id();
    };
    std::sort(v.begin(), v.end(), pred);
  };
  auto as_events = [&](const auto& slices) {
    std::vector<event> result;
    for (auto& slice : slices) {
      auto xs = to_events(*slice);
      std::move(xs.begin(), xs.end(), std::back_inserter(result));
    }
    return result;
  };
#define SANITY_CHECK(event_vec, slice_vec)                                     \
  {                                                                            \
    auto flat_log = as_events(slice_vec);                                      \
    auto sorted_event_vec = event_vec;                                         \
    sort_by_id(sorted_event_vec);                                              \
    REQUIRE_EQUAL(sorted_event_vec.size(), flat_log.size());                   \
    for (size_t i = 0; i < sorted_event_vec.size(); ++i) {                     \
      if (sorted_event_vec[i] != flat_log[i]) {                                \
        FAIL(#event_vec << " != " << #slice_vec << "\ni: " << i << '\n'        \
                        << to_string(sorted_event_vec[i])                      \
                        << " != " << to_string(flat_log[i]));                  \
      }                                                                        \
    }                                                                          \
  }
  SANITY_CHECK(zeek_conn_log, zeek_conn_log_slices);
  SANITY_CHECK(zeek_dns_log, zeek_dns_log_slices);
  SANITY_CHECK(zeek_http_log, zeek_http_log_slices);
  SANITY_CHECK(bgpdump_txt, bgpdump_txt_slices);
  // SANITY_CHECK(random, const_random_slices);
  // Read the full Zeek conn.log.
  // TODO: port remaining slices to new deserialization API and replace
  //       this hard-coded starting offset
  id offset = 100000;
  caf::binary_deserializer src{nullptr, artifacts::logs::zeek::conn_buf,
                               artifacts::logs::zeek::conn_buf_size};
  if (auto err = src(zeek_full_conn_log_slices))
    FAIL("unable to load full Zeek conn logs from buffer");
  VAST_ASSERT(std::all_of(zeek_full_conn_log_slices.begin(),
                          zeek_full_conn_log_slices.end() - 1,
                          [](auto& slice) { return slice->rows() == 100; }));
  for (auto& ptr : zeek_full_conn_log_slices) {
    ptr.unshared().offset(offset);
    offset += ptr->rows();
  }
}

} // namespace fixtures
