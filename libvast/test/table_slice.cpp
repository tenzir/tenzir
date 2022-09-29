//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE table_slice

#include "vast/table_slice.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/project.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/table_slice_row.hpp"
#include "vast/test/fixtures/table_slices.hpp"
#include "vast/test/test.hpp"

#include <caf/make_copy_on_write.hpp>
#include <caf/test/dsl.hpp>

#include <chrono>

using namespace vast;
using namespace std::string_literals;

namespace {

class fixture : public fixtures::table_slices {
public:
  fixture() : fixtures::table_slices(VAST_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(table_slice_tests, fixture)

TEST(random integer slices) {
  auto t = type{integer_type{}, {{"default", "uniform(100,200)"}}};
  auto layout = type{
    "test.integers",
    record_type{
      {"i", t},
    },
  };
  auto slices = unbox(make_random_table_slices(10, 10, layout));
  CHECK_EQUAL(slices.size(), 10u);
  CHECK(std::all_of(slices.begin(), slices.end(), [](auto& slice) {
    return slice.rows() == 10;
  }));
  std::vector<integer> values;
  for (auto& slice : slices)
    for (size_t row = 0; row < slice.rows(); ++row)
      values.emplace_back(get<integer>(slice.at(row, 0, t)));
  auto [lowest, highest] = std::minmax_element(values.begin(), values.end());
  CHECK_GREATER_EQUAL(*lowest, integer{100});
  CHECK_LESS_EQUAL(*highest, integer{200});
}

TEST(column view) {
  auto sut = zeek_conn_log[0];
  auto flat_layout = flatten(caf::get<record_type>(sut.layout()));
  auto ts_index = flat_layout.resolve_key("ts");
  REQUIRE(ts_index);
  auto ts_cview = table_slice_column{sut, flat_layout.flat_index(*ts_index)};
  CHECK_EQUAL(ts_cview.index(), 0u);
  for (size_t column = 0; column < sut.columns(); ++column) {
    auto cview = table_slice_column{sut, column};
    REQUIRE_NOT_EQUAL(cview.size(), 0u);
    CHECK_EQUAL(cview.index(), column);
    CHECK_EQUAL(cview.size(), sut.rows());
    for (size_t row = 0; row < cview.size(); ++row)
      CHECK_EQUAL(cview[row],
                  sut.at(row, column, flat_layout.field(column).type));
  }
}

TEST(row view) {
  auto sut = zeek_conn_log[0];
  auto flat_layout = flatten(caf::get<record_type>(sut.layout()));
  for (size_t row = 0; row < sut.rows(); ++row) {
    auto rview = table_slice_row{sut, row};
    REQUIRE_NOT_EQUAL(rview.size(), 0u);
    CHECK_EQUAL(rview.index(), row);
    CHECK_EQUAL(rview.size(), sut.columns());
    for (size_t column = 0; column < rview.size(); ++column)
      CHECK_EQUAL(rview[column],
                  sut.at(row, column, flat_layout.field(column).type));
  }
}

TEST(select - import time) {
  auto sut
    = table_slice{chunk::copy(zeek_conn_log_full[0]), table_slice::verify::yes};
  sut.offset(100);
  auto time = vast::time{std::chrono::milliseconds(202202141214)};
  sut.import_time(time);
  auto result = select(sut, make_ids({{110, 120}, {170, 180}}));
  REQUIRE_EQUAL(result.size(), 2u);
  CHECK_EQUAL(result[0].import_time(), time);
  CHECK_EQUAL(result[1].import_time(), time);
}

TEST(select all) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{100, 200}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0], sut);
}

TEST(select none) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{200, 300}}));
  CHECK_EQUAL(xs.size(), 0u);
}

TEST(select prefix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{0, 150}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(make_data(xs[0]), make_data(sut, 0, 50));
}

TEST(select off by one prefix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{101, 151}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(make_data(xs[0]), make_data(sut, 1, 50));
}

TEST(select intermediates) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{110, 120}, {170, 180}}));
  REQUIRE_EQUAL(xs.size(), 2u);
  CHECK_EQUAL(xs[0].rows(), 10u);
  CHECK_EQUAL(make_data(xs[0]), make_data(sut, 10, 10));
  CHECK_EQUAL(xs[1].rows(), 10u);
  CHECK_EQUAL(make_data(xs[1]), make_data(sut, 70, 10));
}

TEST(select off by one suffix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{149, 199}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(make_data(xs[0]), make_data(sut, 49, 50));
}

TEST(select suffix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = select(sut, make_ids({{150, 300}}));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(make_data(xs[0]), make_data(sut, 50, 50));
}

TEST(truncate) {
  auto sut = zeek_conn_log[0];
  REQUIRE_EQUAL(sut.rows(), 8u);
  sut.offset(100);
  auto truncated_events = [&](size_t num_rows) {
    auto sub_slice = truncate(sut, num_rows);
    if (sub_slice.rows() != num_rows)
      FAIL("expected " << num_rows << " rows, got " << sub_slice.rows());
    return make_data(sub_slice);
  };
  auto sub_slice = truncate(sut, 8);
  CHECK_EQUAL(sub_slice, sut);
  CHECK_EQUAL(truncated_events(7), make_data(sut, 0, 7));
  CHECK_EQUAL(truncated_events(6), make_data(sut, 0, 6));
  CHECK_EQUAL(truncated_events(5), make_data(sut, 0, 5));
  CHECK_EQUAL(truncated_events(4), make_data(sut, 0, 4));
  CHECK_EQUAL(truncated_events(3), make_data(sut, 0, 3));
  CHECK_EQUAL(truncated_events(2), make_data(sut, 0, 2));
  CHECK_EQUAL(truncated_events(1), make_data(sut, 0, 1));
}

TEST(split) {
  auto sut = zeek_conn_log[0];
  REQUIRE_EQUAL(sut.rows(), 8u);
  sut.offset(100);
  // Splits `sut` using make_data.
  auto manual_split_sut = [&](size_t parition_point) {
    return std::pair{make_data(sut, 0, parition_point),
                     make_data(sut, parition_point)};
  };
  // Splits `sut` using split() and then converting to events.
  auto split_sut = [&](size_t parition_point) {
    auto [first, second] = split(sut, parition_point);
    CHECK(!first.is_serialized());
    CHECK(!second.is_serialized());
    if (first.rows() + second.rows() != 8)
      FAIL("expected 8 rows in total, got " << (first.rows() + second.rows()));
    return std::pair{make_data(first), make_data(second)};
  };
  // We compare the results of the two lambdas, meaning that it should make no
  // difference whether we split via `make_data` or `split`.
  CHECK_EQUAL(split_sut(1), manual_split_sut(1));
  CHECK_EQUAL(split_sut(2), manual_split_sut(2));
  CHECK_EQUAL(split_sut(3), manual_split_sut(3));
  CHECK_EQUAL(split_sut(4), manual_split_sut(4));
  CHECK_EQUAL(split_sut(5), manual_split_sut(5));
  CHECK_EQUAL(split_sut(6), manual_split_sut(6));
  CHECK_EQUAL(split_sut(7), manual_split_sut(7));
}

TEST(filter - import time) {
  auto sut
    = table_slice{chunk::copy(zeek_conn_log[0]), table_slice::verify::yes};
  auto time = vast::time{std::chrono::milliseconds(202202141214)};
  sut.import_time(time);
  auto exp = unbox(
    tailor(unbox(to<expression>("id.orig_h != 192.168.1.102")), sut.layout()));
  auto result = filter(sut, exp);
  REQUIRE(result);
  CHECK_EQUAL(result->import_time(), time);
}

TEST(filter - expression overload) {
  auto sut = zeek_conn_log[0];
  // sut.offset(0);
  auto check_eval = [&](std::string_view expr, size_t x) {
    auto exp = unbox(tailor(unbox(to<expression>(expr)), sut.layout()));
    CHECK_EQUAL(filter(sut, exp)->rows(), x);
  };
  check_eval("id.orig_h != 192.168.1.102", 5);
}

TEST(filter - hints only) {
  auto sut = zeek_conn_log[0];
  // sut.offset(0);
  auto check_eval = [&](std::initializer_list<id_range> id_init, size_t x) {
    auto hints = make_ids(id_init, sut.offset() + sut.rows());
    CHECK_EQUAL(filter(sut, hints)->rows(), x);
  };
  check_eval({{2, 7}}, 5);
}

TEST(filter - expression with hints) {
  auto sut = zeek_conn_log[0];
  // sut.offset(0);
  auto check_eval = [&](std::string_view expr,
                        std::initializer_list<id_range> id_init, size_t x) {
    auto exp = unbox(tailor(unbox(to<expression>(expr)), sut.layout()));
    auto hints = make_ids(id_init, sut.offset() + sut.rows());
    CHECK_EQUAL(filter(sut, exp, hints)->rows(), x);
  };
  check_eval("id.orig_h != 192.168.1.102", {{0, 8}}, 5);
}

TEST(evaluate) {
  auto sut = zeek_conn_log[0];
  sut.offset(0);
  auto check_eval
    = [&](std::string_view expr, std::initializer_list<id_range> id_init) {
        auto ids = make_ids(id_init, sut.offset() + sut.rows());
        auto exp = unbox(to<expression>(expr));
        CHECK_EQUAL(evaluate(exp, sut, {}), ids);
      };
  check_eval("#type == \"zeek.conn\"", {{0, 8}});
  check_eval("#type != \"zeek.conn\"", {});
  check_eval("#field == \"orig_pkts\"", {{0, 8}});
  check_eval("#field != \"orig_pkts\"", {});
}

TEST(project column flat index) {
  auto sut = truncate(zeek_conn_log[0], 3);
  auto proj = project(sut, time_type{}, 0, string_type{}, 6);
  CHECK(proj);
  CHECK(proj.begin() != proj.end());
  for (auto&& [ts, proto] : proj) {
    REQUIRE(ts);
    CHECK_GREATER_EQUAL(*ts, vast::time{});
    REQUIRE(proto);
    CHECK_EQUAL(*proto, "udp");
  }
}

TEST(project column full name) {
  auto sut = zeek_conn_log[0];
  auto proj = project(sut, time_type{}, "zeek.conn.ts", string_type{},
                      "zeek.conn.proto");
  CHECK(proj);
  CHECK(proj.begin() != proj.end());
  for (auto&& [ts, proto] : proj) {
    REQUIRE(ts);
    CHECK_GREATER_EQUAL(*ts, vast::time{});
    REQUIRE(proto);
    CHECK_EQUAL(*proto, "udp");
  }
}

TEST(project column name) {
  auto sut = zeek_conn_log[0];
  auto proj = project(sut, time_type{}, "ts", string_type{}, "proto");
  CHECK(proj);
  CHECK(proj.begin() != proj.end());
  for (auto&& [ts, proto] : proj) {
    REQUIRE(ts);
    CHECK_GREATER_EQUAL(*ts, vast::time{});
    REQUIRE(proto);
    CHECK_EQUAL(*proto, "udp");
  }
}

TEST(project column mixed access) {
  auto sut = zeek_conn_log[0];
  auto proj = project(sut, time_type{}, 0, string_type{}, "proto");
  CHECK(proj);
  CHECK(proj.begin() != proj.end());
  for (auto&& [ts, proto] : proj) {
    REQUIRE(ts);
    CHECK_GREATER_EQUAL(*ts, vast::time{});
    REQUIRE(proto);
    CHECK_EQUAL(*proto, "udp");
  }
}

TEST(project column order independence) {
  auto sut = zeek_conn_log[0];
  auto proj = project(sut, string_type{}, "proto", time_type{}, "ts");
  CHECK(proj);
  CHECK(proj.begin() != proj.end());
  for (auto&& [proto, ts] : proj) {
    REQUIRE(proto);
    CHECK_EQUAL(*proto, "udp");
    REQUIRE(ts);
    CHECK_GREATER_EQUAL(*ts, vast::time{});
  }
}

TEST(project column detect type mismatches) {
  auto sut = zeek_conn_log[0];
  auto proj = project(sut, bool_type{}, "proto", time_type{}, "ts");
  CHECK(!proj);
  CHECK(proj.begin() == proj.end());
}

TEST(project column detect wrong field names) {
  auto sut = zeek_conn_log[0];
  auto proj = project(sut, string_type{}, "porto", time_type{}, "ts");
  CHECK(!proj);
  CHECK(proj.begin() == proj.end());
}

TEST(project column detect wrong flat indices) {
  auto sut = zeek_conn_log[0];
  auto proj = project(sut, string_type{}, 123, time_type{}, "ts");
  CHECK(!proj);
  CHECK(proj.begin() == proj.end());
}

TEST(project column unspecified types) {
  auto sut = zeek_conn_log[0];
  auto proj = project(sut, type{}, "proto", time_type{}, "ts");
  CHECK(proj);
  CHECK(proj.begin() != proj.end());
  for (auto&& [proto, ts] : proj) {
    REQUIRE(caf::holds_alternative<view<std::string>>(proto));
    CHECK_EQUAL(caf::get<vast::view<std::string>>(proto), "udp");
    REQUIRE(ts);
    CHECK_GREATER_EQUAL(*ts, vast::time{});
  }
}

TEST(project column lists) {
  auto sut = zeek_dns_log[0];
  auto proj = project(sut, list_type{string_type{}}, "answers");
  CHECK(proj);
  CHECK(proj.begin() != proj.end());
  CHECK_EQUAL(proj.size(), sut.rows());
  size_t answers = 0;
  for (auto&& [answer] : proj) {
    if (answer) {
      answers++;
      for (auto entry : *answer) {
        CHECK(!caf::holds_alternative<caf::none_t>(entry));
        CHECK(caf::holds_alternative<view<std::string>>(entry));
      }
    }
  }
  CHECK_EQUAL(answers, 4u);
}

TEST(roundtrip) {
  auto slice = zeek_dns_log[0];
  slice.offset(42u);
  table_slice slice_copy;
  caf::byte_buffer buf;
  caf::binary_serializer sink{nullptr, buf};
  CHECK_EQUAL(inspect(sink, slice), true);
  CHECK_EQUAL(detail::legacy_deserialize(buf, slice_copy), true);
  CHECK_EQUAL(slice_copy.offset(), 42u);
  CHECK_EQUAL(slice, slice_copy);
}

FIXTURE_SCOPE_END()
