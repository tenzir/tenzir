//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/table_slice.hpp"

#include "vast/arrow_table_slice.hpp"
#include "vast/cast.hpp"
#include "vast/collect.hpp"
#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/data.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/detail/legacy_deserialize.hpp"
#include "vast/expression.hpp"
#include "vast/ids.hpp"
#include "vast/project.hpp"
#include "vast/table_slice_column.hpp"
#include "vast/table_slice_row.hpp"
#include "vast/test/fixtures/table_slices.hpp"
#include "vast/test/test.hpp"

#include <arrow/record_batch.h>
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
  auto t = type{int64_type{}, {{"default", "uniform(100,200)"}}};
  auto schema = type{
    "test.integers",
    record_type{
      {"i", t},
    },
  };
  auto slices = unbox(make_random_table_slices(10, 10, schema));
  CHECK_EQUAL(slices.size(), 10u);
  CHECK(std::all_of(slices.begin(), slices.end(), [](auto& slice) {
    return slice.rows() == 10;
  }));
  std::vector<int64_t> values;
  for (auto& slice : slices)
    for (size_t row = 0; row < slice.rows(); ++row)
      values.emplace_back(get<int64_t>(slice.at(row, 0, t)));
  auto [lowest, highest] = std::minmax_element(values.begin(), values.end());
  CHECK_GREATER_EQUAL(*lowest, int64_t{100});
  CHECK_LESS_EQUAL(*highest, int64_t{200});
}

TEST(column view) {
  auto sut = zeek_conn_log[0];
  auto flat_schema = flatten(caf::get<record_type>(sut.schema()));
  auto ts_index = flat_schema.resolve_key("ts");
  REQUIRE(ts_index);
  auto ts_cview = table_slice_column{sut, flat_schema.flat_index(*ts_index)};
  CHECK_EQUAL(ts_cview.index(), 0u);
  for (size_t column = 0; column < sut.columns(); ++column) {
    auto cview = table_slice_column{sut, column};
    REQUIRE_NOT_EQUAL(cview.size(), 0u);
    CHECK_EQUAL(cview.index(), column);
    CHECK_EQUAL(cview.size(), sut.rows());
    for (size_t row = 0; row < cview.size(); ++row)
      CHECK_EQUAL(
        materialize(cview[row]),
        materialize(sut.at(row, column, flat_schema.field(column).type)));
  }
}

TEST(row view) {
  auto sut = zeek_conn_log[0];
  auto flat_schema = flatten(caf::get<record_type>(sut.schema()));
  for (size_t row = 0; row < sut.rows(); ++row) {
    auto rview = table_slice_row{sut, row};
    REQUIRE_NOT_EQUAL(rview.size(), 0u);
    CHECK_EQUAL(rview.index(), row);
    CHECK_EQUAL(rview.size(), sut.columns());
    for (size_t column = 0; column < rview.size(); ++column)
      CHECK_EQUAL(
        materialize(rview[column]),
        materialize(sut.at(row, column, flat_schema.field(column).type)));
  }
}

TEST(select - import time) {
  auto sut
    = table_slice{chunk::copy(zeek_conn_log_full[0]), table_slice::verify::yes};
  sut.offset(100);
  auto time = vast::time{std::chrono::milliseconds(202202141214)};
  sut.import_time(time);
  auto result
    = collect(select(sut, expression{}, make_ids({{110, 120}, {170, 180}})));
  REQUIRE_EQUAL(result.size(), 2u);
  CHECK_EQUAL(result[0].import_time(), time);
  CHECK_EQUAL(result[1].import_time(), time);
}

TEST(select all) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = collect(select(sut, {}, make_ids({{100, 200}})));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0], sut);
}

TEST(select none) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = collect(select(sut, {}, make_ids({{200, 300}})));
  CHECK_EQUAL(xs.size(), 0u);
}

TEST(select prefix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = collect(select(sut, {}, make_ids({{0, 150}})));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(make_data(xs[0]), make_data(sut, 0, 50));
}

TEST(select off by one prefix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = collect(select(sut, {}, make_ids({{101, 151}})));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(make_data(xs[0]), make_data(sut, 1, 50));
}

TEST(select intermediates) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = collect(select(sut, {}, make_ids({{110, 120}, {170, 180}})));
  REQUIRE_EQUAL(xs.size(), 2u);
  CHECK_EQUAL(xs[0].rows(), 10u);
  CHECK_EQUAL(make_data(xs[0]), make_data(sut, 10, 10));
  CHECK_EQUAL(xs[1].rows(), 10u);
  CHECK_EQUAL(make_data(xs[1]), make_data(sut, 70, 10));
}

TEST(select off by one suffix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = collect(select(sut, {}, make_ids({{149, 199}})));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(make_data(xs[0]), make_data(sut, 49, 50));
}

TEST(select suffix) {
  auto sut = zeek_conn_log_full[0];
  sut.offset(100);
  auto xs = collect(select(sut, {}, make_ids({{150, 300}})));
  REQUIRE_EQUAL(xs.size(), 1u);
  CHECK_EQUAL(xs[0].rows(), 50u);
  CHECK_EQUAL(make_data(xs[0]), make_data(sut, 50, 50));
}

TEST(truncate) {
  auto sut = zeek_conn_log[0];
  REQUIRE_EQUAL(sut.rows(), 8u);
  sut.offset(100);
  auto truncated_events = [&](size_t num_rows) {
    auto sub_slice = head(sut, num_rows);
    if (sub_slice.rows() != num_rows)
      FAIL("expected " << num_rows << " rows, got " << sub_slice.rows());
    return make_data(sub_slice);
  };
  auto sub_slice = head(sut, 8);
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
    tailor(unbox(to<expression>("id.orig_h != 192.168.1.102")), sut.schema()));
  auto result = filter(sut, exp);
  REQUIRE(result);
  CHECK_EQUAL(result->import_time(), time);
}

TEST(filter - expression overload) {
  auto sut = zeek_conn_log[0];
  // sut.offset(0);
  auto check_eval = [&](std::string_view expr, size_t x) {
    auto exp = unbox(tailor(unbox(to<expression>(expr)), sut.schema()));
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
    auto exp = unbox(tailor(unbox(to<expression>(expr)), sut.schema()));
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
}

TEST(cast) {
  const auto sut = head(zeek_conn_log_full[0], 3);
  REQUIRE_EQUAL(sut.rows(), 3u);
  const auto output_schema = type{
    "zeek.custom",
    record_type{
      // We can add null columns.
      {"foo", int64_type{}},
      // We can remove and assign metadata at the same time.
      {"ts", type{"not_timestamp", time_type{}, {{"foo"}}}},
      // We can change nesting.
      {"id",
       record_type{
         // Even nested fields can be re-ordered.
         {"orig_p", uint64_type{}},
         {"orig_h", ip_type{}},
         // Casting requires a full match on the key, so id.id.resp_h will be
         // all nulls.
         {"id",
          record_type{
            {"resp_h", ip_type{}},
          }},
       }},
      // We can also partially change nesting.
      {"id.resp_h", ip_type{}},
    },
  };
  CHECK_NOT_EQUAL(sut.schema(), output_schema);
  REQUIRE(can_cast(sut.schema(), output_schema));
  const auto output = cast(sut, output_schema);
  REQUIRE_EQUAL(output.schema(), output_schema);
  REQUIRE_EQUAL(output.rows(), 3u);
  const auto rows
    = collect(values(caf::get<record_type>(output_schema),
                     *to_record_batch(output)->ToStructArray().ValueOrDie()));
  const auto expected_rows = std::vector<record>{
    record{
      {"foo", caf::none},
      {"ts", unbox(to<vast::time>("2009-11-18T08:00:21.486539"))},
      {"id",
       record{
         {"orig_p", uint64_t{68}},
         {"orig_h", unbox(to<ip>("192.168.1.102"))},
         {"id",
          record{
            {"resp_h", caf::none},
          }},
       }},
      {"id.resp_h", unbox(to<ip>("192.168.1.1"))},
    },
    record{
      {"foo", caf::none},
      {"ts", unbox(to<vast::time>("2009-11-18T08:08:00.237253"))},
      {"id",
       record{
         {"orig_p", uint64_t{137}},
         {"orig_h", unbox(to<ip>("192.168.1.103"))},
         {"id",
          record{
            {"resp_h", caf::none},
          }},
       }},
      {"id.resp_h", unbox(to<ip>("192.168.1.255"))},
    },
    record{
      {"foo", caf::none},
      {"ts", unbox(to<vast::time>("2009-11-18T08:08:13.816224"))},
      {"id",
       record{
         {"orig_p", uint64_t{137}},
         {"orig_h", unbox(to<ip>("192.168.1.102"))},
         {"id",
          record{
            {"resp_h", caf::none},
          }},
       }},
      {"id.resp_h", unbox(to<ip>("192.168.1.255"))},
    },
  };
  REQUIRE_EQUAL(rows.size(), expected_rows.size());
  REQUIRE(rows[0]);
  REQUIRE(rows[1]);
  REQUIRE(rows[2]);
  // The string to time parsing has rounding errors, so we compare the strings
  // of records instead here; the time values are off by a few bits, but that
  // allows for using to<time>(...) above.
  CHECK_EQUAL(fmt::to_string(materialize(*rows[0])),
              fmt::to_string(expected_rows[0]));
  CHECK_EQUAL(fmt::to_string(materialize(*rows[1])),
              fmt::to_string(expected_rows[1]));
  CHECK_EQUAL(fmt::to_string(materialize(*rows[2])),
              fmt::to_string(expected_rows[2]));
}

TEST(project column flat index) {
  auto sut = head(zeek_conn_log[0], 3);
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
  CHECK(inspect(sink, slice));
  CHECK_EQUAL(detail::legacy_deserialize(buf, slice_copy), true);
  CHECK_EQUAL(slice_copy.offset(), 42u);
  CHECK_EQUAL(slice, slice_copy);
}

TEST(unflatten - order of columns) {
  auto flat_schema
    = type{"test.unflatten",
           record_type{
             {"foo.a", type{int64_type{}, {{"default", "uniform(100,200)"}}}},
             {"a", type{int64_type{}, {{"default", "uniform(100,200)"}}}},
             {"foo.b", type{int64_type{}, {{"default", "uniform(100,200)"}}}},
           }};
  auto input = make_random_table_slices(1, 1, flat_schema)->front();
  auto output = unflatten(input, ".");
  REQUIRE_EQUAL(output.schema(),
                (type{flat_schema.name(), record_type{
                                            {"foo",
                                             record_type{
                                               {"a", int64_type{}},
                                               {"b", int64_type{}},
                                             }},
                                            {"a", int64_type{}},
                                          }}));
  REQUIRE_EQUAL(output.rows(), input.rows());
  REQUIRE_EQUAL(output.columns(), input.columns());
  CHECK_EQUAL(materialize(input.at(0, 0)), materialize(output.at(0, 0)));
  CHECK_EQUAL(materialize(input.at(0, 1)), materialize(output.at(0, 2)));
  CHECK_EQUAL(materialize(input.at(0, 2)), materialize(output.at(0, 1)));
}

TEST(unflatten - unflattened field names are part of nested field names) {
  auto flat_schema = type{
    "test.unflatten",
    record_type{
      {"foo.bar.x.z", type{int64_type{}, {{"default", "uniform(100,200)"}}}},
      {"foo.bar.x", type{int64_type{}, {{"default", "uniform(100,200)"}}}},
      {"rand", type{int64_type{}, {{"default", "uniform(100,200)"}}}},
      {"foo.bar.y", type{int64_type{}, {{"default", "uniform(100,200)"}}}},
      {"foo", type{int64_type{}, {{"default", "uniform(100,200)"}}}},
    }};
  auto input = make_random_table_slices(1, 1, flat_schema)->front();
  auto output = unflatten(input, ".");
  REQUIRE_EQUAL(output.schema(),
                (type{flat_schema.name(), record_type{
                                            {"foo.bar.x.z", int64_type{}},
                                            {"foo.bar",
                                             record_type{
                                               {"x", int64_type{}},
                                               {"y", int64_type{}},
                                             }},
                                            {"rand", int64_type{}},
                                            {"foo", int64_type{}},
                                          }}));
  REQUIRE_EQUAL(output.rows(), input.rows());
  REQUIRE_EQUAL(output.columns(), input.columns());
  CHECK_EQUAL(materialize(input.at(0, 0)), materialize(output.at(0, 0)));
  CHECK_EQUAL(materialize(input.at(0, 1)), materialize(output.at(0, 1)));
  CHECK_EQUAL(materialize(input.at(0, 2)), materialize(output.at(0, 3)));
  CHECK_EQUAL(materialize(input.at(0, 3)), materialize(output.at(0, 2)));
  CHECK_EQUAL(materialize(input.at(0, 4)), materialize(output.at(0, 4)));
}

FIXTURE_SCOPE_END()
