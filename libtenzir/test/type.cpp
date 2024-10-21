//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/type.hpp"

#include "tenzir/collect.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/overload.hpp"
#include "tenzir/legacy_type.hpp"
#include "tenzir/logger.hpp"
#include "tenzir/test/fixtures/actor_system.hpp"
#include "tenzir/test/test.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <random>

namespace tenzir {

TEST(null_type) {
  static_assert(concrete_type<null_type>);
  // TODO: Add some more tests that cover the null type.
  auto t = type{};
  auto n = null_type{};
  auto tn = type{null_type{}};
  CHECK_EQUAL(t, n);
  CHECK_EQUAL(n, tn);
  CHECK_EQUAL(t.to_arrow_type(), arrow::null());
  CHECK_EQUAL(n.to_arrow_type(), arrow::null());
  CHECK_EQUAL(tn.to_arrow_type(), arrow::null());
  auto lt = list_type{t};
  auto ln = list_type{n};
  auto ltn = list_type{tn};
  CHECK_EQUAL(lt, ln);
  CHECK_EQUAL(ln, ltn);
  CHECK_EQUAL(*lt.to_arrow_type(), *arrow::list(arrow::null()));
  CHECK_EQUAL(*ln.to_arrow_type(), *arrow::list(arrow::null()));
  CHECK_EQUAL(*ltn.to_arrow_type(), *arrow::list(arrow::null()));
}

TEST(bool_type) {
  static_assert(concrete_type<bool_type>);
  static_assert(basic_type<bool_type>);
  static_assert(!complex_type<bool_type>);
  const auto t = type{};
  const auto bt = type{bool_type{}};
  CHECK(bt);
  CHECK_EQUAL(as_bytes(bt), as_bytes(bool_type{}));
  CHECK(t != bt);
  CHECK(t < bt);
  CHECK(t <= bt);
  CHECK_EQUAL(fmt::format("{}", bt), "bool");
  CHECK_EQUAL(fmt::format("{}", bool_type{}), "bool");
  CHECK(!caf::holds_alternative<bool_type>(t));
  CHECK(caf::holds_alternative<bool_type>(bt));
  const auto lbt = type::from_legacy_type(legacy_bool_type{});
  CHECK(caf::holds_alternative<bool_type>(lbt));
}

TEST(int64_type) {
  static_assert(concrete_type<int64_type>);
  static_assert(basic_type<int64_type>);
  static_assert(!complex_type<int64_type>);
  const auto t = type{};
  const auto it = type{int64_type{}};
  CHECK(it);
  CHECK_EQUAL(as_bytes(it), as_bytes(int64_type{}));
  CHECK(t != it);
  CHECK(t < it);
  CHECK(t <= it);
  CHECK_EQUAL(fmt::format("{}", it), "int64");
  CHECK_EQUAL(fmt::format("{}", int64_type{}), "int64");
  CHECK(!caf::holds_alternative<int64_type>(t));
  CHECK(caf::holds_alternative<int64_type>(it));
  const auto lit = type::from_legacy_type(legacy_integer_type{});
  CHECK(caf::holds_alternative<int64_type>(lit));
}

TEST(uint64_type) {
  static_assert(concrete_type<uint64_type>);
  static_assert(basic_type<uint64_type>);
  static_assert(!complex_type<uint64_type>);
  const auto t = type{};
  const auto ct = type{uint64_type{}};
  CHECK(ct);
  CHECK_EQUAL(as_bytes(ct), as_bytes(uint64_type{}));
  CHECK(t != ct);
  CHECK(t < ct);
  CHECK(t <= ct);
  CHECK_EQUAL(fmt::format("{}", ct), "uint64");
  CHECK_EQUAL(fmt::format("{}", uint64_type{}), "uint64");
  CHECK(!caf::holds_alternative<uint64_type>(t));
  CHECK(caf::holds_alternative<uint64_type>(ct));
  const auto lct = type::from_legacy_type(legacy_count_type{});
  CHECK(caf::holds_alternative<uint64_type>(lct));
}

TEST(double_type) {
  static_assert(concrete_type<double_type>);
  static_assert(basic_type<double_type>);
  static_assert(!complex_type<double_type>);
  const auto t = type{};
  const auto rt = type{double_type{}};
  CHECK(rt);
  CHECK_EQUAL(as_bytes(rt), as_bytes(double_type{}));
  CHECK(t != rt);
  CHECK(t < rt);
  CHECK(t <= rt);
  CHECK_EQUAL(fmt::format("{}", rt), "double");
  CHECK_EQUAL(fmt::format("{}", double_type{}), "double");
  CHECK(!caf::holds_alternative<double_type>(t));
  CHECK(caf::holds_alternative<double_type>(rt));
  const auto lrt = type::from_legacy_type(legacy_real_type{});
  CHECK(caf::holds_alternative<double_type>(lrt));
}

TEST(duration_type) {
  static_assert(concrete_type<duration_type>);
  static_assert(basic_type<duration_type>);
  static_assert(!complex_type<duration_type>);
  const auto t = type{};
  const auto dt = type{duration_type{}};
  CHECK(dt);
  CHECK_EQUAL(as_bytes(dt), as_bytes(duration_type{}));
  CHECK(t != dt);
  CHECK(t < dt);
  CHECK(t <= dt);
  CHECK_EQUAL(fmt::format("{}", dt), "duration");
  CHECK_EQUAL(fmt::format("{}", duration_type{}), "duration");
  CHECK(!caf::holds_alternative<duration_type>(t));
  CHECK(caf::holds_alternative<duration_type>(dt));
  const auto ldt = type::from_legacy_type(legacy_duration_type{});
  CHECK(caf::holds_alternative<duration_type>(ldt));
}

TEST(time_type) {
  static_assert(concrete_type<time_type>);
  static_assert(basic_type<time_type>);
  static_assert(!complex_type<time_type>);
  const auto t = type{};
  const auto tt = type{time_type{}};
  CHECK(tt);
  CHECK_EQUAL(as_bytes(tt), as_bytes(time_type{}));
  CHECK(t != tt);
  CHECK(t < tt);
  CHECK(t <= tt);
  CHECK_EQUAL(fmt::format("{}", tt), "time");
  CHECK_EQUAL(fmt::format("{}", time_type{}), "time");
  CHECK(!caf::holds_alternative<time_type>(t));
  CHECK(caf::holds_alternative<time_type>(tt));
  const auto ltt = type::from_legacy_type(legacy_time_type{});
  CHECK(caf::holds_alternative<time_type>(ltt));
}

TEST(string_type) {
  static_assert(concrete_type<string_type>);
  static_assert(basic_type<string_type>);
  static_assert(!complex_type<string_type>);
  const auto t = type{};
  const auto st = type{string_type{}};
  CHECK(st);
  CHECK_EQUAL(as_bytes(st), as_bytes(string_type{}));
  CHECK(t != st);
  CHECK(t < st);
  CHECK(t <= st);
  CHECK_EQUAL(fmt::format("{}", st), "string");
  CHECK_EQUAL(fmt::format("{}", string_type{}), "string");
  CHECK(!caf::holds_alternative<string_type>(t));
  CHECK(caf::holds_alternative<string_type>(st));
  const auto lst = type::from_legacy_type(legacy_string_type{});
  CHECK(caf::holds_alternative<string_type>(lst));
}

TEST(ip_type) {
  static_assert(concrete_type<ip_type>);
  static_assert(basic_type<ip_type>);
  static_assert(!complex_type<ip_type>);
  const auto t = type{};
  const auto at = type{ip_type{}};
  CHECK(at);
  CHECK_EQUAL(as_bytes(at), as_bytes(ip_type{}));
  CHECK(t != at);
  CHECK(t < at);
  CHECK(t <= at);
  CHECK_EQUAL(fmt::format("{}", at), "ip");
  CHECK_EQUAL(fmt::format("{}", ip_type{}), "ip");
  CHECK(!caf::holds_alternative<ip_type>(t));
  CHECK(caf::holds_alternative<ip_type>(at));
  const auto lat = type::from_legacy_type(legacy_address_type{});
  CHECK(caf::holds_alternative<ip_type>(lat));
}

TEST(subnet_type) {
  static_assert(concrete_type<subnet_type>);
  static_assert(basic_type<subnet_type>);
  static_assert(!complex_type<subnet_type>);
  const auto t = type{};
  const auto st = type{subnet_type{}};
  CHECK(st);
  CHECK_EQUAL(as_bytes(st), as_bytes(subnet_type{}));
  CHECK(t != st);
  CHECK(t < st);
  CHECK(t <= st);
  CHECK_EQUAL(fmt::format("{}", st), "subnet");
  CHECK_EQUAL(fmt::format("{}", subnet_type{}), "subnet");
  CHECK(!caf::holds_alternative<subnet_type>(t));
  CHECK(caf::holds_alternative<subnet_type>(st));
  const auto lst = type::from_legacy_type(legacy_subnet_type{});
  CHECK(caf::holds_alternative<subnet_type>(lst));
}

TEST(enumeration_type) {
  static_assert(concrete_type<enumeration_type>);
  static_assert(!basic_type<enumeration_type>);
  static_assert(complex_type<enumeration_type>);
  const auto t = type{};
  const auto et = type{enumeration_type{{"first"}, {"third", 2}, {"fourth"}}};
  CHECK(et);
  CHECK(t != et);
  CHECK(t < et);
  CHECK(t <= et);
  CHECK_EQUAL(fmt::format("{}", et), "enum {first: 0, third: 2, fourth: 3}");
  CHECK(!caf::holds_alternative<enumeration_type>(t));
  CHECK(caf::holds_alternative<enumeration_type>(et));
  CHECK_EQUAL(caf::get<enumeration_type>(et).field(0), "first");
  CHECK_EQUAL(caf::get<enumeration_type>(et).field(1), "");
  CHECK_EQUAL(caf::get<enumeration_type>(et).field(2), "third");
  CHECK_EQUAL(caf::get<enumeration_type>(et).field(3), "fourth");
  CHECK_EQUAL(caf::get<enumeration_type>(et).resolve("first"), 0u);
  CHECK_EQUAL(caf::get<enumeration_type>(et).resolve("second"), std::nullopt);
  CHECK_EQUAL(caf::get<enumeration_type>(et).resolve("third"), 2u);
  CHECK_EQUAL(caf::get<enumeration_type>(et).resolve("fourth"), 3u);
  const auto let = type::from_legacy_type(
    legacy_enumeration_type{{"first", "second", "third"}});
  CHECK(caf::holds_alternative<enumeration_type>(let));
  CHECK_EQUAL(caf::get<enumeration_type>(let).field(0), "first");
  CHECK_EQUAL(caf::get<enumeration_type>(let).field(1), "second");
  CHECK_EQUAL(caf::get<enumeration_type>(let).field(2), "third");
  CHECK_EQUAL(caf::get<enumeration_type>(let).field(3), "");
}

TEST(list_type) {
  static_assert(concrete_type<list_type>);
  static_assert(!basic_type<list_type>);
  static_assert(complex_type<list_type>);
  const auto t = type{};
  const auto tlit = type{list_type{int64_type{}}};
  const auto lit = list_type{int64_type{}};
  CHECK(tlit);
  CHECK_EQUAL(as_bytes(tlit), as_bytes(lit));
  CHECK(t != tlit);
  CHECK(t < tlit);
  CHECK(t <= tlit);
  CHECK_EQUAL(fmt::format("{}", tlit), "list<int64>");
  CHECK_EQUAL(fmt::format("{}", list_type{{}}), "list<null>");
  CHECK(!caf::holds_alternative<list_type>(t));
  CHECK(caf::holds_alternative<list_type>(tlit));
  CHECK_EQUAL(caf::get<list_type>(tlit).value_type(), type{int64_type{}});
  const auto llbt
    = type::from_legacy_type(legacy_list_type{legacy_bool_type{}});
  CHECK(caf::holds_alternative<list_type>(llbt));
  CHECK_EQUAL(caf::get<list_type>(llbt).value_type(), type{bool_type{}});
}

TEST(map_type) {
  static_assert(concrete_type<map_type>);
  static_assert(!basic_type<map_type>);
  static_assert(complex_type<map_type>);
  const auto t = type{};
  const auto tmsit = type{map_type{string_type{}, int64_type{}}};
  const auto msit = map_type{string_type{}, int64_type{}};
  CHECK(tmsit);
  CHECK_EQUAL(as_bytes(tmsit), as_bytes(msit));
  CHECK(t != tmsit);
  CHECK(t < tmsit);
  CHECK(t <= tmsit);
  CHECK_EQUAL(fmt::format("{}", tmsit), "map<string, int64>");
  CHECK_EQUAL(fmt::format("{}", map_type{{}, {}}), "map<null, null>");
  CHECK(!caf::holds_alternative<map_type>(t));
  CHECK(caf::holds_alternative<map_type>(tmsit));
  CHECK_EQUAL(caf::get<map_type>(tmsit).key_type(), type{string_type{}});
  CHECK_EQUAL(caf::get<map_type>(tmsit).value_type(), type{int64_type{}});
  const auto lmabt = type::from_legacy_type(
    legacy_map_type{legacy_address_type{}, legacy_bool_type{}});
  CHECK(caf::holds_alternative<map_type>(lmabt));
  CHECK_EQUAL(caf::get<map_type>(lmabt).key_type(), type{ip_type{}});
  CHECK_EQUAL(caf::get<map_type>(lmabt).value_type(), type{bool_type{}});
}

TEST(record_type) {
  static_assert(concrete_type<record_type>);
  static_assert(!basic_type<record_type>);
  static_assert(complex_type<record_type>);
  const auto t = type{};
  const auto rt = type{record_type{
    {"i", int64_type{}},
    {"r1",
     record_type{
       {"p", type{"port", int64_type{}}},
       {"a", ip_type{}},
     }},
    {"b", bool_type{}},
    {"r2",
     record_type{
       {"s", subnet_type{}},
     }},
  }};
  CHECK_EQUAL(fmt::format("{}", rt), "record {i: int64, r1: record {p: port, "
                                     "a: ip}, b: bool, r2: record {s: "
                                     "subnet}}");
  const auto& r = caf::get<record_type>(rt);
  CHECK_EQUAL(r.field(2).type, bool_type{});
  CHECK_EQUAL(r.field({1, 1}).type, ip_type{});
  CHECK_EQUAL(r.field({3, 0}).name, "s");
  CHECK_EQUAL(flatten(rt), type{flatten(r)});
}

TEST(record_type name resolving) {
  const auto rt = record_type{
    {"i", int64_type{}},
    {"r",
     record_type{
       {"p", type{"port", int64_type{}}},
       {"a", ip_type{}},
       {"not_i", int64_type{}},
     }},
    {"b", type{bool_type{}, {{"key"}}}},
    {"r2",
     record_type{
       {"s", type{subnet_type{}, {{"key", "value"}}}},
       {"r",
        record_type{
          {"a", ip_type{}},
        }},
     }},
  };
  CHECK_EQUAL(rt.resolve_key("i"), offset{0});
  CHECK_EQUAL(rt.resolve_key("r2"), offset{3});
  CHECK_EQUAL(rt.resolve_key("r.a"), (offset{1, 1}));
  CHECK_EQUAL(rt.resolve_key("a"), std::nullopt);
  CHECK_EQUAL(rt.resolve_key("r.not"), std::nullopt);
  auto to_vector = [](auto&& rng) {
    std::vector<offset> result{};
    for (auto&& elem : std::forward<decltype(rng)>(rng))
      result.push_back(std::forward<decltype(elem)>(elem));
    return result;
  };
  CHECK_EQUAL(to_vector(rt.resolve_key_suffix("a")),
              (std::vector<offset>{{1, 1}, {3, 1, 0}}));
  CHECK_EQUAL(to_vector(rt.resolve_key_suffix("r.a")),
              (std::vector<offset>{{1, 1}, {3, 1, 0}}));
  CHECK_EQUAL(to_vector(rt.resolve_key_suffix("r")), (std::vector<offset>{}));
  CHECK_EQUAL(to_vector(rt.resolve_key_suffix("r2.r.a")),
              (std::vector<offset>{{3, 1, 0}}));
  CHECK_EQUAL(to_vector(rt.resolve_key_suffix("2.r.a")),
              (std::vector<offset>{}));
  CHECK_EQUAL(to_vector(rt.resolve_key_suffix("i")),
              (std::vector<offset>{{0}}));
  CHECK_EQUAL(to_vector(rt.resolve_key_suffix("")), (std::vector<offset>{}));
  CHECK_EQUAL(to_vector(rt.resolve_key_suffix("t.u.r2.r.a", "t.u")),
              (std::vector<offset>{{3, 1, 0}}));
  CHECK_EQUAL(to_vector(rt.resolve_key_suffix("u.r2.r.a", "t.u")),
              (std::vector<offset>{{3, 1, 0}}));
  CHECK_EQUAL(to_vector(rt.resolve_key_suffix(".u.r2.r.a", "t.u")),
              (std::vector<offset>{}));
  const auto zeek_conn = type{
    "zeek.conn",
    record_type{
      {"ts", type{"timestamp", time_type{}}},
      {"uid", type{string_type{}, {{"index", "hash"}}}},
      {
        "id",
        type{"zeek.conn_id",
             record_type{
               {"orig_h", ip_type{}},
               {"orig_p", type{"port", uint64_type{}}},
               {"resp_h", ip_type{}},
               {"resp_p", type{"port", uint64_type{}}},
             }},
      },
      {"proto", string_type{}},
    },
  };
  CHECK_EQUAL(to_vector(caf::get<record_type>(zeek_conn).resolve_key_suffix(
                "resp_p", zeek_conn.name())),
              (std::vector<offset>{{2, 3}}));
  CHECK_EQUAL(
    to_vector(caf::get<record_type>(zeek_conn).resolve_key_suffix("resp_p")),
    (std::vector<offset>{{2, 3}}));
  const auto zeek_conn_flat = flatten(zeek_conn);
  CHECK_EQUAL(to_vector(caf::get<record_type>(zeek_conn_flat)
                          .resolve_key_suffix("resp_p", zeek_conn.name())),
              (std::vector<offset>{{5}}));
  CHECK_EQUAL(
    to_vector(
      caf::get<record_type>(zeek_conn_flat).resolve_key_suffix("resp_p")),
    (std::vector<offset>{{5}}));
}

TEST(record_type type resolving) {
  const auto layout = record_type{
    {"ts", type{"timestamp", time_type{}}},
    {"uid", type{"id", string_type{}, {{"index", "hash"}}}},
    {
      "id",
      type{"conn_id",
           record_type{
             {"orig_h", ip_type{}},
             {"orig_p", type{"port", uint64_type{}}},
             {"resp_h", ip_type{}},
             {"resp_p", type{"port", uint64_type{}}},
           }},
    },
    {"proto", string_type{}},
  };
  CHECK_EQUAL(collect(layout.resolve_type_extractor(":time")),
              (std::vector<offset>{{0}}));
  CHECK_EQUAL(collect(layout.resolve_type_extractor(":timestamp")),
              (std::vector<offset>{{0}}));
  CHECK_EQUAL(collect(layout.resolve_type_extractor(":id")),
              (std::vector<offset>{{1}}));
  CHECK_EQUAL(collect(layout.resolve_type_extractor(":string")),
              (std::vector<offset>{{1}, {3}}));
  CHECK_EQUAL(collect(layout.resolve_type_extractor(":ip")),
              (std::vector<offset>{{2, 0}, {2, 2}}));
  CHECK_EQUAL(collect(layout.resolve_type_extractor(":port")),
              (std::vector<offset>{{2, 1}, {2, 3}}));
  CHECK_EQUAL(collect(layout.resolve_type_extractor(":uint64")),
              (std::vector<offset>{{2, 1}, {2, 3}}));
  CHECK_EQUAL(collect(layout.resolve_type_extractor(":record")),
              (std::vector<offset>{}));
  CHECK_EQUAL(collect(layout.resolve_type_extractor(":conn_id")),
              (std::vector<offset>{}));
  // TODO: type extractors do not currently support stepping through lists,
  // which we nede to fix as part of the planned unnesting effort.
}

TEST(record_type flat index computation) {
  auto x = record_type{
    {"x",
     record_type{
       {"y",
        record_type{
          {"z", int64_type{}},
          {"k", bool_type{}},
        }},
       {"m",
        record_type{
          {"y",
           record_type{
             {"a", ip_type{}},
           }},
          {"f", double_type{}},
        }},
       {"b", bool_type{}},
     }},
    {"y",
     record_type{
       {"b", bool_type{}},
     }},
  };
  CHECK_EQUAL(x.num_fields(), 2u);
  CHECK_EQUAL(x.num_leaves(), 6u);
  CHECK_EQUAL(caf::get<record_type>(x.field(0).type).num_fields(), 3u);
  CHECK_EQUAL(caf::get<record_type>(x.field(0).type).num_leaves(), 5u);
  CHECK_EQUAL(caf::get<record_type>(x.field(1).type).num_fields(), 1u);
  CHECK_EQUAL(caf::get<record_type>(x.field(1).type).num_leaves(), 1u);
  CHECK_EQUAL(x.flat_index(offset({0, 0, 0})), 0u);
  CHECK_EQUAL(x.flat_index(offset({0, 0, 1})), 1u);
  CHECK_EQUAL(x.flat_index(offset({0, 1, 0, 0})), 2u);
  CHECK_EQUAL(x.flat_index(offset({0, 1, 1})), 3u);
  CHECK_EQUAL(x.flat_index(offset({0, 2})), 4u);
  CHECK_EQUAL(x.flat_index(offset({1, 0})), 5u);
}

TEST(record type transformation) {
  const auto old = record_type{
    {"x",
     record_type{
       {"y",
        record_type{
          {"z", int64_type{}},
          {"k", bool_type{}},
        }},
       {"m",
        record_type{
          {"y",
           record_type{
             {"a", ip_type{}},
           }},
          {"f", double_type{}},
        }},
       {"b", bool_type{}},
     }},
    {"y",
     record_type{
       {"b", bool_type{}},
     }},
  };
  const auto expected = record_type{
    {"x",
     record_type{
       {"y",
        record_type{
          {"z", int64_type{}},
          {"t", type{}},
          {"u", ip_type{}},
          {"k", bool_type{}},
        }},
       {"m",
        record_type{
          {"f", double_type{}},
        }},
       {"b", bool_type{}},
     }},
    {"y",
     record_type{
       {"b2", bool_type{}},
     }},
  };
  const auto result = old.transform({
    {{0, 0, 1}, record_type::insert_before({{"t", type{}}, {"u", ip_type{}}})},
    {{0, 1, 0, 0}, record_type::drop()},
    {{1, 0}, record_type::assign({{"b2", bool_type{}}})},
  });
  REQUIRE(result);
  CHECK_EQUAL(*result, expected);
  CHECK_EQUAL(fmt::format("{}", *result), fmt::format("{}", expected));
  const auto xyz = record_type{{
    "x",
    record_type{{
      "y",
      record_type{
        {"z", int64_type{}},
      },
    }},
  }};
  CHECK_EQUAL(xyz.transform({{{0}, record_type::drop()}}), std::nullopt);
  CHECK_EQUAL(xyz.transform({{{0, 0}, record_type::drop()}}), std::nullopt);
  CHECK_EQUAL(xyz.transform({{{0, 0, 0}, record_type::drop()}}), std::nullopt);
}

TEST(record_type merging) {
  const auto lhs = record_type{
    {"x",
     record_type{
       {"u",
        record_type{
          {"a", int64_type{}},
          {"b", bool_type{}},
        }},
     }},
    {"y",
     record_type{
       {"b", bool_type{}},
     }},
  };
  const auto rhs = record_type{
    {"x",
     record_type{
       {"y",
        record_type{
          {"a", uint64_type{}},
          {"b", double_type{}},
          {"c", int64_type{}},
        }},
       {"b", bool_type{}},
     }},
    {"y", subnet_type{}},
  };
  const auto expected_result_prefer_left = record_type{
    {"x",
     record_type{
       {"u",
        record_type{
          {"a", int64_type{}},
          {"b", bool_type{}},
        }},
       {"y",
        record_type{
          {"a", uint64_type{}},
          {"b", double_type{}},
          {"c", int64_type{}},
        }},
       {"b", bool_type{}},
     }},
    {"y",
     record_type{
       {"b", bool_type{}},
     }},
  };
  const auto expected_result_prefer_right = record_type{
    {"x",
     record_type{
       {"u",
        record_type{
          {"a", int64_type{}},
          {"b", bool_type{}},
        }},
       {"y",
        record_type{
          {"a", uint64_type{}},
          {"b", double_type{}},
          {"c", int64_type{}},
        }},
       {"b", bool_type{}},
     }},
    {"y", subnet_type{}},
  };
  const auto expected_result_fail
    = caf::make_error(ec::logic_error, fmt::format("conflicting field x; "
                                                   "failed to merge {} and "
                                                   "{}",
                                                   lhs, rhs));
  const auto result_prefer_right
    = merge(lhs, rhs, record_type::merge_conflict::prefer_right);
  const auto result_prefer_left
    = merge(lhs, rhs, record_type::merge_conflict::prefer_left);
  const auto result_fail = merge(lhs, rhs, record_type::merge_conflict::fail);
  REQUIRE(result_prefer_right);
  CHECK_EQUAL(fmt::format("{}", *result_prefer_right),
              fmt::format("{}", expected_result_prefer_right));
  REQUIRE(result_prefer_left);
  CHECK_EQUAL(fmt::format("{}", *result_prefer_left),
              fmt::format("{}", expected_result_prefer_left));
  REQUIRE(!result_fail);
  CHECK_EQUAL(result_fail.error(), expected_result_fail);
}

TEST(type inference) {
  CHECK_EQUAL(type::infer(caf::none), type{});
  CHECK_EQUAL(type::infer(bool{}), bool_type{});
  CHECK_EQUAL(type::infer(int64_t{}), int64_type{});
  CHECK_EQUAL(type::infer(uint64_t{}), uint64_type{});
  CHECK_EQUAL(type::infer(double{}), double_type{});
  CHECK_EQUAL(type::infer(duration{}), duration_type{});
  CHECK_EQUAL(type::infer(time{}), time_type{});
  CHECK_EQUAL(type::infer(std::string{}), string_type{});
  CHECK_EQUAL(type::infer(pattern{}), string_type{});
  CHECK_EQUAL(type::infer(ip{}), ip_type{});
  CHECK_EQUAL(type::infer(subnet{}), subnet_type{});
  // Enumeration types cannot be inferred.
  CHECK_EQUAL(type::infer(enumeration{0}), std::nullopt);
  // List and map types can only be inferred if the nested values can be
  // inferred.
  CHECK_EQUAL(type::infer(list{}), list_type{type{}});
  CHECK_EQUAL(type::infer(list{caf::none}), list_type{type{}});
  CHECK_EQUAL(type::infer(list{bool{}}), list_type{bool_type{}});
  CHECK_EQUAL(type::infer(list{caf::none, int64_t{}}), list_type{int64_type{}});
  CHECK_EQUAL(type::infer(list{int64_t{}, uint64_t{}}), std::nullopt);
  CHECK_EQUAL(type::infer(list{caf::none, int64_t{}, uint64_t{}}),
              std::nullopt);
  CHECK_EQUAL(type::infer(map{}), (map_type{type{}, type{}}));
  CHECK_EQUAL(type::infer(map{{caf::none, caf::none}}),
              (map_type{type{}, type{}}));
  CHECK_EQUAL(type::infer(map{{caf::none, int64_t{}}}),
              (map_type{type{}, int64_type{}}));
  CHECK_EQUAL(type::infer(map{{bool{}, caf::none}}),
              (map_type{bool_type{}, type{}}));
  CHECK_EQUAL(type::infer(map{{bool{}, int64_t{}}}),
              (map_type{bool_type{}, int64_type{}}));
  const auto r = record{
    {"a", bool{}},
    {"b", int64_t{}},
    {"c",
     record{
       {"d", uint64_t{}},
     }},
  };
  const auto rt = record_type{
    {"a", bool_type{}},
    {"b", int64_type{}},
    {"c",
     record_type{
       {"d", uint64_type{}},
     }},
  };
  CHECK_EQUAL(type::infer(r), rt);
}

TEST(legacy_type conversion) {
  const auto rt = type{record_type{
    {"i", int64_type{}},
    {"r1",
     record_type{
       {"p", type{"port", int64_type{}}},
       {"a", ip_type{}},
     }},
    {"b", type{bool_type{}, {{"key"}}}},
    {"r2",
     record_type{
       {"s", type{subnet_type{}, {{"key", "value"}}}},
     }},
  }};
  const auto lrt = legacy_type{legacy_record_type{
    {"i", legacy_integer_type{}},
    {"r1",
     legacy_record_type{
       {"p", legacy_alias_type{legacy_integer_type{}}.name("port")},
       {"a", legacy_address_type{}},
     }},
    {"b", legacy_bool_type{}.attributes({{"key"}})},
    {"r2",
     legacy_record_type{
       {"s", legacy_subnet_type{}.attributes({{"key", "value"}})},
     }},
  }};
  // Note that rt == type{lrt} fails because the types are semantically
  // equivalent, but not exactly equivalent because of the inconsistent
  // handling of naming in legacy types. As such, the following checks fail:
  //   CHECK_EQUAL(rt, type{lrt});
  //   CHECK_EQUAL(legacy_type{rt}, lrt);
  // Instead, we instead compare the printed versions of the types for
  // equivalence.
  CHECK_EQUAL(fmt::format("{}", rt),
              fmt::format("{}", type::from_legacy_type(lrt)));
  CHECK_EQUAL(fmt::format("{}", type::from_legacy_type(rt.to_legacy_type())),
              fmt::format("{}", type::from_legacy_type(lrt)));
}

TEST(named types) {
  const auto at = type{"l1", bool_type{}};
  CHECK(caf::holds_alternative<bool_type>(at));
  CHECK_EQUAL(at.name(), "l1");
  CHECK_EQUAL(fmt::format("{}", at), "l1");
  const auto aat = type{"l2", at};
  CHECK(caf::holds_alternative<bool_type>(aat));
  CHECK_EQUAL(aat.name(), "l2");
  CHECK_EQUAL(fmt::format("{}", aat), "l2");
  auto aat_names = std::string{};
  for (auto&& name : aat.names())
    fmt::format_to(std::back_inserter(aat_names), "{}", name);
  CHECK_EQUAL(aat_names, "l2l1");
  const auto lat = type::from_legacy_type(legacy_bool_type{}.name("l3"));
  CHECK(caf::holds_alternative<bool_type>(lat));
  CHECK_EQUAL(lat.name(), "l3");
  CHECK_EQUAL(fmt::format("{}", lat), "l3");
}

TEST(enriched types) {
  const auto at = type{"l1", bool_type{}, {{"first", "value"}, {"second"}}};
  CHECK(caf::holds_alternative<bool_type>(at));
  CHECK_EQUAL(at.name(), "l1");
  CHECK_EQUAL(at.attribute("first"), "value");
  CHECK_EQUAL(at.attribute("second"), "");
  CHECK_EQUAL(at.attribute("third"), std::nullopt);
  CHECK_EQUAL(at.attribute("fourth"), std::nullopt);
  CHECK_EQUAL(fmt::format("{}", at), "l1 #first=value #second");
  const auto aat = type{"l2", at, {{"third", "nestingworks"}}};
  CHECK(caf::holds_alternative<bool_type>(aat));
  CHECK_EQUAL(aat.name(), "l2");
  CHECK_EQUAL(aat.attribute("first"), "value");
  CHECK_EQUAL(aat.attribute("second"), "");
  CHECK_EQUAL(aat.attribute("third"), "nestingworks");
  CHECK_EQUAL(aat.attribute("fourth"), std::nullopt);
  CHECK_EQUAL(fmt::format("{}", aat), "l2 #third=nestingworks");
  const auto lat = type::from_legacy_type(
    legacy_bool_type{}.attributes({{"first", "value"}, {"second"}}).name("l1"));
  CHECK_EQUAL(lat, at);
}

TEST(aliases) {
  const auto t1 = bool_type{};
  const auto t2 = type{"quux", t1};
  const auto t3 = type{"qux", t2, {{"first"}}};
  const auto t4 = type{"baz", t3};
  const auto t5 = type{t4, {{"second"}}};
  const auto t6 = type{"bar", t5, {{"third"}}};
  const auto t7 = type{"foo", t6, {{"fourth"}}};
  auto aliases = std::vector<type>{};
  for (auto&& alias : t7.aliases())
    aliases.push_back(std::move(alias));
  REQUIRE_EQUAL(aliases.size(), 5u);
  CHECK_EQUAL(aliases[0], t6);
  CHECK_EQUAL(aliases[1], t4);
  CHECK_EQUAL(aliases[2], t3);
  CHECK_EQUAL(aliases[3], t2);
  CHECK_EQUAL(aliases[4], t1);
}

TEST(metadata layer merging) {
  const auto t1 = type{
    "foo",
    bool_type{},
    {{"one", "eins"}, {"two", "zwei"}},
  };
  MESSAGE("attributes do get merged in unnamed metadata layers");
  const auto t2 = type{
    "foo",
    type{
      bool_type{},
      {{"two", "zwei"}},
    },
    {{"one", "eins"}},
  };
  CHECK_EQUAL(t1, t2);

  MESSAGE("attributes do not get merged in named metadata layers");
  const auto t3 = type{
    type{
      "foo",
      bool_type{},
      {{"two", "zwei"}},
    },
    {{"one", "eins"}},
  };
  CHECK_NOT_EQUAL(t1, t3);
  MESSAGE("attribute merging prefers new attributes");
  const auto t4 = type{
    "foo",
    type{
      bool_type{},
      {{"one"}, {"two", "zwei"}},
    },
    {{"one", "eins"}},
  };
  CHECK_EQUAL(t1, t4);
  CHECK_EQUAL((type{"x", bool_type{}, {{"first"}, {"second"}}}),
              (type{"x", bool_type{}, {{"second"}, {"first"}}}));
}

TEST(sorting) {
  auto ts = std::vector<type>{
    type{},
    type{bool_type{}},
    type{int64_type{}},
    type{"custom_none", type{}},
    type{"custom_bool", bool_type{}},
    type{"custom_integer", int64_type{}},
  };
  std::shuffle(ts.begin(), ts.end(), std::random_device());
  std::sort(ts.begin(), ts.end());
  const char* expected
    = "null bool int64 custom_bool custom_none custom_integer";
  CHECK_EQUAL(fmt::format("{}", fmt::join(ts, " ")), expected);
}

TEST(construct) {
  // This type is taking from the "tenzir import test" generator feature.
  // The default blueprint record type contains the duplicate field name
  // "s", for which we must still be able to correctly create a record.
  // This is achieved by internally using record::make_unsafe to allow for
  // duplicates.
  // TODO: This test will once we replace record with a better-suited data
  // structure that more clearly enforces its contract. The
  // `record::make_unsafe` functionality should not exist.
  const auto t = type{
    "test.full",
    record_type{
      {"n", list_type{int64_type{}}},
      {"b", type{bool_type{}, {{"default", "uniform(0,1)"}}}},
      {"i", type{int64_type{}, {{"default", "uniform(-42000,1337)"}}}},
      {"c", type{uint64_type{}, {{"default", "pareto(0,1)"}}}},
      {"r", type{double_type{}, {{"default", "normal(0,1)"}}}},
      {"s", type{string_type{}, {{"default", "uniform(0,100)"}}}},
      {"t", type{time_type{}, {{"default", "uniform(0,10)"}}}},
      {"d", type{duration_type{}, {{"default", "uniform(100,200)"}}}},
      {"a", type{ip_type{}, {{"default", "uniform(0,2000000)"}}}},
      {"s", type{subnet_type{}, {{"default", "uniform(1000,2000)"}}}},
    },
  };
  const auto expected = record::vector_type{
    {"n", data{list{}}},    {"b", data{bool{}}},
    {"i", data{int64_t{}}}, {"c", data{uint64_t{}}},
    {"r", data{double{}}},  {"s", data{std::string{}}},
    {"t", data{time{}}},    {"d", data{duration{}}},
    {"a", data{ip{}}},      {"s", data{subnet{}}},
  };
  CHECK_EQUAL(t.construct(), record::make_unsafe(expected));
}

TEST(sum type) {
  // Returns a visitor that checks whether the expected concrete types are
  // the types resulting in the visitation.
  auto is_type = []<concrete_type... T>(const T&...) {
    return []<concrete_type... U>(const U&...) {
      return (std::is_same_v<T, U> && ...);
    };
  };
  CHECK(caf::visit(is_type(ip_type{}), type{ip_type{}}));
  CHECK(caf::visit(is_type(bool_type{}), type{bool_type{}}));
  CHECK(caf::visit(is_type(bool_type{}, int64_type{}), type{bool_type{}},
                   type{int64_type{}}));
}

TEST(hashes) {
  auto hash = []<type_or_concrete_type T>(const T& value) {
    auto hasher = std::hash<T>{};
    return hasher(value);
  };
  // We're comparing strings here because that is easier to change from
  // the log output in failed unit tests. :-)
  CHECK_EQUAL(fmt::format("0x{:X}", hash(type{})), "0xB51ACBDD64EF56FF");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(bool_type{})), "0x295A1E349D71CC2"
                                                        "3");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(int64_type{})), "0x5B0D4F0B0B16740"
                                                         "4");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(uint64_type{})), "0x529C2667783DB09"
                                                          "D");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(double_type{})), "0x41615FDB30A38AA"
                                                          "F");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(duration_type{})), "0x6C3BE97C5D5B269"
                                                            "A");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(time_type{})), "0xAD8E364A7A3BFE7"
                                                        "9");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(string_type{})), "0x2476398993549B"
                                                          "5");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(ip_type{})), "0xD1678F8D9318E8B"
                                                      "2");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(subnet_type{})), "0xA927755C100351"
                                                          "93");
  CHECK_EQUAL(fmt::format("0x{:X}",
                          hash(enumeration_type{{"a"}, {"b"}, {"c"}})),
              "0xFFF139D14A6FFAA4");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(list_type{int64_type{}})), "0x2F697BD2"
                                                                    "223CA310");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(map_type{time_type{}, string_type{}})),
              "0x355D5293D16CC7CD");
  CHECK_EQUAL(fmt::format("0x{:X}", hash(record_type{{"a", ip_type{}},
                                                     {"b", bool_type{}}})),
              "0xC262CE1B00968C16");
}

TEST(congruence) {
  auto i = type{int64_type{}};
  auto j = type{int64_type{}};
  CHECK(i == j);
  i = type{"i", i};
  j = type{"j", j};
  CHECK(i != j);
  auto c = type{"c", uint64_type{}};
  CHECK(congruent(i, i));
  CHECK(congruent(i, j));
  CHECK(!congruent(i, c));
  auto l0 = type{list_type{i}};
  auto l1 = type{list_type{j}};
  auto l2 = type{list_type{c}};
  CHECK(l0 != l1);
  CHECK(l0 != l2);
  CHECK(congruent(l0, l1));
  CHECK(!congruent(l1, l2));
  auto r0 = type{record_type{
    {"a", ip_type{}},
    {"b", bool_type{}},
    {"c", uint64_type{}},
  }};
  auto r1 = type{record_type{
    {"x", ip_type{}},
    {"y", bool_type{}},
    {"z", uint64_type{}},
  }};
  CHECK(r0 != r1);
  CHECK(congruent(r0, r1));
  auto a = type{"a", i};
  CHECK(a != i);
  CHECK(congruent(a, i));
  a = type{"r0", r0};
  CHECK(a != r0);
  CHECK(congruent(a, r0));
}

TEST(compatibility) {
  CHECK(
    compatible(type{ip_type{}}, relational_operator::in, type{subnet_type{}}));
  CHECK(compatible(type{ip_type{}}, relational_operator::in, subnet{}));
  CHECK(compatible(type{subnet_type{}}, relational_operator::in,
                   type{subnet_type{}}));
  CHECK(compatible(type{subnet_type{}}, relational_operator::in, subnet{}));
}

TEST(subset) {
  auto i = type{int64_type{}};
  auto j = type{int64_type{}};
  CHECK(is_subset(i, j));
  i = type{"i", i};
  j = type{"j", j};
  CHECK(is_subset(i, j));
  auto c = type{"c", uint64_type{}};
  CHECK(is_subset(i, i));
  CHECK(is_subset(i, j));
  CHECK(!is_subset(i, c));
  auto r0 = type{record_type{
    {"a", ip_type{}},
    {"b", bool_type{}},
    {"c", uint64_type{}},
  }};
  // Rename a field.
  auto r1 = type{record_type{
    {"a", ip_type{}},
    {"b", bool_type{}},
    {"d", uint64_type{}},
  }};
  // Add a field.
  auto r2 = type{record_type{
    {"a", ip_type{}},
    {"b", bool_type{}},
    {"c", uint64_type{}},
    {"d", uint64_type{}},
  }};
  // Remove a field.
  auto r3 = type{record_type{
    {"a", ip_type{}},
    {"c", uint64_type{}},
  }};
  // Change a field's type.
  auto r4 = type{record_type{
    {"a", string_type{}},
    {"b", bool_type{}},
    {"c", uint64_type{}},
  }};
  CHECK(is_subset(r0, r0));
  CHECK(!is_subset(r0, r1));
  CHECK(is_subset(r0, r2));
  CHECK(!is_subset(r0, r3));
  CHECK(!is_subset(r0, r4));
}

namespace {

class fixture : public fixtures::deterministic_actor_system {
public:
  fixture() : fixtures::deterministic_actor_system(TENZIR_PP_STRINGIFY(SUITE)) {
  }
};

} // namespace

FIXTURE_SCOPE(type_fixture, fixture)

TEST(serialization) {
  CHECK_ROUNDTRIP(type{});
  CHECK_ROUNDTRIP(type{bool_type{}});
  CHECK_ROUNDTRIP(type{int64_type{}});
  CHECK_ROUNDTRIP(type{uint64_type{}});
  CHECK_ROUNDTRIP(type{double_type{}});
  CHECK_ROUNDTRIP(type{duration_type{}});
  CHECK_ROUNDTRIP(type{time_type{}});
  CHECK_ROUNDTRIP(type{string_type{}});
  CHECK_ROUNDTRIP(type{ip_type{}});
  CHECK_ROUNDTRIP(type{subnet_type{}});
  CHECK_ROUNDTRIP(type{enumeration_type{{"a"}, {"b"}, {"c"}}});
  CHECK_ROUNDTRIP(type{list_type{int64_type{}}});
  CHECK_ROUNDTRIP(type{map_type{ip_type{}, subnet_type{}}});
  const auto rt = type{record_type{
    {"i", int64_type{}},
    {"r1",
     record_type{
       {"p", type{"port", int64_type{}}},
       {"a", ip_type{}},
     }},
    {"b", bool_type{}},
    {"r2",
     record_type{
       {"s", subnet_type{}},
     }},
  }};
  CHECK_ROUNDTRIP(rt);
}

FIXTURE_SCOPE_END()

} // namespace tenzir
