//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/multi_series_builder.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"

#include <caf/detail/type_list.hpp>

#include <cstddef>
#include <cstdint>

namespace tenzir {
namespace {

using namespace detail::record_builder;
using namespace std::literals;
using namespace std::chrono_literals;

using r = record;
using vr = std::vector<r>;
using vvr = std::vector<vr>;

[[maybe_unused]] auto print_separator(const char c) -> void {
  fmt::print("{}\n", std::string(10, c));
}

[[maybe_unused]] auto print(const series& res) -> void {
  print_separator('-');
  for (const auto& event : res.values()) {
    fmt::print("{}\n", event);
  }
  print_separator('-');
}

[[maybe_unused]] auto print(const vr& exp) -> void {
  print_separator('-');
  for (const auto& event : exp) {
    fmt::print("{}\n", event);
  }
  print_separator('-');
}

[[maybe_unused]] auto print(const std::vector<series>& res) -> void {
  print_separator('=');
  for (const auto& s : res) {
    print(s);
  }
  print_separator('=');
}

[[maybe_unused]] auto print(const vvr& res) -> void {
  print_separator('=');
  for (const auto& s : res) {
    print(s);
  }
  print_separator('=');
}

auto check_outcome(const std::vector<series>& res,
                   const vvr& expected) -> void {
  auto res_it = res.begin();
  auto exp_it = expected.begin();
  if (res.size() != expected.size()) {
    fmt::print("batch count mismatch. res: {}; expected: {}\n", res.size(),
               expected.size());
    fmt::print("res:\n");
    print(res);
    fmt::print("exp:\n");
    print(expected);
  }
  REQUIRE_EQUAL(res.size(), expected.size());
  size_t batch_number = 0;
  for (; res_it != res.end() and exp_it != expected.end(); ++res_it, ++exp_it) {
    auto exp_event_it = exp_it->begin();
    const auto res_size = static_cast<size_t>(res_it->length());
    const auto exp_size = exp_it->size();
    if (res_size != exp_size) {
      fmt::print("batch size mismatch in batch {}\n", batch_number);
      CHECK(false);
      fmt::print("res size: {}\nexpr size: {}\n", res_size, exp_size);
      fmt::print("res:\n");
      print(*res_it);
      fmt::print("exp:\n");
      print(*exp_it);
    } else {
      CHECK(true);
    }
    ++batch_number;
    size_t event_number = 0;
    for (auto event : res_it->values()) {
      REQUIRE(exp_event_it != exp_it->end());
      if (event != *exp_event_it) {
        CHECK(false);
        fmt::print("Event mismatch in batch {}, event {}\n", batch_number,
                   event_number);
        ++event_number;
        fmt::print("Got: {}\nExp: {}\n", event, *exp_event_it);
      } else {
        CHECK(true);
      }
      ++exp_event_it;
    }
  }
  CHECK_EQUAL(res_it, res.end());
  CHECK_EQUAL(exp_it, expected.end());
}

TEST(empty builder) {
  multi_series_builder b{
    multi_series_builder::policy_merge{},
    multi_series_builder::settings_type{},
  };
  CHECK_EQUAL(b.last_errors().size(), 0);
  CHECK_EQUAL(b.yield_ready().size(), 0);
}

TEST(merging records) {
  multi_series_builder b{
    multi_series_builder::policy_merge{},
    multi_series_builder::settings_type{},
  };
  b.record().exact_field("0").data(0l);
  b.record().exact_field("0").data(1l);
  b.record().exact_field("1").data(2.0);
  const auto res = b.finalize();
  CHECK_EQUAL(res.size(), 1); // merging should produce exactly one series here

  const type expected_type{record_type{
    {"0", int64_type{}},
    {"1", double_type{}},
  }};
  CHECK_EQUAL(res.front().type, expected_type);
  const vvr expected_result = {
    {
      {
        {"0", 0l},
        {"1", caf::none},
      },
      {
        {"0", 1l},
        {"1", caf::none},
      },
      {
        {"0", caf::none},
        {"1", 2.0},
      },
    },
  };
  check_outcome(res, expected_result);

  {
    auto r = b.record();
    r.exact_field("0").data(0l);
    r.exact_field("1").data(0.0);
  }
  const auto res2 = b.finalize();

  const vvr expected_result2 = {
    {
      {
        {"0", 0l},
        {"1", 0.0},
      },
    },
  };
  check_outcome(res2, expected_result2);
}

TEST(merging records with seed and reset) {
  const type seed_schema = {
    "seed",
    record_type{
      {"0", int64_type{}},
      {"1", double_type{}},
    },
  };
  multi_series_builder b{
    multi_series_builder::policy_merge{
      .seed_schema = "seed",
      .reset_on_yield = true,
    },
    multi_series_builder::settings_type{},
    {seed_schema},
  };
  b.record().exact_field("0").data(0l);
  b.record().exact_field("2").data(0ul);
  const auto res = b.finalize();
  CHECK_EQUAL(res.size(), 1); // merging should produce exactly one series here

  const type expected_type{
    "seed",
    record_type{
      {"0", int64_type{}},
      {"1", double_type{}},
      {"2", uint64_type{}},
    },
  };

  CHECK_EQUAL(res.front().type, expected_type);

  const vvr expected_result = {
    {
      {
        {"0", 0l},
        {"1", caf::none},
        {"2", caf::none},
      },
      {
        {"0", caf::none},
        {"1", caf::none},
        {"2", 0ul},
      },
    },
  };
  check_outcome(res, expected_result);

  {
    auto r = b.record();
    r.exact_field("1").data(0.0);
  }
  const auto res2 = b.finalize();
  CHECK_EQUAL(res2.front().type, seed_schema);

  const vvr expected_result2 = {
    {
      {
        {"0", caf::none},
        {"1", 0.0},
      },
    },
  };
  check_outcome(res2, expected_result2);
}

TEST(precise ordered) {
  multi_series_builder b{
    multi_series_builder::policy_precise{},
    multi_series_builder::settings_type{},
  };
  // first schema
  b.record().exact_field("0").data(0l);
  // second schema
  b.record().exact_field("2").data(1ul);
  b.record().exact_field("2").data(2ul);
  b.record().exact_field("2").data(3ul);
  const auto res = b.finalize();

  const vvr expected_result = {
    {
      {
        {"0", 0l},
      },
    },
    {
      {
        {"2", 1ul},
      },
      {
        {"2", 2ul},
      },
      {
        {"2", 3ul},
      },
    },
  };
  check_outcome(res, expected_result);
}

TEST(precise unordered) {
  multi_series_builder b{
    multi_series_builder::policy_precise{},
    multi_series_builder::settings_type{
      .ordered = false,
    },
  };
  // first schema
  b.record().exact_field("0").data(0l);
  // second schema
  b.record().exact_field("1").data(0ul);
  b.record().exact_field("1").data(1ul);
  // first schema again
  b.record().exact_field("0").data(1l);
  b.record().exact_field("0").data(2l);
  // third schema
  b.record().exact_field("2").data(0.0);
  b.record().exact_field("2").data(1.0);
  // second schema again
  b.record().exact_field("1").data(2ul);
  // third schema again
  b.record().exact_field("2").data(2.0);
  const auto res = b.finalize();

  const vvr expected_result = {
    {
      {
        {"0", 0l},
      },
      {
        {"0", 1l},
      },
      {
        {"0", 2l},
      },
    },
    {
      {
        {"1", 0ul},
      },
      {
        {"1", 1ul},
      },
      {
        {"1", 2ul},
      },
    },
    {
      {
        {"2", 0.0},
      },
      {
        {"2", 1.0},
      },
      {
        {"2", 2.0},
      },
    },
  };
  check_outcome(res, expected_result);
}

TEST(precise unordered with seed) {
  const type seed_schema = {
    "seed",
    record_type{
      {"0", int64_type{}},
      {"1", double_type{}},
    },
  };
  multi_series_builder b{
    multi_series_builder::policy_precise{.seed_schema = "seed"},
    multi_series_builder::settings_type{.ordered = false},
    {seed_schema},
  };
  // seed schema only
  b.record().exact_field("0").data(0l);
  b.record().exact_field("1").data(1.0);
  {
    auto r = b.record();
    r.exact_field("0").data(2l);
    r.exact_field("1").data(2.0);
  }
  // outside schema with extended fields
  {
    auto r = b.record();
    r.exact_field("0").data(0l);
    r.exact_field("2").data(0ul);
  }
  b.record().exact_field("2").data(
    1ul); // this should land in the same batch as it
          // has the seed for both seed fields
  // outside of schema only
  { b.record().exact_field("3").data(duration{}); }
  // schema only again
  (void)b.record();

  const auto res = b.finalize();

  const vvr expected_result = {
    {
      {
        {"0", 0l},
        {"1", caf::none},
      },
      {
        {"0", caf::none},
        {"1", 1.0},
      },
      {
        {"0", 2l},
        {"1", 2.0},
      },
      {
        {"0", caf::none},
        {"1", caf::none},
      },
    },
    {
      {
        {"0", 0l},
        {"1", caf::none},
        {"2", 0ul},
      },
      {
        {"0", caf::none},
        {"1", caf::none},
        {"2", 1ul},
      },
    },
    {
      {
        {"0", caf::none},
        {"1", caf::none},
        {"3", duration{}},
      },
    },
  };

  // print(res);
  // print(expected_result);
  check_outcome(res, expected_result);
}

TEST(selector) {
  const type seed_schema_1 = {
    "prefix.seed1",
    record_type{
      {"0", uint64_type{}},
      {"1", double_type{}},
    },
  };
  const type seed_schema_2 = {
    "prefix.seed2",
    record_type{
      {"0", int64_type{}},
      {"1", time_type{}},
    },
  };
  multi_series_builder b{
    multi_series_builder::policy_selector{.field_name = "selector",
                                          .naming_prefix = "prefix"},
    multi_series_builder::settings_type{},
    {seed_schema_1, seed_schema_2},
  };
  {
    auto r = b.record();
    r.exact_field("selector").data("seed1"s);
    r.exact_field("1").data(0.0);
  }
  {
    auto r = b.record();
    r.exact_field("selector").data("seed1"s);
    r.exact_field("0").data(1ul);
    r.exact_field("1").data(1.0);
  }
  {
    auto r = b.record();
    r.exact_field("selector").data("seed2"s);
    r.exact_field("1").data(time{});
  }

  const auto res = b.finalize();

  const vvr expected_result = {
    {
      {
        {"0", caf::none},
        {"1", 0.0},
        {"selector", "seed1"},
      },
      {
        {"0", 1ul},
        {"1", 1.0},
        {"selector", "seed1"},
      },
    },
    {{
      {"0", caf::none},
      {"1", time{}},
      {"selector", "seed2"s},
    }},
  };

  // fmt::print("res:\n");
  // print(res);
  // fmt::print("exp:\n");
  // print(expected_result);
  check_outcome(res, expected_result);
}

TEST(selector unordered) {
  const type seed_schema_1 = {
    "prefix.seed1",
    record_type{
      {"0", uint64_type{}},
      {"1", double_type{}},
    },
  };
  const type seed_schema_2 = {
    "prefix.seed2",
    record_type{
      {"0", int64_type{}},
      {"1", time_type{}},
    },
  };
  multi_series_builder b{
    multi_series_builder::policy_selector{.field_name = "selector",
                                          .naming_prefix = "prefix"},
    multi_series_builder::settings_type{.ordered = false},
    {seed_schema_1, seed_schema_2},
  };
  {
    auto r = b.record();
    r.exact_field("selector").data("seed1"s);
    r.exact_field("1").data(0.0);
  }
  {
    auto r = b.record();
    r.exact_field("selector").data("seed2"s);
    r.exact_field("1").data(time{});
  }
  {
    auto r = b.record();
    r.exact_field("selector").data("seed1"s);
    r.exact_field("0").data(1ul);
    r.exact_field("1").data(1.0);
  }
  {
    auto r = b.record();
    r.exact_field("selector").data("seed2"s);
    r.exact_field("0").data(1l);
  }

  const auto res = b.finalize();

  const vvr expected_result = {
    {
      {
        {"0", caf::none},
        {"1", 0.0},
        {"selector", "seed1"},
      },
      {
        {"0", 1ul},
        {"1", 1.0},
        {"selector", "seed1"},
      },
    },
    {
      {
        {"0", caf::none},
        {"1", time{}},
        {"selector", "seed2"s},
      },
      {
        {"0", 1l},
        {"1", caf::none},
        {"selector", "seed2"s},
      },
    },
  };

  // fmt::print("res:\n");
  // print(res);
  // fmt::print("exp:\n");
  // print(expected_result);
  check_outcome(res, expected_result);
}

TEST(selector unordered schema_only) {
  const type seed_schema_1 = {
    "prefix.seed1",
    record_type{
      {"0", uint64_type{}},
      {"1", double_type{}},
    },
  };
  const type seed_schema_2 = {
    "prefix.seed2",
    record_type{
      {"0", int64_type{}},
      {"1", time_type{}},
    },
  };
  multi_series_builder b{
    multi_series_builder::policy_selector{
      .field_name = "selector",
      .naming_prefix = "prefix",
    },
    multi_series_builder::settings_type{
      .ordered = false,
      .schema_only = true,
    },
    {seed_schema_1, seed_schema_2},
  };
  {
    auto r = b.record();
    r.exact_field("selector").data("seed1"s);
    r.exact_field("1").data(0.0);
    r.exact_field("no").data(0.0);
  }
  {
    auto r = b.record();
    r.exact_field("selector").data("seed2"s);
    r.exact_field("1").data(time{});
    r.exact_field("no").data(0.0);
  }
  {
    auto r = b.record();
    r.exact_field("selector").data("seed1"s);
    r.exact_field("0").data(1ul);
    r.exact_field("1").data(1.0);
    r.exact_field("no").data(0.0);
  }
  {
    auto r = b.record();
    r.exact_field("selector").data("seed2"s);
    r.exact_field("0").data(1l);
  }

  const auto res = b.finalize();

  const vvr expected_result = {
    {
      {
        {"0", caf::none},
        {"1", 0.0},
      },
      {
        {"0", 1ul},
        {"1", 1.0},
      },
    },
    {
      {
        {"0", caf::none},
        {"1", time{}},
      },
      {
        {"0", 1l},
        {"1", caf::none},
      },
    },
  };

  // fmt::print("res:\n");
  // print(res);
  // fmt::print("exp:\n");
  // print(expected_result);
  check_outcome(res, expected_result);
}

} // namespace
} // namespace tenzir
