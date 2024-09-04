//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/data_builder.hpp"

#include "tenzir/aliases.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/type.hpp"

#include <caf/detail/type_list.hpp>

#include <cstddef>
#include <cstdint>

namespace tenzir {
namespace {

using namespace detail::data_builder;
using namespace std::literals;

struct test_diagnsotic_handler : diagnostic_handler {
public:
  virtual void emit(diagnostic d) override {
    switch (d.severity) {
      using enum severity;
      case error:
        ++errors;
        return;
      case warning:
        ++warnings;
        return;
      case note:
        ++notes;
        return;
    }
  }
  operator size_t() const {
    return errors + warnings;
  }
  auto reset() -> void {
    *this = {};
  }

  size_t errors = 0;
  size_t warnings = 0;
  size_t notes = 0;
};


auto safe_as_record( tenzir::data d ) -> tenzir::record {
  auto as_record = caf::get_if<tenzir::record>(&d);
  REQUIRE( as_record );
  return std::move(*as_record);
}

TEST(empty) {
  data_builder b;

  CHECK(not b.has_elements());
}

TEST(materialization record) {
  data_builder b;
  auto* r = b.record();
  r->field("0")->data(uint64_t{0});
  r->field("1")->data(int64_t{1});
  r->field("2")->null();

  CHECK(b.has_elements());

  auto rec = safe_as_record( b.materialize() );
  auto expected = tenzir::record{};
  expected["0"] = uint64_t{0};
  expected["1"] = int64_t{1};
  expected["2"] = caf::none;
  for (const auto& [rk, rv] : rec) {
    CHECK(expected.at(rk) == rv);
  }
  CHECK(not b.has_elements());
}

TEST(materialization list) {
  data_builder b;
  auto* r = b.record();
  auto* l = r->field("int list")->list();
  l->data(uint64_t{0});
  l->data(uint64_t{1});
  l->data(uint64_t{2});

  CHECK(b.has_elements());

  auto rec = safe_as_record( b.materialize() );
  auto expected = tenzir::record{};
  expected["int list"] = tenzir::list{uint64_t{0}, uint64_t{1}, uint64_t{2}};
  for (const auto& [expected_key, expected_data] : expected) {
    CHECK(rec.at(expected_key) == expected_data);
  }
  CHECK(not b.has_elements());
}

TEST(materialization nested record) {
  data_builder b;
  auto* r = b.record();
  r->field("0")->record()->field("1")->null();

  CHECK(b.has_elements());

  auto rec = safe_as_record( b.materialize() );
  auto expected = tenzir::record{};
  expected["0"] = tenzir::record{{"1", caf::none}};
  for (const auto& [rk, rv] : rec) {
    CHECK(expected.at(rk) == rv);
  }
  CHECK(not b.has_elements());
}

TEST(materialization record list record) {
  data_builder b;
  auto* r = b.record();
  r->field("0")->list()->record()->field("1")->data(uint64_t{0});
  (void)r->field("1")->record()->field("0")->list();

  CHECK(b.has_elements());

  auto rec = safe_as_record( b.materialize(false) );
  auto expected = tenzir::record{};
  expected["0"] = tenzir::list{tenzir::record{{"1", uint64_t{0}}}};
  expected["1"] = tenzir::record{{"0", tenzir::list{}}};
  for (const auto& [rk, rv] : rec) {
    CHECK(expected.at(rk) == rv);
  }
  CHECK(b.has_elements());
  auto rec2 = safe_as_record( b.materialize() );
  CHECK(rec == rec2);
  CHECK(not b.has_elements());
}

TEST(overwrite record fields) {
  data_builder b;
  auto* r = b.record();
  r->field("0")->data(uint64_t{0});
  r->field("0")->data(int64_t{0});
  r->field("0")->data(0.0);
  r->field("0")->data(uint64_t{0});

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;

  b.append_signature_to(sig, nullptr);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    {
      const auto key_bytes = as_bytes("0"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, uint64_t>::value));
    }
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  //   fmt::print("{}\n", sig);
  //   fmt::print("{}\n", expected);
  CHECK(sig == expected);
}

TEST(signature record empty) {
  data_builder b;
  (void)b.record();

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;

  b.append_signature_to(sig, nullptr);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  CHECK(sig == expected);
}

TEST(signature record simple) {
  data_builder b;
  auto* r = b.record();
  r->field("0")->data(uint64_t{0});
  r->field("1")->data(int64_t{1});

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;

  b.append_signature_to(sig, nullptr);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    {
      const auto key_bytes = as_bytes("0"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, uint64_t>::value));
    }
    {
      const auto key_bytes = as_bytes("1"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, int64_t>::value));
    }
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  CHECK(sig == expected);
}

TEST(signature list) {
  data_builder b;
  auto* l = b.record()->field("l")->list();
  l->data(uint64_t{0});
  l->data(uint64_t{1});

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;

  b.append_signature_to(sig, nullptr);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    const auto key_bytes = as_bytes("l"sv);
    expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
    expected.insert(expected.end(), detail::data_builder::list_start_marker);
    expected.insert(
      expected.end(),
      static_cast<std::byte>(
        caf::detail::tl_index_of<field_type_list, uint64_t>::value));
    expected.insert(expected.end(), detail::data_builder::list_end_marker);
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  CHECK(sig == expected);
}

TEST(signature list with null) {
  data_builder b;
  auto* l = b.record()->field("l")->list();
  l->data(uint64_t{0});
  l->null();

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;

  b.append_signature_to(sig, nullptr);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    const auto key_bytes = as_bytes("l"sv);
    expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
    expected.insert(expected.end(), detail::data_builder::list_start_marker);
    expected.insert(
      expected.end(),
      static_cast<std::byte>(
        caf::detail::tl_index_of<field_type_list, uint64_t>::value));
    expected.insert(expected.end(), detail::data_builder::list_end_marker);
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  CHECK(sig == expected);
}

TEST(signature list numeric unification) {
  data_builder b;
  auto* l = b.record()->field("l")->list();
  l->data(uint64_t{0});
  l->data(1.0);

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;

  b.append_signature_to(sig, nullptr);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    const auto key_bytes = as_bytes("l"sv);
    expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
    expected.insert(expected.end(), detail::data_builder::list_start_marker);
    expected.insert(
      expected.end(),
      static_cast<std::byte>(detail::data_builder::type_index_double));
    expected.insert(expected.end(), detail::data_builder::list_end_marker);
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  // fmt::print("{}\n", sig);
  // fmt::print("{}\n", expected);
  CHECK(sig == expected);
}

TEST(signature record seeding matching) {
  data_builder b;
  auto* r = b.record();
  r->field("0")->data(uint64_t{0});
  r->field("1")->data(int64_t{1});

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;
  tenzir::type seed{record_type{
    {"0", uint64_type{}},
    {"1", int64_type{}},
  }};

  b.append_signature_to(sig, &seed);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    {
      const auto key_bytes = as_bytes("0"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, uint64_t>::value));
    }
    {
      const auto key_bytes = as_bytes("1"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, int64_t>::value));
    }
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  // fmt::print("{}\n", sig);
  // fmt::print("{}\n", expected);
  CHECK(sig == expected);
}

TEST(signature record seeding field not in data) {
  data_builder b;
  auto* r = b.record();
  r->field("0")->data(uint64_t{0});

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;
  tenzir::type seed{record_type{
    {"0", uint64_type{}},
    {"1", int64_type{}},
  }};

  b.append_signature_to(sig, &seed);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    {
      const auto key_bytes = as_bytes("0"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, uint64_t>::value));
    }
    {
      const auto key_bytes = as_bytes("1"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, int64_t>::value));
    }
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  // fmt::print("{}\n", sig);
  // fmt::print("{}\n", expected);
  CHECK(sig == expected);
}

TEST(signature record seeding field not in data-- no - extend - schema) {
  data_builder b{
    detail::data_builder::basic_parser,
    nullptr,
    true,
  };
  auto* r = b.record();
  r->field("0")->data(uint64_t{0});

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;
  tenzir::type seed{record_type{
    {"0", uint64_type{}},
    {"1", int64_type{}},
  }};

  b.append_signature_to(sig, &seed);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    {
      const auto key_bytes = as_bytes("0"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, uint64_t>::value));
    }
    {
      const auto key_bytes = as_bytes("1"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, int64_t>::value));
    }
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  // fmt::print("{}\n", sig);
  // fmt::print("{}\n", expected);
  CHECK(sig == expected);
}

TEST(signature record seeding data - field not in seed) {
  data_builder b;
  auto* r = b.record();
  r->field("1")->data(int64_t{0});
  r->field("0")->data(uint64_t{0});

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;
  tenzir::type seed{record_type{
    {"0", uint64_type{}},
  }};

  b.append_signature_to(sig, &seed);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    {
      const auto key_bytes = as_bytes("0"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, uint64_t>::value));
    }
    {
      const auto key_bytes = as_bytes("1"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, int64_t>::value));
    }
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  // fmt::print("{}\n", sig);
  // fmt::print("{}\n", expected);
  CHECK(sig == expected);
}

TEST(signature record seeding data - field not in seed-- no - extend - schema) {
  data_builder b{
    detail::data_builder::basic_parser,
    nullptr,
    true,
  };
  auto* r = b.record();
  r->field("1")->data(int64_t{0});
  r->field("0")->data(uint64_t{0});

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;
  tenzir::type seed{record_type{
    {"0", uint64_type{}},
  }};

  b.append_signature_to(sig, &seed);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    {
      const auto key_bytes = as_bytes("0"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, uint64_t>::value));
    }
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  // fmt::print("{}\n", sig);
  // fmt::print("{}\n", expected);
  CHECK(sig == expected);
}

TEST(signature record seeding numeric mismatch) {
  data_builder b;
  auto* r = b.record();
  r->field("0")->data(uint64_t{0});

  CHECK(b.has_elements());
  detail::data_builder::signature_type sig;
  tenzir::type seed{record_type{
    {"0", int64_type{}},
  }};
  // a strictly numeric mismatch does not return an error and is just handled by
  // casting to the seed type

  b.append_signature_to(sig, &seed);

  detail::data_builder::signature_type expected;
  {
    expected.insert(expected.end(),
                    detail::data_builder::record_start_marker);
    {
      const auto key_bytes = as_bytes("0"sv);
      expected.insert(expected.end(), key_bytes.begin(), key_bytes.end());
      expected.insert(
        expected.end(),
        static_cast<std::byte>(
          caf::detail::tl_index_of<field_type_list, int64_t>::value));
    }
    expected.insert(expected.end(), detail::data_builder::record_end_marker);
  }
  // fmt::print("{}\n", sig);
  // fmt::print("{}\n", expected);
  CHECK(sig == expected);
}
} // namespace
} // namespace tenzir
