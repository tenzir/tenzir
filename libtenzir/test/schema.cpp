//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/concept/parseable/tenzir/schema.hpp"

#include "tenzir/concept/parseable/to.hpp"
#include "tenzir/concept/printable/stream.hpp"
#include "tenzir/concept/printable/tenzir/data.hpp"
#include "tenzir/concept/printable/tenzir/error.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/detail/legacy_deserialize.hpp"
#include "tenzir/detail/serialize.hpp"
#include "tenzir/module.hpp"
#include "tenzir/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace tenzir;

using caf::get;
using caf::get_if;
using caf::holds_alternative;
using namespace std::string_literals;

TEST(offset finding) {
  std::string str = R"__(
    type a = int64
    type inner = record{ x: int64, y: double }
    type middle = record{ a: int64, b: inner }
    type outer = record{ a: middle, b: record { y: string }, c: int64 }
    type foo = record{ a: int64, b: double, c: outer, d: middle }
  )__";
  auto mod = unbox(to<module>(str));
  auto* foo_type = mod.find("foo");
  REQUIRE_NOT_EQUAL(foo_type, nullptr);
  REQUIRE(holds_alternative<record_type>(*foo_type));
  const auto& foo_record = get<record_type>(*foo_type);
  CHECK_EQUAL(foo_record.num_fields(), 4u);
  CHECK_EQUAL(foo_record.field(offset{0}).type, int64_type{});
  CHECK_EQUAL(foo_record.field(offset{1}).type, double_type{});
  CHECK_EQUAL(foo_record.field(offset{2}).name, "c");
  CHECK(is<record_type>(foo_record.field(offset{2}).type));
  CHECK_EQUAL(as<record_type>(foo_record.field(offset{2}).type).num_fields(),
              3u);
  CHECK_EQUAL(foo_record.field(offset{2, 0}).name, "a");
  CHECK_EQUAL(foo_record.field(offset{2, 1, 0}).type, string_type{});
  CHECK_EQUAL(foo_record.field(offset{2, 2}).type, int64_type{});
  CHECK_EQUAL(foo_record.field(offset{3}).name, "d");
  CHECK_EQUAL(foo_record.field(offset{3, 0}).type, int64_type{});
  CHECK_EQUAL(foo_record.field(offset{3, 1}).name, "b");
  CHECK_EQUAL(foo_record.field(offset{3, 1, 0}).type, int64_type{});
  CHECK_EQUAL(foo_record.field(offset{3, 1, 1}).type, double_type{});
}

TEST(combining) {
  auto x = unbox(to<module>(R"__(
    type b = double
    type int_custom = int64
    type a = int_custom
  )__"));
  auto y = unbox(to<module>(R"__(
    type c = ip
    type d = string
  )__"));
  auto z = module::combine(x, y);
  CHECK_EQUAL(unbox(z.find("a")),
              (type{"a", type{"int_custom", int64_type{}}}));
  CHECK_EQUAL(unbox(z.find("b")), (type{"b", double_type{}}));
  CHECK_EQUAL(unbox(z.find("c")), (type{"c", ip_type{}}));
  CHECK_EQUAL(unbox(z.find("d")), (type{"d", string_type{}}));
  CHECK_EQUAL(unbox(z.find("int_custom")), (type{"int_custom", int64_type{}}));
}

TEST(merging) {
  std::string str = R"__(
    type a = int64
    type inner = record{ x: int64, y: double }
  )__";
  auto s1 = to<module>(str);
  REQUIRE(s1);
  str = "type a = int64\n" // Same type allowed.
        "type b = int64\n";
  auto s2 = to<module>(str);
  REQUIRE(s2);
  auto merged = module::merge(*s1, *s2);
  REQUIRE(merged);
  CHECK(merged->find("a"));
  CHECK(merged->find("b"));
  CHECK(merged->find("inner"));
}

TEST(serialization) {
  module mod;
  auto t = type{
    "foo",
    record_type{
      {"s1", string_type{}},
      {"d1", double_type{}},
      {"c", type{uint64_type{}, {{"skip"}}}},
      {"i", int64_type{}},
      {"s2", string_type{}},
      {"d2", double_type{}},
    },
  };
  mod.add(t);
  // Save & load
  caf::byte_buffer buf;
  CHECK(detail::serialize(buf, mod));
  module mod2;
  CHECK_EQUAL(detail::legacy_deserialize(buf, mod2), true);
  // Check integrity
  auto u = mod2.find("foo");
  REQUIRE(u);
  CHECK(t == *u);
}

TEST(parseable - simple sequential) {
  auto str = "type a = int64 type b = string type c = a"s;
  module mod;
  CHECK(parsers::module(str, mod));
  CHECK(mod.find("a"));
  CHECK(mod.find("b"));
  CHECK(mod.find("c"));
}

TEST(parseable - toplevel comments) {
  std::string_view str = R"__(
    // A comment at the beginning.
    type foo = int64
    // A comment a the end of the schema.
  )__";
  module mod;
  CHECK(parsers::module(str, mod));
  CHECK(mod.find("foo"));
}

TEST(parseable - inline comments) {
  std::string_view str = R"__(
    type foo = record{  // so
      ts: time,         // much
      uid: string       // more
    }                   // detail,
    type bar = int64    // jeez!
  )__";
  module mod;
  CHECK(parsers::module(str, mod));
  CHECK(mod.find("foo"));
  CHECK(mod.find("bar"));
}

TEST(module : zeek - style) {
  std::string str = R"__(
    type port = uint64
    type zeek.ssl = record{
      ts: time,
      uid: string,
      id: record {orig_h: ip, orig_p: uint64, resp_h: ip, resp_p: uint64},
      version: string,
      cipher: string,
      server_name: string,
      session_id: string,
      subject: string,
      issuer_subject: string,
      not_valid_before: time,
      not_valid_after: time,
      last_alert: string,
      client_subject: string,
      client_issuer_subject: string
    }
  )__";
  module mod;
  CHECK(parsers::module(str, mod));
  auto ssl = mod.find("zeek.ssl");
  REQUIRE(ssl);
  auto r = get_if<record_type>(ssl);
  REQUIRE(r);
  auto id = r->resolve_key("id");
  REQUIRE(id);
  CHECK(holds_alternative<record_type>(r->field(*id).type));
}

TEST(schema : aliases) {
  auto str = R"__(
               type foo = ip
               type bar = foo
               type baz = bar
               type x = baz
             )__";
  module mod;
  CHECK(parsers::module(std::string{str}, mod));
  auto foo = mod.find("foo");
  REQUIRE(foo);
  CHECK(holds_alternative<ip_type>(*foo));
  CHECK(mod.find("bar"));
  CHECK(mod.find("baz"));
  CHECK(mod.find("x"));
}

TEST(parseable - basic types global) {
  auto str = R"__(
    type t1 = bool
    type t2 = int64
    type t3 = uint64
    type t4 = double
    type t5 = duration
    type t6 = time
    type t7 = string
    type t8 = string
    type t9 = ip
    type t10 = subnet
    type foo = record{
      a1: t1,
      a2: t2,
      a3: t3,
      a4: t4,
      a5: t5,
      a6: t6,
      a7: t7,
      a8: t8,
      a9: t9,
      a10: t10,
    }
  )__";
  module mod;
  CHECK(parsers::module(std::string{str}, mod));
  CHECK(mod.find("t1"));
  CHECK(mod.find("t10"));
  auto foo = mod.find("foo");
  REQUIRE(foo);
  auto r = get_if<record_type>(foo);
  REQUIRE(r);
  auto t8 = r->resolve_key("a8");
  REQUIRE(t8);
  CHECK(holds_alternative<string_type>(r->field(*t8).type));
}

TEST(parseable - basic types local) {
  auto str = R"__(
    type foo = record{
      a1: bool,
      a2: int64,
      a3: uint64,
      a4: double,
      a5: duration,
      a6: time,
      a7: string,
      a8: string,
      a9: ip,
      a10: subnet,
    }
  )__";
  module mod;
  CHECK(parsers::module(std::string{str}, mod));
  auto foo = mod.find("foo");
  REQUIRE(foo);
  auto r = get_if<record_type>(foo);
  REQUIRE(r);
  auto p = r->resolve_key("a10");
  REQUIRE(p);
  CHECK(holds_alternative<subnet_type>(r->field(*p).type));
}

TEST(parseable - complex types global) {
  auto str = R"__(
    type enum_t = enum{x, y, z}
    type list_t = list<ip>
    type foo = record{
      e: enum_t,
      v: list_t,
    }
  )__";
  module mod;
  CHECK(parsers::module(std::string{str}, mod));
  auto enum_t = mod.find("enum_t");
  REQUIRE(enum_t);
  CHECK(mod.find("list_t"));
  auto foo = mod.find("foo");
  REQUIRE(foo);
  auto r = get_if<record_type>(foo);
  REQUIRE(r);
  auto e = r->resolve_key("e");
  REQUIRE(e);
  CHECK(r->field(*e).type == *enum_t);
}

TEST(parseable - out of order definitions) {
  using namespace std::string_view_literals;
  auto str = R"__(
    type baz = list<bar>
    type bar = record{
      x: foo
    }
    type foo = int64
  )__"sv;
  module mod;
  CHECK(parsers::module(str, mod));
  auto baz = unbox(mod.find("baz"));
  auto expected = type{
    "baz",
    list_type{
      type{
        "bar",
        record_type{
          {"x", type{"foo", int64_type{}}},
        },
      },
    },
    {{"skip"}},
  };
  CHECK_EQUAL(baz, expected);
}

TEST(parseable - with context) {
  using namespace std::string_view_literals;
  MESSAGE("prepare the context");
  auto global = symbol_map{};
  {
    auto local = symbol_map{};
    auto p = symbol_map_parser{};
    CHECK(p("type foo = uint64", local));
    global = std::move(local);
  }
  {
    MESSAGE("Use definition from global symbol table");
    auto str = R"__(
      type bar = record{
        x: record{
          y: foo
        }
      }
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto bar = unbox(sch.find("bar"));
    auto expected = type{
      "bar",
      record_type{
        {"x",
         record_type{
           {"y", type{"foo", uint64_type{}}},
         }},
      },
    };
    CHECK_EQUAL(bar, expected);
  }
  {
    MESSAGE("Override definition in global symbol table - before use");
    auto str = R"__(
      type foo = int64
      type bar = record{
        x: record{
          y: foo
        }
      }
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto bar = unbox(sch.find("bar"));
    auto expected = type{
      "bar",
      record_type{
        {"x",
         record_type{
           {"y", type{"foo", int64_type{}}},
         }},
      },
    };
    CHECK_EQUAL(bar, expected);
  }
  {
    MESSAGE("Override definition in global symbol table - after use");
    auto str = R"__(
      type bar = record{
        x: record{
          y: foo
        }
      }
      type foo = int64
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto bar = unbox(sch.find("bar"));
    auto expected = type{
      "bar",
      record_type{
        {"x",
         record_type{
           {"y", type{"foo", int64_type{}}},
         }},
      },
    };
    CHECK_EQUAL(bar, expected);
  }
  {
    MESSAGE("Duplicate definition error");
    auto str = R"__(
      type foo = double
      type bar = record{
        x: record{
          y: foo
        }
      }
      type foo = int64
    )__"sv;
    auto p = symbol_map_parser{};
    symbol_map sm;
    CHECK(!p(str, sm));
  }
  {
    MESSAGE("Duplicate definition error - re-entry");
    auto p = symbol_map_parser{};
    symbol_map sm;
    CHECK(p("type foo = double", sm));
    CHECK(!p("type foo = int64", sm));
  }
  {
    MESSAGE("Arithmetic - basic addition");
    auto str = R"__(
      type foo = record{
        x: int64
      }
      type bar = record{
        y: int64
      }
      type gob = foo + bar + tar
      type tar = record{
        z: int64
      }
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto gob = unbox(sch.find("gob"));
    auto expected = type{
      "gob",
      record_type{
        {"x", int64_type{}},
        {"y", int64_type{}},
        {"z", int64_type{}},
      },
    };
    CHECK_EQUAL(gob, expected);
  }
  {
    MESSAGE("Arithmetic - field clash");
    auto str = R"__(
      type foo = record{
        a: int64,
        b: int64
      }
      type bar = record{
        a: double,
        c: double
      }
      type lplus = foo + bar
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    CHECK(!r.resolve());
  }
  {
    MESSAGE("Arithmetic - priorities");
    auto str = R"__(
      type foo = record{
        a: int64,
        b: int64
      } #attr_one #attr_two=val
      type bar = record{
        a: double,
        c: double
      } #attr_one=val #attr_two
      type lplus = foo <+ bar
      type rplus = foo +> bar
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto expected_lplus = type{
      "lplus",
      record_type{
        {"a", int64_type{}},
        {"b", int64_type{}},
        {"c", double_type{}},
      },
      {
        {"attr_one"},
        {"attr_two", "val"},
      },
    };
    auto expected_rplus = type{
      "rplus",
      record_type{
        {"a", double_type{}},
        {"b", int64_type{}},
        {"c", double_type{}},
      },
      {
        {"attr_one", "val"},
        {"attr_two"},
      },
    };
    auto lplus = unbox(sch.find("lplus"));
    CHECK_EQUAL(lplus, expected_lplus);
    auto rplus = unbox(sch.find("rplus"));
    CHECK_EQUAL(rplus, expected_rplus);
  }
  {
    MESSAGE("Arithmetic - removing multiple fields");
    auto str = R"__(
      type foo = record{
        a: record{
          x: uint64,
          y: record {
            z: list<string>
          }
        },
        "b.c": record {
          d: uint64,
          e: uint64
        },
        f: record {
          g: uint64
        }
      }
      type bar = foo - a.y - "b.c".d - f.g
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto bar = unbox(sch.find("bar"));
    auto expected = type{
      "bar",
      record_type{
        {"a",
         record_type{
           {"x", uint64_type{}},
         }},
        {"b.c",
         record_type{
           {"e", uint64_type{}},
         }},
      },
    };
    CHECK_EQUAL(bar, expected);
  }
  {
    MESSAGE("Arithmetic - realistic usage");
    auto str = R"__(
      type base = record{
        a: record{
             x: uint64,
             y: string
           },
        b: int64,
        c: int64,
      }
      type derived1 = base - c +> record{
        a: record {
             y: ip
           },
        b: double,
        d: time,
      }
      type derived2 = base +> record{
        a: record {
             y: ip
           },
        b: double,
        d: time,
      } - c
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto derived1 = unbox(sch.find("derived1"));
    auto derived2 = unbox(sch.find("derived2"));
    auto expected = type{
      "derived1",
      record_type{
        {"a",
         record_type{
           {"x", uint64_type{}},
           {"y", ip_type{}},
         }},
        {"b", double_type{}},
        {"d", time_type{}},
      },
    };
    CHECK_EQUAL(derived1, expected);
    auto rename = [&]<concrete_type T>(const T& x) -> type {
      return type{"derived2", x};
    };
    CHECK_EQUAL(caf::visit(rename, expected), derived2);
  }
}

TEST(parseable - overwriting with self reference) {
  using namespace std::string_view_literals;
  auto global = symbol_map{};
  {
    auto local = symbol_map{};
    auto p = symbol_map_parser{};
    CHECK(p("type foo = record{\"x\": uint64}", local));
    global = std::move(local);
  }
  {
    auto str = R"__(
      type bar = foo
      type foo = foo + record {
        y: string
      }
    )__"sv;
    auto sm = unbox(to<symbol_map>(str));
    auto r = symbol_resolver{global, sm};
    auto sch = unbox(r.resolve());
    auto foo = unbox(sch.find("foo"));
    auto expected = type{
      "foo",
      record_type{
        {"x", uint64_type{}},
        {"y", string_type{}},
      },
    };
    CHECK_EQUAL(foo, expected);
    auto bar = unbox(sch.find("bar"));
    expected.assign_metadata(type{"bar", type{}});
    CHECK_EQUAL(bar, expected);
  }
}
