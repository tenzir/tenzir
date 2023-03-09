//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/collect.hpp"
#include "vast/plugin.hpp"
#include "vast/table_slice.hpp"
#include "vast/table_slice_builder.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>
using namespace vast;

namespace {

// Builds a chain of events where consecutive chunks of
// num_events_per_type events have the same type.
struct basic_table_slice_generator {
  id offset;

  explicit basic_table_slice_generator(type input_schema)
    : offset(0), schema(input_schema) {
  }

  table_slice operator()(size_t num) {
    auto builder = std::make_shared<table_slice_builder>(schema);
    for (size_t i = 0; i < num; ++i) {
      CHECK(builder->add(make_data_view("foo")));
    }
    auto slice = builder->finish();
    slice.offset(offset);
    offset += num;
    return slice;
  }

  type schema;
};

struct fixture {
  struct mock_control_plane : operator_control_plane {
    [[nodiscard]] virtual auto self() noexcept -> caf::event_based_actor& {
      FAIL("no mock implementation available");
    }
    virtual auto stop([[maybe_unused]] caf::error error = {}) noexcept -> void {
      FAIL("no mock implementation available");
    }

    virtual auto warn([[maybe_unused]] caf::error warning) noexcept -> void {
      FAIL("no mock implementation available");
    }

    virtual auto emit([[maybe_unused]] table_slice metrics) noexcept -> void {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] virtual auto
    demand([[maybe_unused]] type schema = {}) const noexcept -> size_t {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] virtual auto schemas() const noexcept
      -> const std::vector<type>& {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] virtual auto concepts() const noexcept
      -> const concepts_map& {
      FAIL("no mock implementation available");
    }
  };

  fixture() {
    // TODO: Move this into a separate fixture when we are starting to test more
    // than one printer type.
    printer_plugin = vast::plugins::find<vast::printer_plugin>("json");
    REQUIRE(printer_plugin);
  }

  generator<table_slice>
  generate_basic_table_slices(int slices, int slice_columns,
                              basic_table_slice_generator& g) {
    for (auto i = 0; i < slices; ++i) {
      co_yield g(slice_columns);
    }
    co_return;
  }

  const vast::printer_plugin* printer_plugin;
  mock_control_plane control_plane;
};

} // namespace

FIXTURE_SCOPE(printer_plugin_tests, fixture)

TEST(json printer - singular slice - singular column) {
  auto schema = type{
    "stub",
    record_type{
      {"content", string_type{}},
    },
  };
  basic_table_slice_generator g(std::move(schema));
  auto current_printer
    = unbox(printer_plugin->make_printer({}, g.schema, control_plane));
  auto str = std::string{R"({"content": "foo"}
)"};
  auto chunks = collect(current_printer(generate_basic_table_slices(1, 1, g)));
  REQUIRE_EQUAL(chunks.size(), size_t{1});
  auto str_chunk = chunk::copy(str);
  REQUIRE(std::equal(chunks.front()->begin(), chunks.front()->end(),
                     str_chunk->begin(), str_chunk->end()));
}

TEST(json printer - multiple slices - singular column) {
  auto schema = type{
    "stub",
    record_type{
      {"content", string_type{}},
    },
  };
  basic_table_slice_generator g(std::move(schema));
  auto current_printer
    = unbox(printer_plugin->make_printer({}, g.schema, control_plane));
  auto strs = std::vector<std::string>{
    R"({"content": "foo"}
)",
    R"({"content": "foo"}
)",
    R"({"content": "foo"}
)"};
  auto chunks = collect(current_printer(generate_basic_table_slices(3, 1, g)));
  REQUIRE_EQUAL(chunks.size(), size_t{3});
  for (auto i = size_t{0}; i < chunks.size(); ++i) {
    auto str_chunk = chunk::copy(strs[i]);
    auto chunk = chunks[i];
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

TEST(json printer - singular slice - multiple columns) {
  auto schema = type{
    "stub",
    record_type{{"content", string_type{}},
                {"content2", string_type{}},
                {"content3", string_type{}}},
  };
  basic_table_slice_generator g(std::move(schema));
  auto current_printer
    = unbox(printer_plugin->make_printer({}, g.schema, control_plane));
  auto str = std::string{
    R"({"content": "foo", "content2": "foo", "content3": "foo"}
)"};
  auto chunks = collect(current_printer(generate_basic_table_slices(1, 3, g)));
  REQUIRE_EQUAL(chunks.size(), size_t{1});
  auto str_chunk = chunk::copy(str);
  REQUIRE(std::equal(chunks.front()->begin(), chunks.front()->end(),
                     str_chunk->begin(), str_chunk->end()));
}

TEST(json printer - multiple slices - multiple columns) {
  auto schema = type{
    "stub",
    record_type{{"content", string_type{}},
                {"content2", string_type{}},
                {"content3", string_type{}}},
  };
  basic_table_slice_generator g(std::move(schema));
  auto current_printer
    = unbox(printer_plugin->make_printer({}, g.schema, control_plane));
  auto strs = std::vector<std::string>{
    R"({"content": "foo", "content2": "foo", "content3": "foo"}
)",
    R"({"content": "foo", "content2": "foo", "content3": "foo"}
)",
    R"({"content": "foo", "content2": "foo", "content3": "foo"}
)"};
  auto chunks = collect(current_printer(generate_basic_table_slices(3, 3, g)));
  REQUIRE_EQUAL(chunks.size(), size_t{3});
  for (auto i = size_t{0}; i < chunks.size(); ++i) {
    auto str_chunk = chunk::copy(strs[i]);
    auto chunk = chunks[i];
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

TEST(json printer - nested columns) {
  auto schema = record_type{
    {"f1", type{string_type{}, {{"key", "value"}}}},
    {"f2", type{"alt_name", uint64_type{}}},
    {
      "f3_rec",
      type{"nested", record_type{{"f3.1", type{"rgx", string_type{}}},
                                 {"f3.2", int64_type{}}}},
    },
  };
  auto slice_type = type{"rec", schema};
  auto builder = std::make_shared<table_slice_builder>(slice_type);
  CHECK(builder->add("n1", uint64_t{2}, "p1", int64_t{7}));
  auto first_slice = builder->finish();
  CHECK(builder->add("n2", uint64_t{3}, "p2", int64_t{222}));
  auto second_slice = builder->finish();
  auto slice_generator
    = [&first_slice, &second_slice]() -> generator<table_slice> {
    co_yield first_slice;
    co_yield second_slice;
    co_return;
  };
  auto current_printer
    = unbox(printer_plugin->make_printer({}, slice_type, control_plane));
  auto strs = std::vector<std::string>{
    R"({"f1": "n1", "f2": 2, "f3_rec": {"f3.1": "p1", "f3.2": 7}}
)",
    R"({"f1": "n2", "f2": 3, "f3_rec": {"f3.1": "p2", "f3.2": 222}}
)"};
  auto chunks = collect(current_printer(slice_generator()));
  REQUIRE_EQUAL(chunks.size(), size_t{2});
  for (auto i = size_t{0}; i < chunks.size(); ++i) {
    auto str_chunk = chunk::copy(strs[i]);
    auto chunk = chunks[i];
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

TEST(json printer - list type) {
  auto slice_type
    = type{"rec", record_type{{"list", list_type{uint64_type{}}}}};
  auto builder = std::make_shared<table_slice_builder>(slice_type);
  CHECK(builder->add(list{uint64_t{0}, uint64_t{1}, uint64_t{2}}));
  auto slice = builder->finish();
  auto slice_generator = [&slice]() -> generator<table_slice> {
    co_yield slice;
    co_return;
  };
  auto current_printer
    = unbox(printer_plugin->make_printer({}, slice_type, control_plane));
  auto str = std::string{R"({"list": [0, 1, 2]}
)"};
  auto chunks = collect(current_printer(slice_generator()));
  REQUIRE_EQUAL(chunks.size(), size_t{1});
  auto str_chunk = chunk::copy(str);
  REQUIRE(std::equal(chunks.front()->begin(), chunks.front()->end(),
                     str_chunk->begin(), str_chunk->end()));
}

TEST(json printer - uint64 type) {
  auto slice_type = type{"rec", record_type{{"foo", uint64_type{}}}};
  auto builder = std::make_shared<table_slice_builder>(slice_type);
  CHECK(builder->add(uint64_t{0}, uint64_t{1}, uint64_t{2}));
  auto slice = builder->finish();
  auto slice_generator = [&slice]() -> generator<table_slice> {
    co_yield slice;
    co_return;
  };
  auto current_printer
    = unbox(printer_plugin->make_printer({}, slice_type, control_plane));
  auto strs = std::vector<std::string>{
    R"({"foo": 0}
)",
    R"({"foo": 1}
)",
    R"({"foo": 2}
)"};
  auto chunks = collect(current_printer(slice_generator()));
  REQUIRE_EQUAL(chunks.size(), size_t{3});
  for (auto i = size_t{0}; i < chunks.size(); ++i) {
    auto str_chunk = chunk::copy(strs[i]);
    auto chunk = chunks[i];
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

TEST(json printer - list of structs) {
  auto schema = record_type{
    {
      "foo",
      list_type{
        record_type{
          {"bar", uint64_type{}},
          {"baz", uint64_type{}},
        },
      },
    },
  };
  auto slice_type = type{"rec", schema};
  auto builder = std::make_shared<table_slice_builder>(slice_type);
  CHECK(builder->add(list{record{
                            {"bar", uint64_t{1}},
                            {"baz", uint64_t{2}},
                          },
                          record{
                            {"bar", uint64_t{3}},
                            {"baz", caf::none},
                          }}));
  auto first_slice = builder->finish();
  CHECK(builder->add(list{record{
                            {"bar", uint64_t{4}},
                            {"baz", uint64_t{5}},
                          },
                          record{
                            {"bar", uint64_t{6}},
                            {"baz", uint64_t{7}},
                          }}));
  auto second_slice = builder->finish();
  auto slice_generator
    = [&first_slice, &second_slice]() -> generator<table_slice> {
    co_yield first_slice;
    co_yield second_slice;
    co_return;
  };
  auto current_printer
    = unbox(printer_plugin->make_printer({}, slice_type, control_plane));
  auto strs = std::vector<std::string>{
    R"({"foo": [{"bar": 1, "baz": 2}, {"bar": 3, "baz": null}]}
)",
    R"({"foo": [{"bar": 4, "baz": 5}, {"bar": 6, "baz": 7}]}
)"};
  auto chunks = collect(current_printer(slice_generator()));
  REQUIRE_EQUAL(chunks.size(), size_t{2});
  for (auto i = size_t{0}; i < chunks.size(); ++i) {
    auto str_chunk = chunk::copy(strs[i]);
    auto chunk = chunks[i];
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

FIXTURE_SCOPE_END()
