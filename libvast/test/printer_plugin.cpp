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
#include "vast/test/fixtures/events.hpp"
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

struct fixture : fixtures::events {
  struct mock_control_plane final : operator_control_plane {
    auto self() noexcept -> system::execution_node_actor::base& override {
      FAIL("no mock implementation available");
    }

    auto node() noexcept -> system::node_actor override {
      FAIL("no mock implementation available");
    }

    auto abort(caf::error) noexcept -> void override {
      FAIL("no mock implementation available");
    }

    auto warn([[maybe_unused]] caf::error warning) noexcept -> void override {
      FAIL("no mock implementation available");
    }

    auto emit([[maybe_unused]] table_slice metrics) noexcept -> void override {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] auto schemas() const noexcept
      -> const std::vector<type>& override {
      FAIL("no mock implementation available");
    }

    [[nodiscard]] auto concepts() const noexcept
      -> const concepts_map& override {
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

  auto collect_chunks(std::function<generator<table_slice>()> slice_generator,
                      printer_plugin::printer current_printer)
    -> std::vector<chunk_ptr> {
    auto chunks = std::vector<chunk_ptr>{};
    for (auto&& x : slice_generator()) {
      auto chunks_per_slice = collect(current_printer->process(x));
      chunks.insert(chunks.end(), chunks_per_slice.begin(),
                    chunks_per_slice.end());
    }
    auto last = collect(current_printer->finish());
    chunks.insert(chunks.end(), last.begin(), last.end());
    return chunks;
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
  auto slice_generator = [this, &g]() -> generator<table_slice> {
    return generate_basic_table_slices(1, 1, g);
  };
  auto chunks
    = collect_chunks(std::move(slice_generator), std::move(current_printer));
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
  auto slice_generator = [this, &g]() -> generator<table_slice> {
    return generate_basic_table_slices(3, 1, g);
  };
  auto chunks
    = collect_chunks(std::move(slice_generator), std::move(current_printer));
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
  auto slice_generator = [this, &g]() -> generator<table_slice> {
    return generate_basic_table_slices(1, 3, g);
  };
  auto chunks
    = collect_chunks(std::move(slice_generator), std::move(current_printer));
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
  auto slice_generator = [this, &g]() -> generator<table_slice> {
    return generate_basic_table_slices(3, 3, g);
  };
  auto chunks
    = collect_chunks(std::move(slice_generator), std::move(current_printer));
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
  auto chunks
    = collect_chunks(std::move(slice_generator), std::move(current_printer));
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
  auto chunks
    = collect_chunks(std::move(slice_generator), std::move(current_printer));
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
{"foo": 1}
{"foo": 2}
)"};
  auto chunks
    = collect_chunks(std::move(slice_generator), std::move(current_printer));
  REQUIRE_EQUAL(chunks.size(), size_t{1});
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

  auto chunks
    = collect_chunks(std::move(slice_generator), std::move(current_printer));
  REQUIRE_EQUAL(chunks.size(), size_t{2});
  for (auto i = size_t{0}; i < chunks.size(); ++i) {
    auto str_chunk = chunk::copy(strs[i]);
    auto chunk = chunks[i];
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

TEST(json printer - suricata netflow) {
  auto slice_type = type{"rec", suricata_netflow_log.front().schema()};
  auto builder = std::make_shared<table_slice_builder>(slice_type);
  auto slice_generator = []() -> generator<table_slice> {
    co_yield suricata_netflow_log.front();
    co_return;
  };
  auto current_printer
    = unbox(printer_plugin->make_printer({}, slice_type, control_plane));
  auto str = std::string{
    R"({"timestamp": "2011-08-14T05:38:55.549713", "flow_id": 929669869939483, "pcap_cnt": null, "vlan": null, "in_iface": null, "src_ip": "147.32.84.165", "src_port": 138, "dest_ip": "147.32.84.255", "dest_port": 138, "proto": "UDP", "event_type": "netflow", "community_id": null, "netflow": {"pkts": 2, "bytes": 486, "start": "2011-08-12T12:53:47.928539", "end": "2011-08-12T12:53:47.928552", "age": 0}, "app_proto": "failed"}
)"};
  auto chunks
    = collect_chunks(std::move(slice_generator), std::move(current_printer));
  REQUIRE_EQUAL(chunks.size(), size_t{1});
  auto str_chunk = chunk::copy(str);
  REQUIRE(std::equal(chunks.front()->begin(), chunks.front()->end(),
                     str_chunk->begin(), str_chunk->end()));
}

TEST(json printer - zeek conn log) {
  auto slice_type = type{"rec", zeek_conn_log.front().schema()};
  auto builder = std::make_shared<table_slice_builder>(slice_type);
  auto slice_generator = []() -> generator<table_slice> {
    for (const auto& slice : zeek_conn_log) {
      co_yield slice;
    }
    co_return;
  };
  auto current_printer
    = unbox(printer_plugin->make_printer({}, slice_type, control_plane));
  auto strs = std::vector<std::string>{
    R"({"ts": "2009-11-18T08:00:21.486539", "uid": "Pii6cUUq1v4", "id.orig_h": "192.168.1.102", "id.orig_p": 68, "id.resp_h": "192.168.1.1", "id.resp_p": 67, "proto": "udp", "service": null, "duration": "163.82ms", "orig_bytes": 301, "resp_bytes": 300, "conn_state": "SF", "local_orig": null, "missed_bytes": 0, "history": "Dd", "orig_pkts": 1, "orig_ip_bytes": 329, "resp_pkts": 1, "resp_ip_bytes": 328, "tunnel_parents": []}
{"ts": "2009-11-18T08:08:00.237253", "uid": "nkCxlvNN8pi", "id.orig_h": "192.168.1.103", "id.orig_p": 137, "id.resp_h": "192.168.1.255", "id.resp_p": 137, "proto": "udp", "service": "dns", "duration": "3.78s", "orig_bytes": 350, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 7, "orig_ip_bytes": 546, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:08:13.816224", "uid": "9VdICMMnxQ7", "id.orig_h": "192.168.1.102", "id.orig_p": 137, "id.resp_h": "192.168.1.255", "id.resp_p": 137, "proto": "udp", "service": "dns", "duration": "3.75s", "orig_bytes": 350, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 7, "orig_ip_bytes": 546, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:07:15.800932", "uid": "bEgBnkI31Vf", "id.orig_h": "192.168.1.103", "id.orig_p": 138, "id.resp_h": "192.168.1.255", "id.resp_p": 138, "proto": "udp", "service": null, "duration": "46.73s", "orig_bytes": 560, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 3, "orig_ip_bytes": 644, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:08:13.825211", "uid": "Ol4qkvXOksc", "id.orig_h": "192.168.1.102", "id.orig_p": 138, "id.resp_h": "192.168.1.255", "id.resp_p": 138, "proto": "udp", "service": null, "duration": "2.25s", "orig_bytes": 348, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 2, "orig_ip_bytes": 404, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:10:03.872834", "uid": "kmnBNBtl96d", "id.orig_h": "192.168.1.104", "id.orig_p": 137, "id.resp_h": "192.168.1.255", "id.resp_p": 137, "proto": "udp", "service": "dns", "duration": "3.75s", "orig_bytes": 350, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 7, "orig_ip_bytes": 546, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:09:07.077011", "uid": "CFIX6YVTFp2", "id.orig_h": "192.168.1.104", "id.orig_p": 138, "id.resp_h": "192.168.1.255", "id.resp_p": 138, "proto": "udp", "service": null, "duration": "59.05s", "orig_bytes": 549, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 3, "orig_ip_bytes": 633, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:12:04.321413", "uid": "KlF6tbPUSQ1", "id.orig_h": "192.168.1.103", "id.orig_p": 68, "id.resp_h": "192.168.1.1", "id.resp_p": 67, "proto": "udp", "service": null, "duration": "44.78ms", "orig_bytes": 303, "resp_bytes": 300, "conn_state": "SF", "local_orig": null, "missed_bytes": 0, "history": "Dd", "orig_pkts": 1, "orig_ip_bytes": 331, "resp_pkts": 1, "resp_ip_bytes": 328, "tunnel_parents": []}
)",
    R"({"ts": "2009-11-18T08:12:19.613070", "uid": "tP3DM6npTdj", "id.orig_h": "192.168.1.102", "id.orig_p": 138, "id.resp_h": "192.168.1.255", "id.resp_p": 138, "proto": "udp", "service": null, "duration": null, "orig_bytes": null, "resp_bytes": null, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 1, "orig_ip_bytes": 229, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:14:06.693816", "uid": "Jb4jIDToo77", "id.orig_h": "192.168.1.104", "id.orig_p": 68, "id.resp_h": "192.168.1.1", "id.resp_p": 67, "proto": "udp", "service": null, "duration": "2.1ms", "orig_bytes": 311, "resp_bytes": 300, "conn_state": "SF", "local_orig": null, "missed_bytes": 0, "history": "Dd", "orig_pkts": 1, "orig_ip_bytes": 339, "resp_pkts": 1, "resp_ip_bytes": 328, "tunnel_parents": []}
{"ts": "2009-11-18T08:15:43.457078", "uid": "xvWLhxgUmj5", "id.orig_h": "192.168.1.102", "id.orig_p": 1170, "id.resp_h": "192.168.1.1", "id.resp_p": 53, "proto": "udp", "service": "dns", "duration": "68.51ms", "orig_bytes": 36, "resp_bytes": 215, "conn_state": "SF", "local_orig": null, "missed_bytes": 0, "history": "Dd", "orig_pkts": 1, "orig_ip_bytes": 64, "resp_pkts": 1, "resp_ip_bytes": 243, "tunnel_parents": []}
{"ts": "2009-11-18T08:16:43.657267", "uid": "feNcvrZfDbf", "id.orig_h": "192.168.1.104", "id.orig_p": 1174, "id.resp_h": "192.168.1.1", "id.resp_p": 53, "proto": "udp", "service": "dns", "duration": "170.96ms", "orig_bytes": 36, "resp_bytes": 215, "conn_state": "SF", "local_orig": null, "missed_bytes": 0, "history": "Dd", "orig_pkts": 1, "orig_ip_bytes": 64, "resp_pkts": 1, "resp_ip_bytes": 243, "tunnel_parents": []}
{"ts": "2009-11-18T08:18:51.365294", "uid": "aLsTcZJHAwa", "id.orig_h": "192.168.1.1", "id.orig_p": 5353, "id.resp_h": "224.0.0.251", "id.resp_p": 5353, "proto": "udp", "service": "dns", "duration": "100.38ms", "orig_bytes": 273, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 2, "orig_ip_bytes": 329, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:18:51.365329", "uid": "EK79I6iD5gl", "id.orig_h": "fe80::219:e3ff:fee7:5d23", "id.orig_p": 5353, "id.resp_h": "ff02::fb", "id.resp_p": 5353, "proto": "udp", "service": "dns", "duration": "100.37ms", "orig_bytes": 273, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 2, "orig_ip_bytes": 369, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:20:04.734263", "uid": "vLsf6ZHtak9", "id.orig_h": "192.168.1.103", "id.orig_p": 137, "id.resp_h": "192.168.1.255", "id.resp_p": 137, "proto": "udp", "service": "dns", "duration": "3.87s", "orig_bytes": 350, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 7, "orig_ip_bytes": 546, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:20:18.272516", "uid": "Su3RwTCaHL3", "id.orig_h": "192.168.1.102", "id.orig_p": 137, "id.resp_h": "192.168.1.255", "id.resp_p": 137, "proto": "udp", "service": "dns", "duration": "3.75s", "orig_bytes": 350, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 7, "orig_ip_bytes": 546, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
)",
    R"({"ts": "2009-11-18T08:20:04.859430", "uid": "rPM1dfJKPmj", "id.orig_h": "192.168.1.103", "id.orig_p": 138, "id.resp_h": "192.168.1.255", "id.resp_p": 138, "proto": "udp", "service": null, "duration": "2.26s", "orig_bytes": 348, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 2, "orig_ip_bytes": 404, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:20:56.089023", "uid": "4x5ezf34Rkh", "id.orig_h": "192.168.1.102", "id.orig_p": 1173, "id.resp_h": "192.168.1.1", "id.resp_p": 53, "proto": "udp", "service": "dns", "duration": "267.0us", "orig_bytes": 33, "resp_bytes": 497, "conn_state": "SF", "local_orig": null, "missed_bytes": 0, "history": "Dd", "orig_pkts": 1, "orig_ip_bytes": 61, "resp_pkts": 1, "resp_ip_bytes": 525, "tunnel_parents": []}
{"ts": "2009-11-18T08:20:18.281001", "uid": "mymcd8Veike", "id.orig_h": "192.168.1.102", "id.orig_p": 138, "id.resp_h": "192.168.1.255", "id.resp_p": 138, "proto": "udp", "service": null, "duration": "2.25s", "orig_bytes": 348, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 2, "orig_ip_bytes": 404, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
{"ts": "2009-11-18T08:22:05.592454", "uid": "07mJRfg5RU5", "id.orig_h": "192.168.1.1", "id.orig_p": 5353, "id.resp_h": "224.0.0.251", "id.resp_p": 5353, "proto": "udp", "service": "dns", "duration": "99.82ms", "orig_bytes": 273, "resp_bytes": 0, "conn_state": "S0", "local_orig": null, "missed_bytes": 0, "history": "D", "orig_pkts": 2, "orig_ip_bytes": 329, "resp_pkts": 0, "resp_ip_bytes": 0, "tunnel_parents": []}
)"};
  auto chunks
    = collect_chunks(std::move(slice_generator), std::move(current_printer));
  REQUIRE_EQUAL(chunks.size(), size_t{3});
  for (auto i = size_t{0}; i < chunks.size(); ++i) {
    auto str_chunk = chunk::copy(strs[i]);
    auto chunk = chunks[i];
    REQUIRE(std::equal(chunk->begin(), chunk->end(), str_chunk->begin(),
                       str_chunk->end()));
  }
}

FIXTURE_SCOPE_END()
