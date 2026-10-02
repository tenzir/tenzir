//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/nova_json_printer.hpp"

#include "tenzir/data.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/test/test.hpp"

#include <cmath>
#include <limits>
#include <string>
#include <vector>

using namespace tenzir;
using namespace std::chrono_literals;

namespace {

/// One row with every kind of value.
auto sample() -> data {
  auto const address = ip::v4(uint32_t{0x0a000001});
  return record{
    {"null", data{}},
    {"bool", true},
    {"int", int64_t{-42}},
    {"uint", uint64_t{42}},
    {"one", 1.0},
    {"big", 1e300},
    {"nan", std::numeric_limits<double>::quiet_NaN()},
    {"negative_zero", -0.0},
    {"fraction", 0.25},
    {"string", "quote\" backslash\\ newline\n tab\t ctrl\x01 utf8 \xc3\xa4"},
    {"blob", blob{std::byte{0x00}, std::byte{0x61}, std::byte{0xff}}},
    {"ip", address},
    {"subnet", subnet{address, 104}},
    {"time", tenzir::time{1'700'000'000s}},
    {"duration", duration{1500ms}},
    {"list", list{int64_t{1}, data{}, "two", list{}, record{}}},
    {"record", record{{"nested", record{{"x", int64_t{1}}, {"y", data{}}}},
                      {"not an identifier", "z"}}},
  };
}

auto print(json_printer_options options) -> std::string {
  auto dh = collecting_diagnostic_handler{};
  auto builder = nova::ArrayBuilder<nova::Data>{};
  nova::append_legacy_data(builder, sample(), dh);
  auto array = builder.finish();
  auto printer = nova::json_printer{std::move(options)};
  printer.print(array.get(0));
  auto const bytes = printer.bytes();
  return std::string{reinterpret_cast<char const*>(bytes.data()), bytes.size()};
}

auto plain(bool oneline) -> json_printer_options {
  return json_printer_options{.style = no_style(), .oneline = oneline};
}

} // namespace

TEST("print unstyled on one line") {
  CHECK_EQUAL(print(plain(true)),
              "{\"null\":null,\"bool\":true,\"int\":-42,\"uint\":42,\"one\":1."
              "0,\"big\":1e+300,\"nan\":null,\"negative_zero\":-0.0,"
              "\"fraction\":0.25,\"string\":\"quote\\\" backslash\\\\ "
              "newline\\n tab\\t ctrl\\u0001 utf8 \xc3"
              "\xa4"
              "\",\"blob\":\"AGH/\",\"ip\":\"10.0.0.1\",\"subnet\":\"10.0.0.0/"
              "8\",\"time\":\"2023-11-14T22:13:20Z\",\"duration\":\"1.5s\","
              "\"list\":[1,null,\"two\",[],{}],\"record\":{\"nested\":{\"x\":1,"
              "\"y\":null},\"not an identifier\":\"z\"}}");
}

TEST("print unstyled on several lines") {
  CHECK_EQUAL(print(plain(false)), "{\n"
                                   "  \"null\": null,\n"
                                   "  \"bool\": true,\n"
                                   "  \"int\": -42,\n"
                                   "  \"uint\": 42,\n"
                                   "  \"one\": 1.0,\n"
                                   "  \"big\": 1e+300,\n"
                                   "  \"nan\": null,\n"
                                   "  \"negative_zero\": -0.0,\n"
                                   "  \"fraction\": 0.25,\n"
                                   "  \"string\": \"quote\\\" backslash\\\\ "
                                   "newline\\n tab\\t ctrl\\u0001 utf8 \xc3"
                                   "\xa4"
                                   "\",\n"
                                   "  \"blob\": \"AGH/\",\n"
                                   "  \"ip\": \"10.0.0.1\",\n"
                                   "  \"subnet\": \"10.0.0.0/8\",\n"
                                   "  \"time\": \"2023-11-14T22:13:20Z\",\n"
                                   "  \"duration\": \"1.5s\",\n"
                                   "  \"list\": [\n"
                                   "    1,\n"
                                   "    null,\n"
                                   "    \"two\",\n"
                                   "    [],\n"
                                   "    {}\n"
                                   "  ],\n"
                                   "  \"record\": {\n"
                                   "    \"nested\": {\n"
                                   "      \"x\": 1,\n"
                                   "      \"y\": null\n"
                                   "    },\n"
                                   "    \"not an identifier\": \"z\"\n"
                                   "  }\n"
                                   "}");
}

TEST("print TQL") {
  auto options = plain(false);
  options.tql = true;
  CHECK_EQUAL(print(options), "{\n"
                              "  \"null\": null,\n"
                              "  bool: true,\n"
                              "  int: -42,\n"
                              "  uint: 42,\n"
                              "  one: 1.0,\n"
                              "  big: 1e+300,\n"
                              "  nan: null,\n"
                              "  negative_zero: -0.0,\n"
                              "  fraction: 0.25,\n"
                              "  string: \"quote\\\" backslash\\\\ newline\\n "
                              "tab\\t ctrl\\u0001 utf8 \xc3"
                              "\xa4"
                              "\",\n"
                              "  blob: b\"\\u0000a\xff"
                              "\",\n"
                              "  ip: 10.0.0.1,\n"
                              "  subnet: 10.0.0.0/8,\n"
                              "  time: 2023-11-14T22:13:20Z,\n"
                              "  duration: 1.5s,\n"
                              "  list: [\n"
                              "    1,\n"
                              "    null,\n"
                              "    \"two\",\n"
                              "    [],\n"
                              "    {},\n"
                              "  ],\n"
                              "  record: {\n"
                              "    nested: {\n"
                              "      x: 1,\n"
                              "      y: null,\n"
                              "    },\n"
                              "    \"not an identifier\": \"z\",\n"
                              "  },\n"
                              "}");
}

TEST("print TQL on one line") {
  auto options = plain(true);
  options.tql = true;
  CHECK_EQUAL(print(options),
              "{\"null\":null,bool:true,int:-42,uint:42,one:1.0,big:1e+300,nan:"
              "null,negative_zero:-0.0,fraction:0.25,string:\"quote\\\" "
              "backslash\\\\ newline\\n tab\\t ctrl\\u0001 utf8 \xc3"
              "\xa4"
              "\",blob:b\"\\u0000a\xff"
              "\",ip:10.0.0.1,subnet:10.0.0.0/"
              "8,time:2023-11-14T22:13:20Z,duration:1.5s,list:[1,null,\"two\",["
              "],{}],record:{nested:{x:1,y:null},\"not an "
              "identifier\":\"z\"}}");
}

TEST("print styled") {
  auto options = plain(true);
  options.style = jq_style();
  CHECK_EQUAL(print(options), "\x1b"
                              "[1m\x1b"
                              "[37m{\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"null\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[30mnull\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"bool\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[37mtrue\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"int\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[37m-42\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"uint\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[37m42\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"one\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m1.0\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"big\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m1e+300\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"nan\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[30mnull\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"negative_zero\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m-0.0\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"fraction\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m0.25\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"string\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[32m\"quote\\\" backslash\\\\ newline\\n tab\\t "
                              "ctrl\\u0001 utf8 \xc3"
                              "\xa4"
                              "\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"blob\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[32m\"AGH/\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"ip\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[32m\"10.0.0.1\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"subnet\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[32m\"10.0.0.0/8\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"time\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[32m\"2023-11-14T22:13:20Z\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"duration\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[32m\"1.5s\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"list\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m[\x1b"
                              "[0m\x1b"
                              "[37m1\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[30mnull\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[32m\"two\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m[\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m]\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m{\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m}\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m]\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"record\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m{\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"nested\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m{\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"x\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[37m1\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"y\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[30mnull\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m}\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m,\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[34m\"not an identifier\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m:\x1b"
                              "[0m\x1b"
                              "[32m\"z\"\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m}\x1b"
                              "[0m\x1b"
                              "[1m\x1b"
                              "[37m}\x1b"
                              "[0m");
}

TEST("print without nulls and with numeric durations") {
  auto options = plain(true);
  options.omit_null_fields = true;
  options.omit_nulls_in_lists = true;
  options.numeric_durations = true;
  CHECK_EQUAL(print(options),
              "{\"bool\":true,\"int\":-42,\"uint\":42,\"one\":1.0,\"big\":1e+"
              "300,\"nan\":null,\"negative_zero\":-0.0,\"fraction\":0.25,"
              "\"string\":\"quote\\\" backslash\\\\ newline\\n tab\\t "
              "ctrl\\u0001 utf8 \xc3"
              "\xa4"
              "\",\"blob\":\"AGH/\",\"ip\":\"10.0.0.1\",\"subnet\":\"10.0.0.0/"
              "8\",\"time\":\"2023-11-14T22:13:20Z\",\"duration\":1.5,\"list\":"
              "[1,\"two\",[],{}],\"record\":{\"nested\":{\"x\":1},\"not an "
              "identifier\":\"z\"}}");
}

TEST("print DEL unescaped unless styled") {
  auto dh = collecting_diagnostic_handler{};
  auto builder = nova::ArrayBuilder<nova::Data>{};
  nova::append_legacy_data(builder,
                           data{"a\x7f"
                                "b"},
                           dh);
  auto array = builder.finish();
  auto text = [&](json_printer_options options) {
    auto printer = nova::json_printer{std::move(options)};
    printer.print(array.get(0));
    auto const bytes = printer.bytes();
    return std::string{reinterpret_cast<char const*>(bytes.data()),
                       bytes.size()};
  };
  CHECK_EQUAL(text(plain(true)), "\"a\x7f"
                                 "b\"");
  auto styled = plain(true);
  styled.style = jq_style();
  CHECK_EQUAL(text(styled), "\x1b[32m\"a\\u007Fb\"\x1b[0m");
}

TEST("print replaces the previous row") {
  auto dh = collecting_diagnostic_handler{};
  auto builder = nova::ArrayBuilder<nova::Data>{};
  nova::append_legacy_data(builder, data{record{{"x", int64_t{1}}}}, dh);
  nova::append_legacy_data(builder, data{record{{"x", "y"}}}, dh);
  auto array = builder.finish();
  auto printer = nova::json_printer{plain(true)};
  auto text = [&] {
    auto const bytes = printer.bytes();
    return std::string{reinterpret_cast<char const*>(bytes.data()),
                       bytes.size()};
  };
  printer.print(array.get(0));
  CHECK_EQUAL(text(), R"({"x":1})");
  printer.print(array.get(1));
  CHECK_EQUAL(text(), R"({"x":"y"})");
}
