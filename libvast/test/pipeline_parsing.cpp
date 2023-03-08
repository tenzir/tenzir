//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/system/make_pipelines.hpp"

#include <vast/pipeline.hpp>
#include <vast/pipeline_operator.hpp>
#include <vast/plugin.hpp>
#include <vast/test/test.hpp>

TEST(pipeline string parsing - extractor - space after comma) {
  std::string pipeline_str = "select field1, field2, field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("select", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - extractor - space before comma) {
  std::string pipeline_str = "select field1 ,field2 ,field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("select", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - extractor - missing comma) {
  std::string pipeline_str = "select field1 ,field2 field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("select", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - extractor - no extractor between commas) {
  std::string pipeline_str = "drop field1,  ,field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("select", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - extractor - no spaces) {
  std::string pipeline_str = "select field1,field2,field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("select", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - extractor - random spaces) {
  std::string pipeline_str = "select field1     ,field2 ,   field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("select", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - extractor - single field) {
  std::string pipeline_str = "select   field3   ";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("select", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - extractor - comma at end) {
  std::string pipeline_str = "select   field3,";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("select", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - extractor - pipeline delimiter) {
  std::string pipeline_str = "select field3 | drop field1";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("select", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - single group
     - no time resolution) {
  std::string pipeline_str
    = "summarize min(connections), max(timeouts) by timestamp";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - single group
     - multiple aggregator extractors) {
  std::string pipeline_str
    = "summarize min(connections, timeouts) by timestamp";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - single group
     - spaces in aggregator) {
  std::string pipeline_str
    = "summarize min( net.src.ip ), max( net.dest.port ) by timestamp";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - single group - time resolution) {
  std::string pipeline_str = "summarize min(net.src.ip), max(net.dest.port) by "
                             "timestamp resolution 1 hour";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - multiple groups
     - no time resolution) {
  std::string pipeline_str = "summarize min(net.src.ip), max(net.dest.port) by "
                             "timestamp, proto, event_type";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - multiple groups
     - groups start with comma) {
  std::string pipeline_str = "summarize min(net.src.ip), max(net.dest.port) by "
                             ", timestamp, proto, event_type";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - multiple groups
     - time resolution) {
  std::string pipeline_str = "summarize min(net.src.ip), max(net.dest.port) by "
                             "timestamp, event_type resolution 5 hours";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - multiple groups - missing 'by') {
  std::string pipeline_str = "summarize min(net.src.ip), max(net.dest.port) "
                             "timestamp resolution 5 hours";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - multiple groups
     - missing aggregator comma) {
  std::string pipeline_str = "summarize min(net.src.ip) max(net.dest.port) by "
                             "timestamp resolution 5 hours";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - aggregators
     - recursive aggregators not supported) {
  std::string pipeline_str
    = "summarize min(net.src.ip), max(min(net.dest.port)) by timestamp "
      "resolution 5 hours";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - missing opening bracket) {
  std::string pipeline_str = "summarize minnet.src.ip), max(net.dest.port) by "
                             "timestamp resolution 5 hours";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - missing closing bracket) {
  std::string pipeline_str = "summarize min(net.src.ip), max(net.dest.port by "
                             "timestamp resolution 5 hours";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - missing aggregator) {
  std::string pipeline_str = "summarize  by timestamp resolution 5 hours";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - starting with comma) {
  std::string pipeline_str
    = "summarize  , distinct() by timestamp resolution 5 hours";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - no grouping extractor) {
  std::string pipeline_str
    = "summarize distinct() by timestamp resolution 5 hours";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - aggregators
     - missing grouping extractor brackets) {
  std::string pipeline_str
    = "summarize distinct by timestamp resolution 5 hours";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - aggregators - multiple time resolution values) {
  std::string pipeline_str
    = "summarize distinct() by timestamp resolution 5 minutes 10 seconds";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("summarize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - options - long form options) {
  std::string pipeline_str
    = R"(pseudonymize --method = "cryptopan" --seed="deadbeef" field)";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - options - long form option - wrong space) {
  std::string pipeline_str
    = R"(pseudonymize - -method="cryptopan" --seed="deadbeef" field)";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - options
     - long form options with comma in middle) {
  std::string pipeline_str
    = R"(pseudonymize --method="crypto", "pan" --seed="deadbeef" field)";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - options - long form options with comma at end) {
  std::string pipeline_str
    = R"(pseudonymize --method="cryptopan" --seed="deadbeef", field)";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - options - long form option with no key
     or value) {
  std::string pipeline_str = "pseudonymize -- field";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - options - short form options) {
  std::string pipeline_str
    = R"(pseudonymize -m "cryptopan" -s "deadbeef" field)";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - options - short form option - wrong space) {
  std::string pipeline_str = R"(pseudonymize - m "cryptopan" field)";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - options - short form option - pseudonymize
     - single valueless options currently not supported) {
  std::string pipeline_str = "pseudonymize -m field";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - options - short form option - pseudonymize
     - multiple valueless options currently not supported) {
  std::string pipeline_str = "pseudonymize -m -a field";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - options - short form option
     - too many letters in key) {
  std::string pipeline_str = "pseudonymize -me cryptopan";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - options
     - short form options with long form options) {
  std::string pipeline_str
    = R"(pseudonymize --method="cryptopan" -s "deadbeef" field)";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - options
     - operator with wrong short form option) {
  std::string pipeline_str = R"(pseudonymize -X "cryptopan" field)";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - options - operator with wrong long form option) {
  std::string pipeline_str = R"(pseudonymize --unused="cryptopan" field)";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline
    = vast::pipeline::parse("pseudonymize", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - value assignment - simple renaming) {
  std::string pipeline_str = R"(rename secret=xxx)";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("rename", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - value assignment - simple assignments) {
  std::string pipeline_str
    = R"(extend abc_str ="123", abc= 123, abc = ["a","b", "c"])";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("extend", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - value assignment - list assignment) {
  std::string pipeline_str = R"(extend strs = ["a", "b", "c"])";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("extend", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - value assignment - no comma) {
  std::string pipeline_str
    = R"(extend abc_str ="123", abc= 123, int = 2 abc = ["a","b", "c"] )";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("replace", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - value assignment - comma at end) {
  std::string pipeline_str
    = R"(rename secret="xxx", my.connection =suricata.flow, int= 2, strs = ["a", "b", "c"], )";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("rename", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - value assignment
     - missing list closing bracket) {
  std::string pipeline_str
    = R"(extend abc_str ="123", abc= 123, abc = ["a","b", "c")";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("extend", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - value assignment
     - missing list opening bracket) {
  std::string pipeline_str
    = R"(extend abc_str ="123", abc= 123, abc = "a","b", "c"], int= 2, )";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("extend", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - value assignment - double assignment) {
  std::string pipeline_str
    = R"(extend abc_str ="123", abc= 123 = 2, abc = ["a","b", "c"])";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("extend", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - pass) {
  std::string pipeline_str = "pass";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("export", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - pass with extra input) {
  std::string pipeline_str = "pass haha";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("export", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - string with superfluous delimiter) {
  std::string pipeline_str = "pass | ";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("export", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline
     - string with two superfluous delimiters) {
  std::string pipeline_str = "pass | | ";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("export", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - invalid operator syntax) {
  std::string pipeline_str = "iden,tity";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("export", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - pass twice - no space) {
  std::string pipeline_str = "pass|pass";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("export", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - pass twice - space after delimiter) {
  std::string pipeline_str = "pass| pass";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("export", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - pass twice - space before delimiter) {
  std::string pipeline_str = "pass |pass";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("export", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - invalid operator) {
  std::string pipeline_str = "pass | invalid --test=test";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("export", pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - pass->select->where) {
  std::string pipeline_str
    = "pass | select ip, timestamp | where ip !=127.0.0.1";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::pipeline::parse("export", pipeline_str_view);
  REQUIRE(parsed_pipeline);
}
