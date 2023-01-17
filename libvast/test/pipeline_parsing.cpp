//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE pipeline_parsing

#include "vast/system/make_pipelines.hpp"

#include <vast/pipeline_operator.hpp>
#include <vast/plugin.hpp>
#include <vast/test/test.hpp>

TEST(pipeline string parsing - extractor - space after comma) {
  std::string pipeline_str = " field1, field2, field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.extractors.size(), 3);
  REQUIRE_EQUAL(parsed_pipeline_input.extractors,
                (vast::list{"field1", "field2", "field3"}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - extractor - space before comma) {
  std::string pipeline_str = " field1 ,field2 ,field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.extractors.size(), 3);
  REQUIRE_EQUAL(parsed_pipeline_input.extractors,
                (vast::list{"field1", "field2", "field3"}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - extractor - missing comma) {
  std::string pipeline_str = " field1 ,field2 field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - extractor - no extractor between commas) {
  std::string pipeline_str = " field1,  ,field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - extractor - no spaces) {
  std::string pipeline_str = " field1,field2,field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.extractors.size(), 3);
  REQUIRE_EQUAL(parsed_pipeline_input.extractors,
                (vast::list{"field1", "field2", "field3"}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - extractor - random spaces) {
  std::string pipeline_str = " field1     ,field2 ,   field3";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.extractors.size(), 3);
  REQUIRE_EQUAL(parsed_pipeline_input.extractors,
                (vast::list{"field1", "field2", "field3"}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - extractor - single field) {
  std::string pipeline_str = "   field3   ";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.extractors.size(), 1);
  REQUIRE_EQUAL(parsed_pipeline_input.extractors, (vast::list{"field3"}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - extractor - comma at end) {
  std::string pipeline_str = "   field3,";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - extractor - pipeline delimiter) {
  std::string pipeline_str = " field3 | field1";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.extractors.size(), 1);
  REQUIRE_EQUAL(parsed_pipeline_input.extractors, (vast::list{"field3"}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - aggregators - single group
     - no time resolution) {
  std::string pipeline_str
    = " min(net.src.ip), max(net.dest.port) by timestamp";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(plugin);
  REQUIRE_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - single group
     - spaces in aggregator) {
  std::string pipeline_str
    = " min( net.src.ip ), max( net.dest.port ) by timestamp";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(plugin);
  REQUIRE_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - single group - time resolution) {
  std::string pipeline_str
    = " min(net.src.ip), max(net.dest.port) by timestamp resolution 1h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(plugin);
  REQUIRE_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - multiple groups
     - no time resolution) {
  std::string pipeline_str
    = " min(net.src.ip), max(net.dest.port) by timestamp, proto, event_type";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(plugin);
  REQUIRE_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - multiple groups
     - groups start with comma) {
  std::string pipeline_str
    = " min(net.src.ip), max(net.dest.port) by , timestamp, proto, event_type";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(!plugin);
  REQUIRE_NOT_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - multiple groups
     - time resolution) {
  std::string pipeline_str
    = " min(net.src.ip), max(net.dest.port) by timestamp resolution 5h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(plugin);
  REQUIRE_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - multiple groups - missing 'by') {
  std::string pipeline_str
    = " min(net.src.ip), max(net.dest.port) timestamp resolution 5h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(!plugin);
  REQUIRE_NOT_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - multiple groups
     - missing aggregator comma) {
  std::string pipeline_str
    = " min(net.src.ip) max(net.dest.port) timestamp resolution 5h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(!plugin);
  REQUIRE_NOT_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators
     - recursive aggregators not supported) {
  std::string pipeline_str
    = " min(net.src.ip), max(min(net.dest.port)) by timestamp resolution 5h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(!plugin);
  REQUIRE_NOT_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - missing opening bracket) {
  std::string pipeline_str
    = " minnet.src.ip), max(net.dest.port) by timestamp resolution 5h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(!plugin);
  REQUIRE_NOT_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - missing closing bracket) {
  std::string pipeline_str
    = " min(net.src.ip), max(net.dest.port by timestamp resolution 5h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(!plugin);
  REQUIRE_NOT_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - missing aggregator) {
  std::string pipeline_str = "  by timestamp resolution 5h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(!plugin);
  REQUIRE_NOT_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - starting with comma) {
  std::string pipeline_str = "  , distinct() by timestamp resolution 5h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(!plugin);
  REQUIRE_NOT_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - no grouping extractor) {
  std::string pipeline_str = "distinct() by timestamp resolution 5h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(plugin);
  REQUIRE_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators
     - missing grouping extractor brackets) {
  std::string pipeline_str = "distinct by timestamp resolution 5h";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(!plugin);
  REQUIRE_NOT_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - aggregators - multiple time resolution values) {
  std::string pipeline_str = "distinct() by timestamp resolution 5h 10s";
  auto* summarize_plugin
    = vast::plugins::find<vast::pipeline_operator_plugin>("summarize");
  std::string_view pipeline_str_view = pipeline_str;
  auto [parsed_iterator, plugin]
    = summarize_plugin->parse_pipeline_string(pipeline_str_view);
  REQUIRE(!plugin);
  REQUIRE_NOT_EQUAL(parsed_iterator, pipeline_str_view.end());
}

TEST(pipeline string parsing - options - long form options) {
  std::string pipeline_str = " --method=cryptopan --seed=deadbeef";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.long_form_options.size(), 2);
  REQUIRE_EQUAL(parsed_pipeline_input.long_form_options,
                (vast::record{{"method", "cryptopan"}, {"seed", "deadbeef"}}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - options - long form option - wrong space) {
  std::string pipeline_str = " - -method=cryptopan";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - options
     - long form options with comma in middle) {
  std::string pipeline_str = " --method=crypto, pan --seed=deadbeef";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - options - long form options with comma at end) {
  std::string pipeline_str = " --method=cryptopan, --seed=deadbeef";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.long_form_options.size(), 2);
  REQUIRE_EQUAL(parsed_pipeline_input.long_form_options,
                (vast::record{{"method", "cryptopan,"}, {"seed", "deadbeef"}}));
}

TEST(pipeline string parsing - options - long form option with no key
     or value) {
  std::string pipeline_str = " --";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - options - short form options) {
  std::string pipeline_str = " -m cryptopan -s deadbeef";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.short_form_options.size(), 2);
  REQUIRE_EQUAL(parsed_pipeline_input.short_form_options,
                (vast::record{{"m", "cryptopan"}, {"s", "deadbeef"}}));
}

TEST(pipeline string parsing - options - short form option - wrong space) {
  std::string pipeline_str = " - m cryptopan";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - options - short form option
     - single valueless options currently not supported) {
  std::string pipeline_str = " -m";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - options - short form option
     - multiple valueless options currently not supported) {
  std::string pipeline_str = " -m -a";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - options - short form option
     - too many letters in key) {
  std::string pipeline_str = " -me cryptopan";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - options
     - short form options with long form options) {
  std::string pipeline_str = " -D 10s  --method=cryptopan -s deadbeef";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.long_form_options.size(), 1);
  REQUIRE_EQUAL(parsed_pipeline_input.long_form_options,
                (vast::record{{"method", "cryptopan"}}));
  REQUIRE_EQUAL(parsed_pipeline_input.short_form_options.size(), 2);
  REQUIRE_EQUAL(parsed_pipeline_input.short_form_options,
                (vast::record{{"D", "10s"}, {"s", "deadbeef"}}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - value assignment - simple assignments) {
  std::string pipeline_str
    = R"( secret="xxx", my.connection =:suricata.flow, int= 2, strs = ["a", "b", "c"] )";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.assignments,
                (vast::list{vast::list{"secret", "\"xxx\""},
                            vast::list{"my.connection", ":suricata.flow"},
                            vast::list{"int", "2"},
                            vast::list{"strs", R"(["a", "b", "c"])"}}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - value assignment - list assignment) {
  std::string pipeline_str = R"(strs = ["a", "b", "c"])";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.assignments,
                (vast::list{vast::list{"strs", R"(["a", "b", "c"])"}}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - value assignment - nested list assignment) {
  std::string pipeline_str = R"(nested = [[1,2,3], [2,[2]], 1])";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE_EQUAL(parsed_pipeline_input.assignments,
                (vast::list{vast::list{"nested", R"([[1,2,3], [2,[2]], 1])"}}));
  REQUIRE(!parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - value assignment - no comma) {
  std::string pipeline_str
    = R"( secret="xxx", my.connection =:suricata.flow, int= 2 strs = ["a", "b", "c"] )";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - value assignment - comma at end) {
  std::string pipeline_str
    = R"( secret="xxx", my.connection =:suricata.flow, int= 2, strs = ["a", "b", "c"], )";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - value assignment - missing assignment) {
  std::string pipeline_str
    = R"( secret="xxx", my.connection=,strs = ["a", "b", "c"], int= 2, )";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - value assignment
     - missing list closing bracket) {
  std::string pipeline_str
    = R"( secret="xxx", my.connection=,strs = ["a", "b", "c", int= 2, )";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - value assignment
     - missing list opening bracket) {
  std::string pipeline_str
    = R"( secret="xxx", my.connection=,strs = "a", "b", "c"], int= 2, )";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - value assignment - double assignment) {
  std::string pipeline_str
    = R"(secret="xxx", my.connection=:suricata.flow=2, strs = ["a", "b", "c"])";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline_input = vast::system::parse_pipeline(pipeline_str_view);
  REQUIRE_NOT_EQUAL(parsed_pipeline_input.new_str_it, pipeline_str_view.end());
  REQUIRE(parsed_pipeline_input.parse_error);
}

TEST(pipeline string parsing - pipeline - identity) {
  std::string pipeline_str = "identity";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::system::make_pipeline(pipeline_str_view);
  REQUIRE(parsed_pipeline);
  REQUIRE_EQUAL(parsed_pipeline->size(), 1);
}

TEST(pipeline string parsing - pipeline - identity with extra input) {
  std::string pipeline_str = "identity haha";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::system::make_pipeline(pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - string with superfluous delimiter) {
  std::string pipeline_str = "identity | ";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::system::make_pipeline(pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline
     - string with two superfluous delimiters) {
  std::string pipeline_str = "identity | ";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::system::make_pipeline(pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline
     - invalid operator syntax) {
  std::string pipeline_str = "iden,tity";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::system::make_pipeline(pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - two identities - no space) {
  std::string pipeline_str = "identity|identity";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::system::make_pipeline(pipeline_str_view);
  REQUIRE(parsed_pipeline);
  REQUIRE_EQUAL(parsed_pipeline->size(), 2);
}

TEST(pipeline string parsing - pipeline - two identities
     - space after delimiter) {
  std::string pipeline_str = "identity| identity";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::system::make_pipeline(pipeline_str_view);
  REQUIRE(parsed_pipeline);
  REQUIRE_EQUAL(parsed_pipeline->size(), 2);
}

TEST(pipeline string parsing - pipeline - two identities
     - space before delimiter) {
  std::string pipeline_str = "identity |identity";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::system::make_pipeline(pipeline_str_view);
  REQUIRE(parsed_pipeline);
  REQUIRE_EQUAL(parsed_pipeline->size(), 2);
}

TEST(pipeline string parsing - pipeline - invalid operator) {
  std::string pipeline_str = "identity | invalid --test=test";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::system::make_pipeline(pipeline_str_view);
  REQUIRE(!parsed_pipeline);
}

TEST(pipeline string parsing - pipeline - identity->select->where) {
  std::string pipeline_str
    = "identity | select ip, timestamp | where ip !=127.0.0.1";
  std::string_view pipeline_str_view = pipeline_str;
  auto parsed_pipeline = vast::system::make_pipeline(pipeline_str_view);
  REQUIRE(parsed_pipeline);
  REQUIRE_EQUAL(parsed_pipeline->size(), 3);
}
