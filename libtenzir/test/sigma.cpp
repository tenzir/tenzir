//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/sigma.hpp"

#include "tenzir/data.hpp"
#include "tenzir/test/test.hpp"

#include <fmt/format.h>

using namespace tenzir;

namespace {

auto parse(std::string_view yaml) -> Result<sigma::Document, diagnostic> {
  auto document_data = unbox(from_yaml(yaml));
  return sigma::parse_document(document_data);
}

auto parse_rule(std::string_view yaml) -> sigma::DetectionRule {
  auto document = parse(yaml);
  REQUIRE(document.is_ok());
  auto* rule = try_as<sigma::DetectionRule>(document.unwrap().content);
  REQUIRE(rule != nullptr);
  return std::move(*rule);
}

constexpr auto basic_rule = R"yaml(
title: Suspicious Process
id: 0cb1b399-a086-4312-b7ed-2f2e78a3a628
level: high
logsource:
  category: process_creation
  product: windows
detection:
  selection:
    Image|endswith: '\evil.exe'
  filter:
    User: SYSTEM
  condition: selection and not filter
)yaml";

} // namespace

TEST("sigma ir - basic detection rule") {
  auto rule = parse_rule(basic_rule);
  CHECK(rule.metadata.title == "Suspicious Process");
  CHECK(rule.metadata.id == "0cb1b399-a086-4312-b7ed-2f2e78a3a628");
  CHECK(rule.metadata.level == "high");
  CHECK(rule.log_source.category == "process_creation");
  CHECK(rule.log_source.product == "windows");
  CHECK(not rule.log_source.service);
  REQUIRE_EQUAL(rule.detections.size(), 2u);
  CHECK(rule.detections.contains("selection"));
  CHECK(rule.detections.contains("filter"));
  auto const& selection = rule.detections.at("selection");
  REQUIRE_EQUAL(selection.groups.size(), 1u);
  REQUIRE_EQUAL(selection.groups[0].size(), 1u);
  auto const& item = selection.groups[0][0];
  CHECK_EQUAL(item.field.raw, "Image");
  REQUIRE_EQUAL(item.modifiers.size(), 1u);
  CHECK_EQUAL(item.modifiers[0], "endswith");
  REQUIRE_EQUAL(item.values.size(), 1u);
  CHECK(not item.value_is_list);
}

TEST("sigma ir - complete document is retained") {
  auto rule = parse_rule(basic_rule);
  CHECK(rule.metadata.raw.contains("detection"));
  CHECK(rule.metadata.raw.contains("logsource"));
}

TEST("sigma ir - sigma-version resolves to floor when absent") {
  auto document = parse(basic_rule);
  REQUIRE(document.is_ok());
  CHECK_EQUAL(document.unwrap().major, sigma::version_floor_major);
}

TEST("sigma ir - sigma-version accepts supported major") {
  auto document = parse("sigma-version: 2\n"
                        "detection:\n"
                        "  selection:\n"
                        "    a: 1\n"
                        "  condition: selection\n");
  REQUIRE(document.is_ok());
  CHECK_EQUAL(document.unwrap().major, 2);
}

TEST("sigma ir - sigma-version accepts release string") {
  auto document = parse("sigma-version: 2.1.0\n"
                        "detection:\n"
                        "  selection:\n"
                        "    a: 1\n"
                        "  condition: selection\n");
  REQUIRE(document.is_ok());
  CHECK_EQUAL(document.unwrap().major, 2);
}

TEST("sigma ir - sigma-version rejects malformed release strings") {
  // Note: `'2.'` is absent because YAML scalar type inference turns it into
  // the double 2.0 before the IR sees it, which resolves to major 2.
  for (auto const* version : {
         "-2",
         "2.-1",
         "2.1.-3",
         "2garbage",
         "2e3",
         "2.1.",
         "2.x",
         "v2",
       }) {
    auto document = parse(fmt::format("sigma-version: '{}'\n"
                                      "detection:\n"
                                      "  selection:\n"
                                      "    a: 1\n"
                                      "  condition: selection\n",
                                      version));
    CHECK(document.is_err());
  }
}

TEST("sigma ir - sigma-version rejects non-finite and out-of-range "
     "numbers") {
  for (auto const* version : {"1e309", "-1e309", "1e30"}) {
    auto document = parse(fmt::format("sigma-version: {}\n"
                                      "detection:\n"
                                      "  selection:\n"
                                      "    a: 1\n"
                                      "  condition: selection\n",
                                      version));
    CHECK(document.is_err());
  }
}

TEST("sigma ir - sigma-version rejects unsupported major") {
  auto document = parse("sigma-version: 3\n"
                        "detection:\n"
                        "  selection:\n"
                        "    a: 1\n"
                        "  condition: selection\n");
  CHECK(document.is_err());
}

TEST("sigma ir - unknown modifier rejects the rule") {
  auto document = parse("detection:\n"
                        "  selection:\n"
                        "    a|frobnicate: 1\n"
                        "  condition: selection\n");
  CHECK(document.is_err());
}

TEST("sigma ir - unimplemented modifier rejects the rule") {
  auto document = parse("detection:\n"
                        "  selection:\n"
                        "    a|utf16le|contains: x\n"
                        "  condition: selection\n");
  CHECK(document.is_err());
}

TEST("sigma ir - nested records are rejected") {
  auto document = parse("detection:\n"
                        "  selection:\n"
                        "    a:\n"
                        "      b: 1\n"
                        "  condition: selection\n");
  CHECK(document.is_err());
}

TEST("sigma ir - list of maps forms disjunction") {
  auto rule = parse_rule("detection:\n"
                         "  selection:\n"
                         "    - a: 1\n"
                         "      b: 2\n"
                         "    - c: 3\n"
                         "  condition: selection\n");
  auto const& selection = rule.detections.at("selection");
  REQUIRE_EQUAL(selection.groups.size(), 2u);
  CHECK_EQUAL(selection.groups[0].size(), 2u);
  CHECK_EQUAL(selection.groups[1].size(), 1u);
}

TEST("sigma ir - value list is preserved") {
  auto rule = parse_rule("detection:\n"
                         "  selection:\n"
                         "    a:\n"
                         "      - 1\n"
                         "      - 2\n"
                         "  condition: selection\n");
  auto const& item = rule.detections.at("selection").groups[0][0];
  CHECK(item.value_is_list);
  CHECK_EQUAL(item.values.size(), 2u);
}

TEST("sigma ir - empty value lists are rejected") {
  auto document = parse("detection:\n"
                        "  selection:\n"
                        "    a: []\n"
                        "  condition: selection\n");
  CHECK(document.is_err());
}

TEST("sigma ir - keyword lists are rejected") {
  auto document = parse("detection:\n"
                        "  keywords:\n"
                        "    - evil\n"
                        "  condition: keywords\n");
  CHECK(document.is_err());
}

TEST("sigma ir - missing detection") {
  CHECK(parse("title: no detection\n").is_err());
}

TEST("sigma ir - missing condition") {
  CHECK(parse("detection:\n"
              "  selection:\n"
              "    a: 1\n")
          .is_err());
}

TEST("sigma ir - condition references unknown identifier") {
  CHECK(parse("detection:\n"
              "  selection:\n"
              "    a: 1\n"
              "  condition: selektion\n")
          .is_err());
}

TEST("sigma ir - wildcard pattern must match at least one identifier") {
  CHECK(parse("detection:\n"
              "  selection:\n"
              "    a: 1\n"
              "  condition: 1 of nothing_*\n")
          .is_err());
  CHECK(parse("detection:\n"
              "  selection_a:\n"
              "    a: 1\n"
              "  condition: 1 of selection_*\n")
          .is_ok());
}

TEST("sigma ir - bare wildcard pattern must match at least one identifier") {
  CHECK(parse("detection:\n"
              "  selection:\n"
              "    a: 1\n"
              "  condition: missing_*\n")
          .is_err());
  CHECK(parse("detection:\n"
              "  selection_a:\n"
              "    a: 1\n"
              "  condition: selection_*\n")
          .is_ok());
}

TEST("sigma ir - sigma-version rejects majors that narrow to a supported "
     "value") {
  CHECK(parse("sigma-version: 4294967298\n"
              "detection:\n"
              "  selection:\n"
              "    a: 1\n"
              "  condition: selection\n")
          .is_err());
  CHECK(parse("sigma-version: 18446744073709551615\n"
              "detection:\n"
              "  selection:\n"
              "    a: 1\n"
              "  condition: selection\n")
          .is_err());
}

TEST("sigma ir - correlation documents fail explicitly") {
  CHECK(parse("correlation:\n"
              "  type: event_count\n")
          .is_err());
}

TEST("sigma ir - filter documents fail explicitly") {
  CHECK(parse("filter:\n"
              "  rules:\n"
              "    - some-rule\n")
          .is_err());
}

TEST("sigma condition - operators and precedence") {
  auto condition = sigma::parse_condition("a and b or not c");
  REQUIRE(condition.is_ok());
  // Expect ((a and b) or (not c)).
  auto const* root = try_as<sigma::Disjunction>(condition.unwrap().node);
  REQUIRE(root != nullptr);
  auto const* left = try_as<sigma::Conjunction>(root->left->node);
  REQUIRE(left != nullptr);
  auto const* right = try_as<sigma::Negation>(root->right->node);
  REQUIRE(right != nullptr);
}

TEST("sigma condition - parentheses") {
  auto condition = sigma::parse_condition("a and (b or c)");
  REQUIRE(condition.is_ok());
  auto const* root = try_as<sigma::Conjunction>(condition.unwrap().node);
  REQUIRE(root != nullptr);
  CHECK(is<sigma::Disjunction>(root->right->node));
}

TEST("sigma condition - quantifiers") {
  auto one = sigma::parse_condition("1 of selection_*");
  REQUIRE(one.is_ok());
  auto const* quantified = try_as<sigma::Quantified>(one.unwrap().node);
  REQUIRE(quantified != nullptr);
  CHECK(quantified->quantifier == sigma::Quantifier::one);
  CHECK_EQUAL(quantified->pattern, "selection_*");
  CHECK(not quantified->all_identifiers);
  auto all = sigma::parse_condition("all of them");
  REQUIRE(all.is_ok());
  auto const* all_quantified = try_as<sigma::Quantified>(all.unwrap().node);
  REQUIRE(all_quantified != nullptr);
  CHECK(all_quantified->quantifier == sigma::Quantifier::all);
  CHECK(all_quantified->all_identifiers);
}

TEST("sigma condition - numeric quantifiers other than one are rejected") {
  CHECK(sigma::parse_condition("2 of them").is_err());
  CHECK(sigma::parse_condition("any of them").is_err());
}

TEST("sigma condition - syntax errors") {
  CHECK(sigma::parse_condition("").is_err());
  CHECK(sigma::parse_condition("a and").is_err());
  CHECK(sigma::parse_condition("(a or b").is_err());
  CHECK(sigma::parse_condition("a b").is_err());
  CHECK(sigma::parse_condition("1 of").is_err());
}

TEST("sigma ir - wildcard matching") {
  CHECK(sigma::wildcard_match("*", "anything"));
  CHECK(sigma::wildcard_match("selection_*", "selection_a"));
  CHECK(not sigma::wildcard_match("selection_*", "filter"));
  CHECK(sigma::wildcard_match("a*c", "abc"));
  CHECK(not sigma::wildcard_match("a*c", "abd"));
}
