
//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#define SUITE sigma

#include "vast/detail/sigma.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/expression.hpp"
#include "vast/test/test.hpp"

#include <caf/test/dsl.hpp>

using namespace std::string_literals;
using namespace vast;
using namespace vast::detail;

namespace {

expression to_search_id(std::string_view yaml) {
  return unbox(sigma::parse_search_id(unbox(from_yaml(yaml))));
}

expression to_rule(std::string_view yaml) {
  return unbox(sigma::parse_rule(unbox(from_yaml(yaml))));
}

expression to_expr(std::string_view expr) {
  return unbox(to<expression>(expr));
}

} // namespace

TEST(wildcard unescaping) {
  CHECK_EQUAL(to_search_id("x: '*'"), to_expr("x ~ /.*/"));
  CHECK_EQUAL(to_search_id("x: '?'"), to_expr("x ~ /./"));
  CHECK_EQUAL(to_search_id("x: 'f*'"), to_expr("x ~ /f.*/"));
  CHECK_EQUAL(to_search_id("x: 'f?'"), to_expr("x ~ /f./"));
  CHECK_EQUAL(to_search_id("x: 'f*bar'"), to_expr("x ~ /f.*bar/"));
  CHECK_EQUAL(to_search_id("x: 'f?bar'"), to_expr("x ~ /f.bar/"));
  CHECK_EQUAL(to_search_id("x: 'f\\*bar'"), to_expr("x ~ /f*bar/"));
  CHECK_EQUAL(to_search_id("x: 'f\\?bar'"), to_expr("x ~ /f?bar/"));
  CHECK_EQUAL(to_search_id("x: 'f\\\\*bar'"), to_expr("x ~ /f\\.*bar/"));
  CHECK_EQUAL(to_search_id("x: 'f\\\\?bar'"), to_expr("x ~ /f\\.bar/"));
}

TEST(maps - single value) {
  auto yaml = "foo: 42";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42");
  CHECK_EQUAL(search_id, expected);
}

TEST(maps - empty value) {
  auto yaml = "foo: ''";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == \"\"");
  CHECK_EQUAL(search_id, expected);
}

TEST(maps - null value) {
  auto yaml = "foo: null";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == nil");
  CHECK_EQUAL(search_id, expected);
}

TEST(maps - multiple values) {
  auto yaml = R"__(
    foo: 42
    bar: 43
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42 && bar == 43");
  CHECK_EQUAL(search_id, expected);
}

TEST(list - single value) {
  auto yaml = R"__(
    foo:
      - 42
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42");
  CHECK_EQUAL(search_id, expected);
}

TEST(lists - multiple values) {
  auto yaml = R"__(
    foo:
      - 42
      - 43
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42 || foo == 43");
  CHECK_EQUAL(search_id, expected);
}

TEST(list of maps) {
  auto yaml = R"__(
    - foo: 42
    - bar: 43
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42 || bar == 43");
  CHECK_EQUAL(search_id, expected);
}

TEST(modifier - all) {
  auto yaml = R"__(
    foo|all:
      - 42
      - 43
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42 && foo == 43");
  CHECK_EQUAL(search_id, expected);
}

TEST(modifier - sequenced all) {
  auto yaml = R"__(
    foo|all:
      - 42
      - 43
    bar: 42
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("(foo == 42 && foo == 43) && bar == 42");
  CHECK_EQUAL(search_id, expected);
}

TEST(modifier - contains) {
  auto yaml = R"__(
    foo|contains: "10.0.0.0/8"
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo ni 10.0.0.0/8");
  CHECK_EQUAL(search_id, expected);
}

TEST(modifier - re) {
  auto yaml = R"__(
    foo|re: "^.*$"
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo ~ /^.*$/");
  CHECK_EQUAL(search_id, expected);
}

TEST(modifier - startswith) {
  auto yaml = R"__(
    foo|startswith: "x"
  )__";
  auto search_id = to_search_id(yaml);
  // TODO: blocked by VAST has pattern matching capability
  // auto expected = to_expr("foo ~ /^x/");
  auto expected = to_expr("foo ni \"x\"");
  CHECK_EQUAL(search_id, expected);
}

TEST(modifier - endswith) {
  auto yaml = R"__(
    foo|startswith: "x"
  )__";
  auto search_id = to_search_id(yaml);
  // TODO: blocked by VAST has pattern matching capability
  // auto expected = to_expr("foo ~ /x$/");
  auto expected = to_expr("foo ni \"x\"");
  CHECK_EQUAL(search_id, expected);
}

TEST(search id selection - exact match) {
  auto yaml = R"__(
    detection:
      test:
        foo: 42
        bar: 42
      condition: test
  )__";
  auto rule = to_rule(yaml);
  auto expected = to_expr("foo == 42 && bar == 42");
  CHECK_EQUAL(rule, expected);
}

TEST(search id selection - boolean algebra 1) {
  auto yaml = R"__(
    detection:
      a:
        foo: 42
      b:
        bar: 42
      c:
        baz: 42
      condition: a and not (b or c)
  )__";
  auto rule = to_rule(yaml);
  auto expected = to_expr("foo == 42 && ! (bar == 42 || baz == 42)");
  CHECK_EQUAL(rule, expected);
}

TEST(search id selection - boolean algebra - nested) {
  auto yaml = R"__(
    detection:
      a:
        foo: 42
      b:
        bar:
          - 42
          - 43
        baz:
          - 42
      condition: a or b
  )__";
  auto rule = to_rule(yaml);
  auto expected
    = to_expr("foo == 42 || ((bar == 42 || bar == 43) && baz == 42)");
  CHECK_EQUAL(rule, expected);
}

TEST(search id selection - 1 of them) {
  auto yaml = R"__(
    detection:
      selection1:
        foo: 42
      selection2:
        bar: 42
      condition: 1 of them
  )__";
  auto rule = to_rule(yaml);
  auto expected = to_expr("foo == 42 || bar == 42");
  CHECK_EQUAL(rule, expected);
}

TEST(search id selection - 1 of pattern) {
  auto yaml = R"__(
    detection:
      selection1:
        foo: 42
      selection2:
        bar: 42
      not_considered:
        evil: 6.6.6.6
      condition: 1 of sele*
  )__";
  auto rule = to_rule(yaml);
  auto expected = to_expr("foo == 42 || bar == 42");
  CHECK_EQUAL(rule, expected);
}

TEST(search id selection - all of pattern) {
  auto yaml = R"__(
    detection:
      selection1:
        foo: 42
      selection2:
        bar: 42
      not_considered:
        evil: 6.6.6.6
      condition: all of sele*
  )__";
  auto rule = to_rule(yaml);
  auto expected = to_expr("foo == 42 && bar == 42");
  CHECK_EQUAL(rule, expected);
}

TEST(search id selection - flip to AND) {
  auto yaml = R"__(
    detection:
      test:
        - foo: 42
        - bar: 42
      condition: all of test
  )__";
  auto rule = to_rule(yaml);
  auto expected = to_expr("foo == 42 && bar == 42");
  CHECK_EQUAL(rule, expected);
}

// Source:
// https://github.com/Neo23x0/sigma/commit/b62c705bf02e2b9089d21567e34ac05037f56338
auto unc2452 = R"__(
title: UNC2452 Process Creation Patterns
id: 9be34ad0-b6a7-4fbd-91cf-fc7ec1047f5f
description: Detects a specific process creation patterns as seen used by UNC2452 and provided by Microsoft as Microsoft Defender ATP queries
status: experimental
references:
    - https://www.microsoft.com/security/blog/2021/01/20/deep-dive-into-the-solorigate-second-stage-activation-from-sunburst-to-teardrop-and-raindrop/
tags:
    - attack.execution
    - attack.t1059.001
    - sunburst
    - unc2452
author: Florian Roth
date: 2021/01/22
logsource:
    category: process_creation
    product: windows
detection:
    selection1:
        CommandLine|contains:
            - '7z.exe a -v500m -mx9 -r0 -p'
    selection2:
        ParentCommandLine|contains|all:
            - 'wscript.exe'
            - '.vbs'
        CommandLine|contains|all:
            - 'rundll32.exe'
            - 'C:\Windows'
            - '.dll,Tk_'
    selection3:
        ParentImage|endswith: '\rundll32.exe'
        ParentCommandLine|contains: 'C:\Windows'
        CommandLine|contains: 'cmd.exe /C '
    selection4:
        CommandLine|contains|all:
            - 'rundll32 c:\windows\\'
            - '.dll '
    specific1:
        ParentImage|endswith: '\rundll32.exe'
        Image|endswith: '\dllhost.exe'
    filter1:
        CommandLine:
            - ' '
            - ''
    condition: selection1 or selection2 or selection3 or selection4 or ( specific1 and not filter1 )
falsepositives:
    - Unknown
level: critical
)__";

TEST(real example) {
  auto expr = to_rule(unc2452);
  // clang-format off
  auto selection1 = R"__(CommandLine ni "7z.exe a -v500m -mx9 -r0 -p")__"s;
  auto selection2a = R"__(ParentCommandLine ni "wscript.exe" && ParentCommandLine ni ".vbs")__"s;
  auto selection2b = R"__(CommandLine ni "rundll32.exe" && CommandLine ni "C:\Windows" && CommandLine ni ".dll,Tk_")__"s;
  auto selection3 = R"__(ParentImage ni "\rundll32.exe" && ParentCommandLine ni "C:\Windows" && CommandLine ni "cmd.exe /C ")__"s;
  auto selection4 = R"__(CommandLine ni "rundll32 c:\windows\\" && CommandLine ni ".dll ")__"s;
  auto specific1 = R"__(ParentImage ni "\rundll32.exe" && Image ni "\dllhost.exe")__"s;
  auto filter1 = R"__(CommandLine == " " || CommandLine == "")__"s;
  // clang-format on
  conjunction selection2;
  selection2.emplace_back(to_expr(selection2a));
  selection2.emplace_back(to_expr(selection2b));
  conjunction tail;
  tail.emplace_back(to_expr(specific1));
  tail.emplace_back(negation{to_expr(filter1)});
  disjunction expected;
  expected.emplace_back(to_expr(selection1));
  expected.emplace_back(selection2);
  expected.emplace_back(to_expr(selection3));
  expected.emplace_back(to_expr(selection4));
  expected.emplace_back(tail);
  CHECK_EQUAL(expr, expression{expected});
}
