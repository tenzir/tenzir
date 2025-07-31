//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2022 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "sigma/parse.hpp"

#include <tenzir/concept/parseable/tenzir/expression.hpp>
#include <tenzir/concept/parseable/to.hpp>
#include <tenzir/concept/printable/stream.hpp>
#include <tenzir/concept/printable/tenzir/expression.hpp>
#include <tenzir/concept/printable/to_string.hpp>
#include <tenzir/detail/base64.hpp>
#include <tenzir/expression.hpp>
#include <tenzir/test/test.hpp>

#include <string_view>

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace tenzir;
using namespace tenzir::detail;

namespace {

expression to_search_id(std::string_view yaml) {
  return unbox(plugins::sigma::parse_search_id(unbox(from_yaml(yaml))));
}

expression to_rule(std::string_view yaml) {
  return unbox(plugins::sigma::parse_rule(unbox(from_yaml(yaml))));
}

expression to_expr(std::string_view expr) {
  return unbox(to<expression>(expr));
}

} // namespace

TEST("wildcard unescaping") {
  auto check_pattern_equality
    = [](std::string_view sigma, std::string_view pattern) {
        // Patterns generated from Sigma glob strings are case-insensitive.
        auto case_insensitive_pattern = fmt::format("{}i", pattern);
        CHECK_EQUAL(to_search_id(sigma), to_expr(case_insensitive_pattern));
        CHECK_NOT_EQUAL(to_search_id(sigma), to_expr(pattern));
      };
  check_pattern_equality("x: '*'", "x == /.*/");
  check_pattern_equality("x: '?'", "x == /./");
  check_pattern_equality("x: 'f*'", "x == /f.*/");
  check_pattern_equality("x: 'f?'", "x == /f./");
  check_pattern_equality("x: 'f*bar'", "x == /f.*bar/");
  check_pattern_equality("x: 'f?bar'", "x == /f.bar/");
  check_pattern_equality("x: 'f\\*bar'", "x == /f\\*bar/");
  check_pattern_equality("x: 'f\\?bar'", "x == /f\\?bar/");
  check_pattern_equality("x: 'f\\\\*bar'", "x == /f\\\\.*bar/");
  check_pattern_equality("x: 'f\\\\?bar'", "x == /f\\\\.bar/");
}

TEST("maps - single value") {
  auto yaml = "foo: 42";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42");
  CHECK_EQUAL(search_id, expected);
}

TEST("maps - empty value") {
  auto yaml = "foo: ''";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == //i");
  CHECK_EQUAL(search_id, expected);
}

TEST("maps - null value") {
  auto yaml = "foo: null";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == null");
  CHECK_EQUAL(search_id, expected);
}

TEST("maps - multiple values") {
  auto yaml = R"__(
    foo: 42
    bar: 43
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42 && bar == 43");
  CHECK_EQUAL(search_id, expected);
}

TEST("list - single value") {
  auto yaml = R"__(
    foo:
      - 42
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42");
  CHECK_EQUAL(search_id, expected);
}

TEST("lists - multiple values") {
  auto yaml = R"__(
    foo:
      - 42
      - 43
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42 || foo == 43");
  CHECK_EQUAL(search_id, expected);
}

TEST("list of maps") {
  auto yaml = R"__(
    - foo: 42
    - bar: 43
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42 || bar == 43");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - all") {
  auto yaml = R"__(
    foo|all:
      - 42
      - 43
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == 42 && foo == 43");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - sequenced all") {
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

TEST("modifier - contains") {
  auto yaml = R"__(
    foo|contains: "10.0.0.0/8"
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo ni 10.0.0.0/8");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - re") {
  auto yaml = R"__(
    foo|re: "^.*$"
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == /^.*$/");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - re 2") {
  auto yaml = R"__(
    foo|re: '.*foobar.*'
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == /.*foobar.*/");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - re - no wildcards") {
  auto yaml = R"__(
    foo|re: 'foobar'
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == /foobar/");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - startswith") {
  auto yaml = R"__(
    foo|startswith: "x"
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == /^x.*/i");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - startswith - proper backslash escaping") {
  auto yaml = R"__(
    foo|startswith: "C:\rundll32"
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr(R"(foo == /^C:\\rundll32.*/i)");
  CHECK_EQUAL(search_id, expected);
  auto str = to_string(expected);
  CHECK_EQUAL(str, R"(foo == /^C:\\rundll32.*/i)");
}

TEST("modifier - startswith - wildcards") {
  auto yaml = R"__(
    foo|startswith: "f*b?r"
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == /^f.*b.r.*/i");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - endswith") {
  auto yaml = R"__(
    foo|endswith: "x"
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == /.*x$/i");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - endswith - proper backslash escaping") {
  auto yaml = R"__(
    foo|endswith: "\rundll32.exe"
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr(R"(foo == /.*\\rundll32\.exe$/i)");
  CHECK_EQUAL(search_id, expected);
  auto str = to_string(expected);
  CHECK_EQUAL(str, R"(foo == /.*\\rundll32\.exe$/i)");
}

TEST("modifier - endswith - wildcards") {
  auto yaml = R"__(
    foo|endswith: "f*b?r"
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo == /.*f.*b.r$/i");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - lt") {
  auto yaml = R"__(
    foo|lt: 42
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo < 42");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - lte") {
  auto yaml = R"__(
    foo|lte: 42
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo <= 42");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - gt") {
  auto yaml = R"__(
    foo|gt: 42
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo > 42");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - gte") {
  auto yaml = R"__(
    foo|gte: 42
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo >= 42");
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - base64") {
  auto yaml = R"__(
    foo|base64: value
  )__";
  auto search_id = to_search_id(yaml);
  auto base64_value = detail::base64::encode("value"sv);
  REQUIRE_EQUAL(base64_value, "dmFsdWU="); // echo -n value | base64
  auto expected = to_expr(fmt::format("foo == /{}/i", base64_value));
  CHECK_EQUAL(search_id, expected);
}

TEST("modifier - double base64") {
  auto yaml = R"__(
    foo|base64|base64: value
  )__";
  auto search_id = to_search_id(yaml);
  auto base64_value = detail::base64::encode("value"sv);
  base64_value = detail::base64::encode(base64_value);
  REQUIRE_EQUAL(base64_value, "ZG1Gc2RXVT0="); // echo -n dmFsdWU= | base64
  auto expected = to_expr(fmt::format("foo == /{}/i", base64_value));
  CHECK_EQUAL(search_id, expected);
}

// The ground truth is in the sigma repo. We use the rule
// tests/test-modifiers.yml and invoke it as follows:
//
//   tools/sigmac -t es-qs -c winlogbeat tests/test-modifiers.yml
//
// This yields:
//
// (winlog.channel:"Security" AND field.keyword:/.*foobar.*/ AND
// encoded:"VABoAGkAcwAgAHMAdAByAGkAbgBnACAAaQBzACAAQgBhAHMAZQA2ADQAIABlAG4AYwBvAGQAZQBkAA\=\="
// AND obfuscated.keyword:(*aHR0cDovL* OR *h0dHA6Ly* OR *odHRwOi8v* OR
// *aHR0cHM6Ly* OR *h0dHBzOi8v* OR *odHRwczovL*) AND allmatch.keyword:*foo* AND
// allmatch.keyword:*bar* AND allmatch.keyword:*bla* AND end.keyword:*test AND
// start.keyword:test*)
//
// Note the following sub-expression: *aHR0cDovL* OR *h0dHA6Ly* OR *odHRwOi8v*.
// This is a combination of 'base64offset' and 'contains'. Without the contains
// modifier, it would look like this: aHR0cDovL OR h0dHA6Ly OR odHRwOi8v.
TEST("modifier - base64offset") {
  auto yaml = R"__(
    foo|base64offset: "http://"
  )__";
  auto search_id = to_search_id(yaml);
  auto expr
    = R"__(foo == "aHR0cDovL" || foo == "h0dHA6Ly" || foo == "odHRwOi8v")__"s;
  CHECK_EQUAL(search_id, to_expr(expr));
}

TEST("modifier - base64offset and contains") {
  auto yaml = R"__(
    foo|base64offset|contains: "http://"
  )__";
  auto search_id = to_search_id(yaml);
  auto expr
    = R"__(foo ni "aHR0cDovL" || foo ni "h0dHA6Ly" || foo ni "odHRwOi8v")__"s;
  CHECK_EQUAL(search_id, to_expr(expr));
}

TEST("modifier - cidr") {
  auto yaml = R"__(
    foo|cidr: 192.168.0.0/24
  )__";
  auto search_id = to_search_id(yaml);
  auto expected = to_expr("foo in 192.168.0.0/24"s);
  CHECK_EQUAL(search_id, expected);
}

// FIXME: The expression parser doesn't handle escaped strings properly, so we
// can't include NUL bytes in a query string yet. Once this is possible.
// TEST("modifier - wide") {
//   auto yaml = R"__(
//     foo|wide: abc
//   )__";
//   auto search_id = to_search_id(yaml);
//   auto expected = to_expr(R"__(foo == "a\x00b\x00c\x00")__"s);
//   CHECK_EQUAL(search_id, expected);
// }

TEST("search id selection - exact match") {
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

TEST("search id selection - boolean algebra 1") {
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

TEST("search id selection - boolean algebra - nested") {
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

TEST("search id selection - 1 of them") {
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

TEST("search id selection - 1 of pattern") {
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

TEST("search id selection - all of pattern") {
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

TEST("search id selection - flip to AND") {
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

TEST("real example") {
  auto expr = to_rule(unc2452);
  // clang-format off
  auto selection1 = R"__(CommandLine == /.*7z\.exe a -v500m -mx9 -r0 -p.*/i)__"s;
  auto selection2a = R"__(ParentCommandLine == /.*wscript\.exe.*/i && ParentCommandLine == /.*\.vbs.*/i)__"s;
  auto selection2b = R"__(CommandLine == /.*rundll32\.exe.*/i && CommandLine == /.*C:\\Windows.*/i && CommandLine == /.*\.dll,Tk_.*/i)__"s;
  auto selection3 = R"__(ParentImage == /.*\\rundll32\.exe$/i && ParentCommandLine == /.*C:\\Windows.*/i && CommandLine == /.*cmd\.exe \/C .*/i)__"s;
  auto selection4 = R"__(CommandLine == /.*rundll32 c:\\windows\\.*/i && CommandLine == /.*\.dll .*/i)__"s;
  auto specific1 = R"__(ParentImage == /.*\\rundll32\.exe$/i && Image == /.*\\dllhost\.exe$/i)__"s;
  auto filter1 = R"__(CommandLine == / /i || CommandLine == //i)__"s;
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

auto modifier_test = R"__(
 title: Modifier test rule
 logsource:
     product: windows
     service: security
 detection:
     selection:
         field|re: '.*foobar.*'
         encoded|wide|base64: 'This string is Base64 encoded'
         obfuscated|base64offset|contains:
             - 'http://'
             - 'https://'
         allmatch|contains|all:
             - foo
             - bar
             - bla
         end|endswith: test
         start|startswith: test
     condition: selection
)__";

// TODO: For this to run through we need to implement the utf* modifiers.
// TEST("modifier test rule") {
//   auto expr = to_rule(modifier_test);
//   auto field = R"__(field|re: '.*foobar.*')__"s;
//   auto encoded = R"__(encoded|wide|base64: 'This string is Base64
//   encoded')__"s; auto obfuscated
//     = R"__(obfuscated|base64offset|contains: ['http://', 'https://'])__"s;
//   auto allmatch = R"__(allmatch|contains|all: [foo, bar, bla])__"s;
//   auto end = R"__(end|endswith: test)__"s;
//   auto start = R"__(start|startswith: test)__"s;
//   conjunction expected;
//   expected.emplace_back(to_expr(field));
//   expected.emplace_back(to_expr(encoded));
//   expected.emplace_back(to_expr(obfuscated));
//   expected.emplace_back(to_expr(allmatch));
//   expected.emplace_back(to_expr(end));
//   expected.emplace_back(to_expr(start));
//   CHECK_EQUAL(expr, expression{expected});
// }
