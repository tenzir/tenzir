
/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE sigma

#include "vast/detail/sigma.hpp"

#include "vast/test/test.hpp"

#include "vast/concept/parseable/to.hpp"
#include "vast/concept/parseable/vast/expression.hpp"
#include "vast/concept/printable/stream.hpp"
#include "vast/concept/printable/vast/expression.hpp"
#include "vast/expression.hpp"

#include <caf/test/dsl.hpp>

using namespace std::string_literals;
using namespace vast;
using namespace vast::detail;

// Source:
// https://github.com/Neo23x0/sigma/commit/b62c705bf02e2b9089d21567e34ac05037f56338
auto rule = R"__(
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

TEST(parse) {
  auto yaml = unbox(from_yaml(rule));
  auto expr = unbox(sigma::parse_rule(yaml));
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
  selection2.emplace_back(unbox(to<expression>(selection2a)));
  selection2.emplace_back(unbox(to<expression>(selection2b)));
  conjunction tail;
  tail.emplace_back(unbox(to<expression>(specific1)));
  tail.emplace_back(negation{unbox(to<expression>(filter1))});
  disjunction expected;
  expected.emplace_back(unbox(to<expression>(selection1)));
  expected.emplace_back(selection2);
  expected.emplace_back(unbox(to<expression>(selection3)));
  expected.emplace_back(unbox(to<expression>(selection4)));
  expected.emplace_back(tail);
  CHECK_EQUAL(normalize(expr), normalize(expected));
}
