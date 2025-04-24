//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2021 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/detail/backtrace.hpp"

#include "tenzir/detail/string.hpp"

#include <fmt/format.h>

#include <regex>

using namespace std::string_view_literals;

namespace tenzir::detail {

namespace {

auto simplify_name(std::string name) -> std::string {
  constexpr static auto actor_prefix
    = std::string_view{"caf::detail::default_behavior_impl<std::__1::tuple<"};
  if (auto actor_start = name.find(actor_prefix); actor_start != name.npos) {
    actor_start += actor_prefix.size();
    auto actor_end = name.find("::make_behavior()::'lambda'", actor_start);
    if (actor_end != std::string::npos) {
      name = name.substr(actor_start, actor_end - actor_start);
      name += "::make_behavior";
    }
  }
  constexpr static auto replacements = std::array{
    // transform make_behaviour calls to only include the final/called functor
    std::pair{
      R"(make_behavior\(\)::.*::make_behavior\(\)::(.*))",
      "make_behavior<...>::$1",
    },
    // replace internal name for `std::string`
    std::pair{
      R"(std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >)",
      "std::string",
    },
    // remove `std::allocator` from vector
    std::pair{
      R"(std::vector<(.+),\s*std::allocator<.+>\s*>)",
      "std::vector<$1>",
    },
    // cut type argument of `std::allocator`
    std::pair{
      R"(std::allocator<.+>)",
      "std::allocator<...>",
    },
    // remove internal std namespaces
    std::pair{
      "std::__cxx11::",
      "std::",
    },
    std::pair{
      "std::__1::",
      "std::",
    },
    // shorten anonymous namespaces
    std::pair{
      R"(\(anonymous namespace\))",
      "(anon)",
    },
    /// integer sequence parameter packs: `0ul, 1ul, ...`
    std::pair{
      R"((\d+ul(, )?)+)",
      "...",
    }};
  for (auto [original, replacement] : replacements) {
    const auto re = std::regex(original);
    name = std::regex_replace(name, re, replacement);
  }
  return name;
}

} // namespace

auto format_frame(const boost::stacktrace::frame& frame) -> std::string {
  auto file_name = frame.source_file();
  if (true) {
    auto function_name = simplify_name(frame.name());
    return fmt::format("{} @ {}", function_name, frame.address());
  } else {
    return fmt::format("{}:{} @ {}", file_name, frame.source_line(),
                       frame.address());
  }
}

} // namespace tenzir::detail
