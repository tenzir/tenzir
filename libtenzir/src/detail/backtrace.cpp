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
#include <folly/coro/AsyncStack.h>

#include <ostream>

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
  name = detail::replace_all(name, "std::__1::", "std::");
  name = detail::replace_all(name, "(anonymous namespace)", "(anon)");
  for (auto it = name.begin(); it != name.end(); ++it) {
    if (*it != '<') {
      continue;
    }
    auto start = it;
    while (true) {
      ++it;
      if (it == name.end()) {
        return name;
      }
      if (*it != '>') {
        continue;
      }
      auto end = it;
      auto start_pos = start - name.begin();
      name = name.replace(start, end, "<...");
      it = name.begin() + start_pos + 1;
      break;
    }
  }
  return name;
}

auto format_async_frame(const boost::stacktrace::frame& frame) -> std::string {
  auto file_name = frame.source_file();
  if (file_name.empty()) {
    auto function_name
      = simplify_name(replace_all(frame.name(), " (.resume)", ""));
    return fmt::format("{} @ {}", function_name, frame.address());
  } else {
    return fmt::format("{}:{} @ {}", file_name, frame.source_line(),
                       frame.address());
  }
}

} // namespace

auto format_frame(const boost::stacktrace::frame& frame) -> std::string {
  auto file_name = frame.source_file();
  if (file_name.empty()) {
    auto function_name = simplify_name(frame.name());
    return fmt::format("{} @ {}", function_name, frame.address());
  } else {
    return fmt::format("{}:{} @ {}", file_name, frame.source_line(),
                       frame.address());
  }
}

auto has_async_stacktrace() -> bool {
  auto root = folly::tryGetCurrentAsyncStackRoot();
  return root != nullptr;
}

void print_async_stacktrace(std::ostream& out) {
  auto root = folly::tryGetCurrentAsyncStackRoot();
  if (not root) {
    return;
  }
  auto frame = root->getTopFrame();
  while (frame) {
    auto f = boost::stacktrace::frame{frame->getReturnAddress()};
    out << detail::format_frame(f) << '\n';
    frame = frame->getParentFrame();
  }
}

} // namespace tenzir::detail
