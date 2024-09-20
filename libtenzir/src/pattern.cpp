//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/pattern.hpp"

#include "tenzir/concept/printable/tenzir/pattern.hpp"
#include "tenzir/concept/printable/to_string.hpp"
#include "tenzir/data.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/error.hpp"
#include "tenzir/view.hpp"

#include <re2/re2.h>

namespace tenzir {

struct regex_impl : re2::RE2 {
  using RE2::RE2;
};

auto pattern::make(std::string str, pattern_options options) noexcept
  -> caf::expected<pattern> {
  auto opts = re2::RE2::Options(re2::RE2::CannedOptions::Quiet);
  opts.set_case_sensitive(!options.case_insensitive);
  auto regex = re2::RE2(str, opts);
  auto result = pattern{};
  result.str_ = std::move(str);
  result.options_ = options;
  result.regex_ = std::make_shared<regex_impl>(result.str_, opts);
  if (!result.regex_->ok()) {
    return diagnostic::error("failed to create regex from `{}`", result.str_)
      .to_error();
  }
  return result;
}

bool pattern::match(std::string_view str) const {
  if (!regex_) {
    return false;
  }
  return re2::RE2::FullMatch(str, *regex_);
}

bool pattern::search(std::string_view str) const {
  if (!regex_) {
    return false;
  }
  return re2::RE2::PartialMatch(str, *regex_);
}

const std::string& pattern::string() const {
  return str_;
}

const pattern_options& pattern::options() const {
  return options_;
}

bool operator==(const pattern& lhs, const pattern& rhs) noexcept {
  return pattern_view{lhs} == pattern_view{rhs};
}

std::strong_ordering
operator<=>(const pattern& lhs, const pattern& rhs) noexcept {
  return pattern_view{lhs} <=> pattern_view{rhs};
}

bool operator==(const pattern& lhs, std::string_view rhs) noexcept {
  return lhs.match(rhs);
}

bool convert(const pattern& p, data& d) {
  d = to_string(p);
  return true;
}

} // namespace tenzir
