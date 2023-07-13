//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <vast/argument_parser.hpp>
#include <vast/concept/parseable/numeric/integral.hpp>
#include <vast/concept/parseable/vast/pipeline.hpp>
#include <vast/error.hpp>
#include <vast/logger.hpp>
#include <vast/pipeline.hpp>
#include <vast/plugin.hpp>
#include <vast/table_slice.hpp>

#include <arrow/type.h>

namespace vast::plugins::taste {

namespace {

class taste_operator final
  : public schematic_operator<taste_operator, uint64_t> {
public:
  taste_operator() = default;

  explicit taste_operator(uint64_t limit) : limit_{limit} {
  }

  auto initialize(const type&, operator_control_plane&) const
    -> caf::expected<state_type> override {
    return limit_;
  }

  auto process(table_slice slice, state_type& remaining) const
    -> table_slice override {
    auto result = head(slice, remaining);
    remaining -= result.rows();
    return result;
  }

  auto to_string() const -> std::string override {
    return fmt::format("taste {}", limit_);
  }

  auto name() const -> std::string override {
    return "taste";
  }

  friend auto inspect(auto& f, taste_operator& x) -> bool {
    return f.apply(x.limit_);
  }

private:
  uint64_t limit_;
};

class plugin final : public virtual operator_plugin<taste_operator> {
public:
  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"taste", "https://docs.tenzir.com/next/"
                                           "operators/transformations/taste"};
    auto count = std::optional<uint64_t>{};
    parser.add(count, "<limit>");
    parser.parse(p);
    return std::make_unique<taste_operator>(count.value_or(10));
  }
};

} // namespace

} // namespace vast::plugins::taste

TENZIR_REGISTER_PLUGIN(vast::plugins::taste::plugin)
