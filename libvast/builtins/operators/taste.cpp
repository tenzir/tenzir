//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

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
  auto make_operator(std::string_view pipeline) const
    -> std::pair<std::string_view, caf::expected<operator_ptr>> override {
    using parsers::optional_ws_or_comment, parsers::required_ws_or_comment,
      parsers::end_of_pipeline_operator, parsers::u64;
    const auto* f = pipeline.begin();
    const auto* const l = pipeline.end();
    const auto p = -(required_ws_or_comment >> u64) >> optional_ws_or_comment
                   >> end_of_pipeline_operator;
    auto limit = std::optional<uint64_t>{};
    if (!p(f, l, limit)) {
      return {
        std::string_view{f, l},
        caf::make_error(ec::syntax_error, fmt::format("failed to parse "
                                                      "taste operator: '{}'",
                                                      pipeline)),
      };
    }
    return {
      std::string_view{f, l},
      std::make_unique<taste_operator>(limit.value_or(10)),
    };
  }
};

} // namespace

} // namespace vast::plugins::taste

VAST_REGISTER_PLUGIN(vast::plugins::taste::plugin)
