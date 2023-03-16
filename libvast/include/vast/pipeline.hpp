//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "vast/transformer.hpp"

namespace vast {

class pipeline final : public transformer {
public:
  explicit pipeline(std::vector<transformer_ptr> transformers)
    : transformers_{std::move(transformers)} {
  }

  static auto parse(std::string_view repr) -> caf::expected<pipeline>;

  auto instantiate(dynamic_input input, transformer_control& control) const
    -> caf::expected<dynamic_output> override;

  auto unwrap() && -> std::vector<transformer_ptr> {
    return std::move(transformers_);
  }

private:
  std::vector<transformer_ptr> transformers_;
};

auto make_local_executor(pipeline p) -> generator<caf::error>;

} // namespace vast
