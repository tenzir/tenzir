//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The VAST Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "vast/logical_operator.hpp"

#include <vast/plugin.hpp>

namespace vast {

class dump_operator : public logical_operator<chunks, void> {
public:
  explicit dump_operator(const dumper_plugin& dumper) noexcept
    : dumper_plugin_{dumper} {
  }

  [[nodiscard]] auto
  make_physical_operator(const type& input_schema,
                         operator_control_plane& ctrl) noexcept
    -> caf::expected<physical_operator<chunks, void>> override {
    auto new_dumper = dumper_plugin_.make_dumper({}, input_schema, ctrl);
    if (!new_dumper) {
      return new_dumper.error();
    }
    dumper_ = std::move(*new_dumper);
    return [this](generator<chunk_ptr> input) -> generator<std::monostate> {
      return dumper_(std::move(input));
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::format("to {}", dumper_plugin_.name());
  }

private:
  const dumper_plugin& dumper_plugin_;
  dumper_plugin::dumper dumper_;
};

} // namespace vast
