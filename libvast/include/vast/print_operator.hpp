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

class print_operator : public logical_operator<events, chunks> {
public:
  explicit print_operator(const printer_plugin& printer) noexcept
    : printer_plugin_{printer} {
  }

  [[nodiscard]] auto
  make_physical_operator(const type& input_schema,
                         operator_control_plane& ctrl) noexcept
    -> caf::expected<physical_operator<events, chunks>> override {
    auto new_printer = printer_plugin_.make_printer({}, input_schema, ctrl);
    if (!new_printer) {
      return new_printer.error();
    }
    printer_ = std::move(*new_printer);
    return [this](generator<table_slice> input) -> generator<chunk_ptr> {
      return printer_(std::move(input));
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::format("write {}", printer_plugin_.name());
  }

private:
  const printer_plugin& printer_plugin_;
  printer_plugin::printer printer_;
};

} // namespace vast
