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

/// The logical operator for printing and dumping data without joining.
class print_dump_operator : public logical_operator<table_slice, void> {
public:
  explicit print_dump_operator(const printer_plugin& printer,
                               const dumper_plugin& dumper) noexcept
    : printer_plugin_{printer}, dumper_plugin_{dumper} {
  }

  [[nodiscard]] auto
  make_physical_operator(const type& input_schema,
                         operator_control_plane& ctrl) noexcept
    -> caf::expected<physical_operator<table_slice, void>> override {
    auto new_printer = printer_plugin_.make_printer({}, input_schema, ctrl);
    if (!new_printer) {
      return new_printer.error();
    }
    printer_ = std::move(*new_printer);
    auto new_dumper = dumper_plugin_.make_dumper({}, input_schema, ctrl);
    if (!new_dumper) {
      return new_dumper.error();
    }
    dumper_ = std::move(*new_dumper);
    return [this](generator<table_slice> input) -> generator<std::monostate> {
      return dumper_(printer_(std::move(input)));
    };
  }

  [[nodiscard]] auto to_string() const noexcept -> std::string override {
    return fmt::format("write {} to {}", printer_plugin_.name(),
                       dumper_plugin_.name());
  }

private:
  const printer_plugin& printer_plugin_;
  printer_plugin::printer printer_;
  const dumper_plugin& dumper_plugin_;
  dumper_plugin::dumper dumper_;
};

} // namespace vast
