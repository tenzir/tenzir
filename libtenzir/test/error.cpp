//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2018 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/error.hpp"

#include "tenzir/test/test.hpp"

using namespace std::string_literals;
using namespace tenzir;

TEST("error to_string") {
  auto str = [](auto x) {
    return to_string(x);
  };
  CHECK_EQUAL(str(ec::no_error), "no_error"s);
  CHECK_EQUAL(str(ec::unspecified), "unspecified"s);
  CHECK_EQUAL(str(ec::filesystem_error), "filesystem_error"s);
  CHECK_EQUAL(str(ec::type_clash), "type_clash"s);
  CHECK_EQUAL(str(ec::unsupported_operator), "unsupported_operator"s);
  CHECK_EQUAL(str(ec::parse_error), "parse_error"s);
  CHECK_EQUAL(str(ec::print_error), "print_error"s);
  CHECK_EQUAL(str(ec::convert_error), "convert_error"s);
  CHECK_EQUAL(str(ec::invalid_query), "invalid_query"s);
  CHECK_EQUAL(str(ec::format_error), "format_error"s);
  CHECK_EQUAL(str(ec::end_of_input), "end_of_input"s);
  CHECK_EQUAL(str(ec::timeout), "timeout"s);
  CHECK_EQUAL(str(ec::version_error), "version_error"s);
  CHECK_EQUAL(str(ec::syntax_error), "syntax_error"s);
  CHECK_EQUAL(str(ec::lookup_error), "lookup_error"s);
  CHECK_EQUAL(str(ec::logic_error), "logic_error"s);
  CHECK_EQUAL(str(ec::invalid_table_slice_type), "invalid_table_slice_type"s);
  CHECK_EQUAL(str(ec::invalid_synopsis_type), "invalid_synopsis_type"s);
  CHECK_EQUAL(str(ec::remote_node_down), "remote_node_down"s);
  CHECK_EQUAL(str(ec::invalid_result), "invalid_result"s);
  CHECK_EQUAL(str(ec::invalid_configuration), "invalid_configuration"s);
  CHECK_EQUAL(str(ec::unrecognized_option), "unrecognized_option"s);
  CHECK_EQUAL(str(ec::invalid_subcommand), "invalid_subcommand"s);
  CHECK_EQUAL(str(ec::missing_subcommand), "missing_subcommand"s);
  CHECK_EQUAL(str(ec::missing_component), "missing_component"s);
  CHECK_EQUAL(str(ec::unimplemented), "unimplemented"s);
  CHECK_EQUAL(str(ec::silent), "silent"s);
  CHECK_EQUAL(str(ec::out_of_memory), "out_of_memory"s);
}

TEST("render") {
  CHECK_EQUAL(render(caf::make_error(ec::unspecified)), "!! unspecified");
  CHECK_EQUAL(render(caf::make_error(ec::syntax_error, "msg")),
              "!! syntax_error: msg");
  CHECK_EQUAL(render(caf::make_error(ec::syntax_error, "test with", "multiple",
                                     "messages")),
              "!! syntax_error: test with multiple messages");
  CHECK_EQUAL(render(caf::make_error(caf::pec::type_mismatch, "ttt")),
              "!! type_mismatch: ttt");
  CHECK_EQUAL(render(caf::make_error(caf::sec::unexpected_message, "msg")),
              "!! unexpected_message: msg");
}
