/******************************************************************************
 *                    _   _____   __________                                  *
 *                   | | / / _ | / __/_  __/     Visibility                   *
 *                   | |/ / __ |_\ \  / /          Across                     *
 *                   |___/_/ |_/___/ /_/       Space and Time                 *
 *                                                                            *
 * This file is part of VAST. It is subject to the license terms in the       *
 * LICENSE file found in the top-level directory of this distribution and at  *
 * http://vast.io/license. No part of VAST, including this file, may be       *
 * copied, modified, propagated, or distributed except according to the terms *
 * contained in the LICENSE file.                                             *
 ******************************************************************************/

#define SUITE test

#include "vast/error.hpp"

#include "vast/test/test.hpp"

using namespace std::string_literals;
using namespace vast;

TEST(to_string) {
  auto str = [](auto x) { return to_string(x); };
  CHECK_EQUAL(str(ec::no_error), "no error"s);
  CHECK_EQUAL(str(ec::unspecified), "unspecified"s);
  CHECK_EQUAL(str(ec::filesystem_error), "filesystem error"s);
  CHECK_EQUAL(str(ec::type_clash), "type clash"s);
  CHECK_EQUAL(str(ec::unsupported_operator), "unsupported operator"s);
  CHECK_EQUAL(str(ec::parse_error), "parse error"s);
  CHECK_EQUAL(str(ec::print_error), "print error"s);
  CHECK_EQUAL(str(ec::convert_error), "convert error"s);
  CHECK_EQUAL(str(ec::invalid_query), "invalid query"s);
  CHECK_EQUAL(str(ec::format_error), "format error"s);
  CHECK_EQUAL(str(ec::end_of_input), "end of input"s);
  CHECK_EQUAL(str(ec::version_error), "version error"s);
  CHECK_EQUAL(str(ec::syntax_error), "syntax error"s);
  CHECK_EQUAL(str(ec::invalid_table_slice_type), "invalid table slice type"s);
  CHECK_EQUAL(str(ec::invalid_synopsis_type), "invalid synopsis type"s);
  CHECK_EQUAL(str(ec::remote_node_down), "remote node down"s);
  CHECK_EQUAL(str(ec::invalid_result), "invalid result"s);
  CHECK_EQUAL(str(ec::invalid_configuration), "invalid configuration"s);
  CHECK_EQUAL(str(ec::unrecognized_option), "unrecognized option"s);
  CHECK_EQUAL(str(ec::invalid_subcommand), "invalid subcommand"s);
  CHECK_EQUAL(str(ec::missing_subcommand), "missing subcommand"s);
  CHECK_EQUAL(str(ec::no_importer), "no importer"s);
  CHECK_EQUAL(str(ec::unimplemented), "unimplemented"s);
}

TEST(render) {
  CHECK_EQUAL(render(make_error(ec::unspecified)), "!! unspecified");
  CHECK_EQUAL(render(make_error(ec::syntax_error, "msg")),
              "!! syntax error: \"msg\"");
  CHECK_EQUAL(render(make_error(ec::syntax_error, "test with", "multiple",
                                "messages")),
              "!! syntax error: \"test with\" \"multiple\" \"messages\"");
  CHECK_EQUAL(render(make_error(caf::pec::type_mismatch, "ttt")),
              "!! type_mismatch: ([(\"argument\", \"ttt\")])");
  CHECK_EQUAL(render(make_error(caf::sec::unexpected_message, "msg")),
              "!! unexpected_message: (\"msg\")");
  CHECK_EQUAL(render(caf::error(255, caf::atom("foobar"),
                                caf::make_message(255, "msg"))),
              "!! foobar: (255, \"msg\")");
}
