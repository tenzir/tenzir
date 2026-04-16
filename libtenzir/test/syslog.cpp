//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/test/test.hpp"

#include <tenzir/detail/syslog.hpp>

namespace tenzir {

TEST("syslog builder keeps pending multiline state when flushing committed "
     "rows") {
  auto dh = null_diagnostic_handler{};
  auto opts = multi_series_builder::options{};
  auto builder = plugins::syslog::syslog_builder{
    plugins::syslog::infuse_new_schema(opts), dh};

  auto line = std::string{"<165>1 2023-01-01T00:00:00Z host app 123 - - First"};
  auto msg = plugins::syslog::message{};
  auto f = line.begin();
  auto l = line.end();
  REQUIRE(plugins::syslog::message_parser{}.parse(f, l, msg));

  builder.add_new({std::move(msg), 1});
  REQUIRE(builder.last_message.is_some());
  CHECK_EQUAL(builder.last_message->line_count, size_t{1});

  // This mirrors read_syslog::prepare_snapshot: flush committed builder rows
  // without finalizing pending multiline state.
  auto slices = builder.builder.finalize_as_table_slice();
  CHECK(slices.empty());
  REQUIRE(builder.last_message.is_some());

  CHECK(builder.add_line_to_latest("stack trace continuation"));
  CHECK_EQUAL(builder.last_message->line_count, size_t{2});
  REQUIRE(builder.last_message->parsed.msg.has_value());
  CHECK_EQUAL(*builder.last_message->parsed.msg,
              std::string{"First\nstack trace continuation"});
}

} // namespace tenzir
