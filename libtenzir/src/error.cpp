//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2016 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/error.hpp"

#include "tenzir/detail/assert.hpp"
#include "tenzir/detail/env.hpp"
#include "tenzir/diagnostics.hpp"

#include <caf/deep_to_string.hpp>
#include <caf/exit_reason.hpp>
#include <caf/message_handler.hpp>
#include <caf/pec.hpp>
#include <caf/sec.hpp>

#include <sstream>
#include <string>

namespace tenzir {
namespace {

const char* descriptions[] = {
  "no_error",
  "unspecified",
  "no_such_file",
  "filesystem_error",
  "type_clash",
  "unsupported_operator",
  "parse_error",
  "print_error",
  "convert_error",
  "invalid_query",
  "format_error",
  "end_of_input",
  "timeout",
  "stalled",
  "incomplete",
  "version_error",
  "syntax_error",
  "lookup_error",
  "logic_error",
  "invalid_table_slice_type",
  "invalid_synopsis_type",
  "remote_node_down",
  "invalid_argument",
  "invalid_result",
  "invalid_configuration",
  "unrecognized_option",
  "invalid_subcommand",
  "missing_subcommand",
  "missing_component",
  "unimplemented",
  "recursion_limit_reached",
  "silent",
  "out_of_memory",
  "system_error",
  "breaking_change",
  "serialization_error",
  "diagnostic",
};

static_assert(ec{std::size(descriptions)} == ec::ec_count,
              "Mismatch between number of error codes and descriptions");

void render_default_ctx(std::ostringstream& oss, const caf::message& ctx) {
  size_t size = ctx.size();
  if (size > 0) {
    oss << ":";
    for (size_t i = 0; i < size; ++i) {
      oss << ' ';
      if (ctx.match_element<std::string>(i))
        oss << ctx.get_as<std::string>(i);
      else
        oss << to_string(ctx);
    }
  }
}

} // namespace

const char* to_string(ec x) {
  auto index = static_cast<size_t>(x);
  TENZIR_ASSERT(index < sizeof(descriptions));
  return descriptions[index];
}

std::string render(caf::error err, bool pretty_diagnostics) {
  if (!err)
    return "";
  std::ostringstream oss;
  auto category = err.category();
  if (category == caf::type_id_v<tenzir::ec>
      && static_cast<tenzir::ec>(err.code()) == ec::diagnostic) {
    const auto color = (isatty(STDERR_FILENO) == 1
                        && detail::getenv("NO_COLOR").value_or("").empty())
                         ? color_diagnostics::yes
                         : color_diagnostics::no;
    auto printer = make_diagnostic_printer(std::nullopt, color, oss);
    auto ctx = err.context();
    caf::message_handler{
      [&](const diagnostic& diag) {
        if (pretty_diagnostics) {
          printer->emit(diag);
        } else {
          oss << fmt::format("{:?}", diag);
        }
      },
      [&](const std::vector<diagnostic>& diags) {
        for (const auto& diag : diags) {
          if (pretty_diagnostics) {
            printer->emit(diag);
          } else {
            oss << fmt::format("{:?}", diag);
          }
        }
      },
      [&](const caf::message& msg) {
        oss << "unexpected diagnostic format: " << caf::deep_to_string(msg);
      },
    }(ctx);
    return std::move(oss).str();
  }
  oss << "!! ";
  switch (category) {
    default:
      oss << "Unknown";
      render_default_ctx(oss, err.context());
      break;
    case caf::type_id_v<tenzir::ec>: {
      const auto code = static_cast<tenzir::ec>(err.code());
      oss << to_string(code);
      render_default_ctx(oss, err.context());
      break;
    }
    case caf::type_id_v<caf::pec>:
      oss << to_string(static_cast<caf::pec>(err.code()));
      render_default_ctx(oss, err.context());
      break;
    case caf::type_id_v<caf::sec>:
      oss << to_string(static_cast<caf::sec>(err.code()));
      render_default_ctx(oss, err.context());
      break;
    case caf::type_id_v<caf::exit_reason>:
      oss << to_string(static_cast<caf::exit_reason>(err.code()));
      render_default_ctx(oss, err.context());
      break;
  }
  return oss.str();
}

auto add_context_impl(const caf::error& error, std::string str) -> caf::error {
  if (!error)
    return error;
  if (error.category() == caf::type_id_v<tenzir::ec>
      && static_cast<tenzir::ec>(error.code()) == ec::diagnostic) {
    auto ctx = error.context();
    auto* inner = static_cast<diagnostic*>(nullptr);
    caf::message_handler{
      [&](diagnostic& diag) {
        inner = &diag;
      },
      [](const caf::message&) {},
    }(ctx);
    if (inner) {
      return caf::make_error(
        ec::diagnostic, std::move(*inner).modify().note(std::move(str)).done());
    }
  }
  if (!error.context()) {
    return caf::error{
      error.code(),
      error.category(),
      caf::make_message(std::move(str)),
    };
  }
  return caf::error{
    error.code(),
    error.category(),
    caf::message::concat(error.context(), caf::make_message(std::move(str))),
  };
}

} // namespace tenzir
