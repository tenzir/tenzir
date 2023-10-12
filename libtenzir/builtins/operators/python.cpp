//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/as_bytes.hpp>
#include <tenzir/chunk.hpp>
#include <tenzir/concept/parseable/string/quoted_string.hpp>
#include <tenzir/concept/parseable/tenzir/pipeline.hpp>
#include <tenzir/error.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/si_literals.hpp>
#include <tenzir/type.hpp>

#include <boost/asio.hpp>
#include <boost/process.hpp>
#include <boost/process/v2.hpp>
#include <caf/detail/scope_guard.hpp>

#include <mutex>
#include <queue>
#include <thread>

namespace asio = boost::asio;
namespace bp = boost::process::v2;

namespace tenzir::plugins::python {
namespace {

using namespace tenzir::binary_byte_literals;

struct pipe {
  boost::asio::readable_pipe read;
  boost::asio::writable_pipe write;
};

struct PreservedFds : boost::process::detail::handler,
                      boost::process::detail::uses_handles {
  std::vector<int> fds;
  PreservedFds(std::vector<int> pfds) : fds(pfds) {
  }

  std::vector<int>& get_used_handles() {
    return fds;
  }

  class python_operator final
    : public schematic_operator<python_operator, pipe> {
  public:
    using state_type = pipe;

    python_operator() = default;

    std::string code_ = {};
    std::optional<bp::process> proc_ = {};
    asio::io_context ctx;
    asio::any_io_executor executor = ctx.get_executor();
    asio::local::datagram_protocol::socket socket1{ctx};
    asio::local::datagram_protocol::socket socket2{ctx};
    explicit python_operator(std::string code) : code_{std::move(code)} {
      asio::local::connect_pair(socket1, socket2);
      proc_ = bp::process(ctx, "python", {"-c", code_},
                          bp::process_stdio{{}, {}, {}});
    }

    auto initialize(const type&, operator_control_plane&) const
      -> caf::expected<state_type> override {
      auto result = caf::expected{state_type{asio::readable_pipe{executor},
                                             asio::writable_pipe{executor}}};
      asio::connect_pipe(result->read, result->write);
      return result;
    }

    auto process(table_slice slice, state_type& state) const
      -> table_slice override {
      // write_end.write(state.read.native_handle());
      return slice;
    }

    auto to_string() const -> std::string override {
      return code_;
    }

    auto name() const -> std::string override {
      return "python";
    }

    auto optimize(expression const& filter, event_order order) const
      -> optimize_result override {
      // Note: The `unordered` means that we do not necessarily return the first
      // `limit_` events.
      (void)filter, (void)order;
      return optimize_result{std::nullopt, event_order::unordered, copy()};
    }

    friend auto inspect(auto& f, python_operator& x) -> bool {
      return f.apply(x.code_);
    }

  private:
  };

  class plugin final : public virtual operator_plugin<python_operator> {
  public:
    auto signature() const -> operator_signature override {
      return {
        .source = true,
        .transformation = true,
      };
    }

    auto parse_operator(parser_interface& p) const -> operator_ptr override {
      auto command = std::string{};
      auto parser
        = argument_parser{"python", "https://docs.tenzir.com/next/"
                                    "operators/transformations/python"};
      parser.add(command, "<command>");
      parser.parse(p);
      return std::make_unique<python_operator>(std::move(command));
    }
  };

} // namespace
} // namespace tenzir::plugins::python

TENZIR_REGISTER_PLUGIN(tenzir::plugins::python::plugin)
