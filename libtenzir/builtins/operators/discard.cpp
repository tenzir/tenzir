//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/compile_ctx.hpp"
#include "tenzir/exec/checkpoint.hpp"
#include "tenzir/finalize_ctx.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/substitute_ctx.hpp"
#include "tenzir/view3.hpp"

#include <tenzir/argument_parser.hpp>
#include <tenzir/exec/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/binary_deserializer.hpp>
#include <caf/scheduled_actor/flow.hpp>

namespace tenzir::plugins::discard {

namespace {

class discard_operator final : public crtp_operator<discard_operator> {
public:
  discard_operator() = default;

  auto name() const -> std::string override {
    return "discard";
  }

  template <operator_input_batch Batch>
  auto operator()(generator<Batch> input) const -> generator<std::monostate> {
    for (auto&& slice : input) {
      (void)slice;
      co_yield {};
    }
  }

  auto internal() const -> bool override {
    return true;
  }

  auto optimize(expression const& filter, event_order order) const
    -> optimize_result override {
    (void)filter, (void)order;
    return optimize_result{std::nullopt, event_order::unordered, copy()};
  }

  friend auto inspect(auto& f, discard_operator& x) -> bool {
    return f.object(x).fields();
  }
};

template <class Derived>
struct serializable_actor {
  auto deserialize(const chunk_ptr& chunk) -> void {
    if (not chunk) {
      return;
    }
    auto bytes = as_bytes(chunk);
    auto deserializer = caf::binary_deserializer{
      caf::const_byte_span{bytes.data(), bytes.size()}};
    const auto ok = deserializer.apply(static_cast<Derived&>(*this));
    TENZIR_ASSERT(ok);
  }

  auto serialize() const -> chunk_ptr {
    auto buffer = std::vector<std::byte>{};
    auto serializer = caf::binary_serializer{buffer};
    const auto ok = serializer.apply(
      static_cast<Derived&>(const_cast<serializable_actor&>(*this)));
    TENZIR_ASSERT(ok);
    return chunk::make(std::move(buffer));
  }
};

#if 0
class discard_exec : public serializable_actor<discard_exec> {
public:
  explicit discard_exec(exec::operator_actor::pointer self,
                        exec::checkpoint_receiver_actor checkpoint_receiver,
                        exec::shutdown_handler_actor shutdown_handler)
    : self_{self},
      checkpoint_receiver_{std::move(checkpoint_receiver)},
      shutdown_handler_{std::move(shutdown_handler)} {
    // TODO: Does this make sense?
    deserialize(chunk_ptr{});
  }

  auto make_behavior() -> exec::operator_actor::behavior_type {
    return {
      [this](exec::handshake hs) -> caf::result<exec::handshake_response> {
        auto out
          = self_->observe(as<exec::stream<table_slice>>(hs.input), 30, 10)
              .concat_map([this](exec::message<table_slice> msg)
                            -> exec::observable<void> {
                return match(
                  msg,
                  [&](exec::checkpoint checkpoint) -> exec::observable<void> {
                    TENZIR_WARN("got checkpoint");
                    return self_->mail(checkpoint, serialize())
                      .request(checkpoint_receiver_, caf::infinite)
                      .as_observable()
                      .map([checkpoint](caf::unit_t) {
                        // precommit
                        TENZIR_WARN("pre-commit done");
                        return exec::message<void>{checkpoint};
                      })
                      .as_observable();
                  },
                  [&](exec::exhausted) -> exec::observable<void> {
                    TENZIR_WARN("got exhausted");
                    self_->mail(atom::done_v)
                      .request(shutdown_handler_, caf::infinite)
                      .then(
                        []() {

                        },
                        [](caf::error err) {
                          TENZIR_WARN("ERROR: {}", err);
                        });
                    return self_->make_observable()
                      .empty<exec::message<void>>()
                      .as_observable();
                  },
                  [&](const table_slice& slice) -> exec::observable<void> {
                    TENZIR_WARN("discard got table slice with {} rows",
                                slice.rows());
                    // FIXME: remove this again
                    for (auto row : values3(slice)) {
                      fmt::println("{}", row);
                    }
                    return self_->make_observable()
                      .empty<exec::message<void>>()
                      .as_observable();
                  });
              })
              .do_on_complete([] {
                TENZIR_WARN("discard completed");
              })
              .to_typed_stream("discard-exec", std::chrono::milliseconds{1}, 1);
        return {std::move(out)};
      },
      // post-commit
      [](exec::checkpoint checkpoint) -> caf::result<void> {
        TENZIR_UNUSED(checkpoint);
        TENZIR_WARN("discard post-commit");
        return {};
      },
      [](atom::stop) -> caf::result<void> {
        TENZIR_TODO();
      },
    };
  }

  friend auto inspect(auto& f, discard_exec& x) -> bool {
    // NOTE: must not list self
    return f.object(x).fields();
  }

private:
  exec::operator_actor::pointer self_;
  exec::checkpoint_receiver_actor checkpoint_receiver_;
  exec::shutdown_handler_actor shutdown_handler_;
};
#else
class discard_exec {
public:
  [[maybe_unused]] static constexpr auto name = "discard";

  explicit discard_exec(exec::operator_actor::pointer self) : self_{self} {
  }

  auto make_behavior() -> exec::operator_actor::behavior_type {
    return {
      /// @see operator_actor
      [this](exec::connect_t connect) -> caf::result<void> {
        // TODO: Connector our operators already before?
        TENZIR_WARN("connecting discard");
        connect_ = std::move(connect);
        return {};
      },
      [this](atom::start) -> caf::result<void> {
        // TODO: Do we do anything?
        TENZIR_WARN("discard got start");
        return {};
      },
      [this](atom::commit) -> caf::result<void> {
        return {};
      },
      /// @see upstream_actor
      [this](atom::pull, uint64_t items) -> caf::result<void> {
        TENZIR_TODO();
      },
      [this](atom::stop) -> caf::result<void> {
        // TODO: Anything else?
        TENZIR_TODO();
      },
      /// @see downstream_actor
      [](atom::push, table_slice slice) -> caf::result<void> {
        // TODO: Anything else?
        TENZIR_WARN("discard got {} events", slice.rows());
        return {};
      },
      [](atom::push, chunk_ptr chunk) -> caf::result<void> {
        // TODO: Anything else?
        TENZIR_ASSERT(chunk);
        TENZIR_WARN("discard got {} bytes", chunk->size());
        return {};
      },
      [this](atom::persist, exec::checkpoint check) -> caf::result<void> {
        TENZIR_INFO("discard got checkpoint");
        // TODO: Nothing to do, right?
        self_->mail(atom::persist_v, check)
          .request(connect_.downstream, caf::infinite)
          .then([] {});
        return {};
      },
      [this](atom::done) -> caf::result<void> {
        // Cool, we are done too.
        TENZIR_WARN("discard got done");
        self_->mail(atom::done_v)
          .request(connect_.downstream, caf::infinite)
          .then([] {},
                [](caf::error) {
                  TENZIR_TODO();
                });
        self_->mail(atom::stop_v)
          .request(connect_.upstream, caf::infinite)
          .then([] {},
                [](caf::error) {
                  TENZIR_TODO();
                });
        self_->mail(atom::shutdown_v)
          .request(connect_.shutdown, caf::infinite)
          .then([] {},
                [](caf::error) {
                  TENZIR_TODO();
                });
        return {};
      },
    };
  }

private:
  exec::operator_actor::pointer self_;
  exec::connect_t connect_;
};
#endif

class discard_bp final : public plan::operator_base {
public:
  discard_bp() = default;

  auto name() const -> std::string override {
    return "discard_bp";
  }

  auto spawn(plan::operator_spawn_args args) const
    -> exec::operator_actor override {
    return args.sys.spawn(caf::actor_from_state<discard_exec>);
  }

  friend auto inspect(auto& f, discard_bp& x) -> bool {
    return f.object(x).fields();
  }
};

class discard_ir final : public ir::operator_base {
public:
  discard_ir() = default;

  auto name() const -> std::string override {
    return "discard_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    TENZIR_UNUSED(ctx, instantiate);
    return {};
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    TENZIR_UNUSED(ctx);
    return std::make_unique<discard_bp>();
  }

  auto infer_type(element_type_tag input, diagnostic_handler&) const
    -> failure_or<std::optional<element_type_tag>> override {
    TENZIR_ASSERT(input == tag_v<table_slice>);
    return tag_v<void>;
  }

  friend auto inspect(auto& f, discard_ir& x) -> bool {
    return f.object(x).fields();
  }
};

class plugin final : public virtual operator_plugin<discard_operator>,
                     public virtual operator_factory_plugin,
                     public virtual operator_compiler_plugin {
public:
  auto signature() const -> operator_signature override {
    return {.sink = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"discard", "https://docs.tenzir.com/"
                                             "operators/discard"};
    parser.parse(p);
    return std::make_unique<discard_operator>();
  }

  auto make(invocation inv, session ctx) const
    -> failure_or<operator_ptr> override {
    argument_parser2::operator_("discard").parse(inv, ctx).ignore();
    return std::make_unique<discard_operator>();
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::operator_ptr> override {
    // TODO
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(inv.args.empty());
    return std::make_unique<discard_ir>();
  }
};

} // namespace

} // namespace tenzir::plugins::discard

TENZIR_REGISTER_PLUGIN(tenzir::plugins::discard::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::ir::operator_base,
                            tenzir::plugins::discard::discard_ir>);
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::plan::operator_base,
                            tenzir::plugins::discard::discard_bp>);
