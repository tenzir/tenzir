//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/exec/operator_impl.hpp>
#include <tenzir/exec/trampoline.hpp>
#include <tenzir/finalize_ctx.hpp>
#include <tenzir/ir.hpp>
#include <tenzir/pipeline.hpp>
#include <tenzir/plan/pipeline.hpp>
#include <tenzir/plugin.hpp>
#include <tenzir/substitute_ctx.hpp>
#include <tenzir/tql2/eval.hpp>

#include <caf/actor_from_state.hpp>
#include <caf/actor_registry.hpp>

namespace tenzir::plugins::head {

namespace {

class head_exec : public exec::operator_base<uint64_t> {
public:
  explicit head_exec(initializer init) : operator_base{std::move(init)} {
  }

  void next(const table_slice& slice) override {
    auto& remaining = state();
    auto take = std::min(remaining, slice.rows());
    push(subslice(slice, 0, take));
    remaining -= take;
    ready();
  }

  auto should_stop() -> bool override {
    return get_input_ended() or state() == 0;
  }
};

class head_bp final : public plan::operator_base {
public:
  explicit head_bp(int64_t count) : count_{count} {
  }

  auto name() const -> std::string override {
    return "head_bp";
  }

  auto spawn(spawn_args args) const -> exec::operator_actor override {
    return exec::spawn_operator<head_exec>(std::move(args), count_);
  }

  friend auto inspect(auto& f, head_bp& x) -> bool {
    return f.apply(x.count_);
  }

private:
  int64_t count_;
};

class head_ir final : public ir::operator_base {
public:
  head_ir() = default;

  explicit head_ir(ast::expression count) : count_{std::move(count)} {
  }

  auto name() const -> std::string override {
    return "head_ir";
  }

  auto substitute(substitute_ctx ctx, bool instantiate)
    -> failure_or<void> override {
    if (auto expr = try_as<ast::expression>(count_)) {
      TRY(expr->substitute(ctx));
      if (instantiate or expr->is_deterministic(ctx)) {
        TRY(auto value, const_eval(*expr, ctx));
        auto count = try_as<int64_t>(value);
        if (not count or *count < 0) {
          diagnostic::error("TODO").primary(*expr).emit(ctx);
          return failure::promise();
        }
        count_ = *count;
      }
    }
    return {};
  }

  auto infer_type(operator_type2 input, diagnostic_handler& dh) const
    -> failure_or<std::optional<operator_type2>> override {
    // TODO!
    (void)dh;
    TENZIR_ASSERT(input.is<table_slice>());
    return operator_type2{tag_v<table_slice>};
  }

  auto finalize(finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    (void)ctx;
    return std::make_unique<head_bp>(as<int64_t>(count_));
  }

  friend auto inspect(auto& f, head_ir& x) -> bool {
    return f.apply(x.count_);
  }

private:
  variant<ast::expression, int64_t> count_;
};

// FIXME: Remove this. Just here for completeness right now.
class manual_head_implementation {
public:
  manual_head_implementation(exec::operator_actor::pointer self,
                             exec::checkpoint_receiver_actor checkpoint_receiver,
                             exec::operator_shutdown_actor operator_shutdown,
                             exec::operator_stop_actor operator_stop,
                             base_ctx ctx, uint64_t count, uint64_t seen)
    : self_{self},
      checkpoint_receiver_{std::move(checkpoint_receiver)},
      operator_shutdown_{std::move(operator_shutdown)},
      operator_stop_{std::move(operator_stop)},
      ctx_{ctx},
      count_{count},
      seen_{seen} {
  }

  auto start(exec::handshake hs) -> caf::result<exec::handshake_response> {
    return match(
      std::move(hs.input),
      [&](exec::stream<void>) {
        TENZIR_TODO();
        return exec::handshake_response{};
      },
      [&](exec::stream<table_slice> s) {
        auto output = run(self_->observe(std::move(s), 10, 30))
                        .to_typed_stream("output-stream",
                                         std::chrono::milliseconds{1}, 1);
        return exec::handshake_response{std::move(output)};
      });
  }

  auto run(exec::observable<table_slice> input)
    -> exec::observable<table_slice> {
    // TODO: Does this guarantee sequencing?
    return input.concat_map(
      [this](exec::message<table_slice> msg) -> exec::observable<table_slice> {
        return match(
          msg,
          [&](table_slice slice) -> exec::observable<table_slice> {
            // TODO: If this would be async, how do we ensure that we finish
            // processing it before we serialize?
            if (seen_ == count_) {
              return self_->make_observable()
                .empty<exec::message<table_slice>>()
                .as_observable();
            }
            TENZIR_ASSERT(seen_ < count_);
            auto left = detail::narrow<uint64_t>(count_ - seen_);
            auto result = subslice(slice, 0, std::min(left, slice.rows()));
            seen_ += result.rows();
            auto output = std::vector<exec::message<table_slice>>{};
            output.emplace_back(std::move(result));
            if (seen_ == count_) {
              // TODO: Do we want to wait until the diagnostic arrived? How does
              // this interact with checkpointing?
              // Maybe: We don't care right now.
              // Alternative: If diagnostics are considered observable behavior
              // that needs to be rolled back or awaited for…
              diagnostic::warning("wow, we got so many events").emit(ctx_);
              // The first time we reach this, we also emit `exhausted`.
              output.emplace_back(exec::exhausted{});
              // And we also signal the previous operator that we are done.
            }
            return self_->make_observable()
              .from_container(std::move(output))
              .as_observable();
          },
          [&](exec::checkpoint checkpoint) -> exec::observable<table_slice> {
            // TODO: How do we ensure that we processed all previous messages?
            // Async save... Does this need to block incoming messages? Yes!
            auto buffer = std::vector<std::byte>{};
            auto serializer = caf::binary_serializer{buffer};
            // TODO: Just `seen_`?
            auto ok = serializer.apply(seen_);
            TENZIR_ASSERT(ok);
            auto chunk = chunk::make(std::move(buffer));
            return self_->mail(checkpoint, std::move(chunk))
              .request(checkpoint_receiver_, caf::infinite)
              .as_observable()
              .map([checkpoint](caf::unit_t) -> exec::message<table_slice> {
                return checkpoint;
              })
              .as_observable();
          },
          [&](exec::exhausted exhausted) -> exec::observable<table_slice> {
            TENZIR_WARN("head received exhausted");
            self_->mail(atom::done_v)
              .request(operator_shutdown_, caf::infinite)
              .then([] {});
            self_->mail(atom::stop_v)
              .request(operator_stop_, caf::infinite)
              .then([] {});
            return self_->make_observable()
              .just(exec::message<table_slice>{exhausted})
              .as_observable();
          });
      });
  }

  auto post_commit(exec::checkpoint checkpoint) -> caf::result<void> {
    (void)checkpoint;
    // Let's say we are kafka.
    // auto map = std::unordered_map<exec::checkpoint, int64_t>{};
    // auto it = map.find(checkpoint);
    // // TODO: Are we guaranteed that we previously tried to checkpoint this?
    // // TODO: Are we guaranteed that this gets called for every checkpoint?
    // TENZIR_ASSERT(it != map.end());
    // TENZIR_WARN("committing {}", it->second);
    // map.erase(it);
    return {};
  }

  auto stop() -> caf::result<void> {
    // The subsequent operator doesn't want any new input... Since `head` doesn't
    // create output out of thin air, we just tell our predecessor about it.
    // TODO: Or do we also from now on drop all output?
    TENZIR_TODO();
  }

  auto make_behavior() -> exec::operator_actor::behavior_type {
    return {
      [this](exec::handshake hs) -> caf::result<exec::handshake_response> {
        return start(std::move(hs));
      },
      [this](exec::checkpoint checkpoint) -> caf::result<void> {
        return post_commit(checkpoint);
      },
      [this](atom::stop) -> caf::result<void> {
        return stop();
      },
    };
  }

private:
  exec::operator_actor::pointer self_;
  exec::checkpoint_receiver_actor checkpoint_receiver_;
  exec::operator_shutdown_actor operator_shutdown_;
  exec::operator_stop_actor operator_stop_;
  base_ctx ctx_; // <-- assume we need a way to emit diags

  uint64_t count_; // <-- this should be immutable…
  uint64_t seen_;  // <-- this should be mutable…
};

class plugin final : public virtual operator_parser_plugin,
                     public virtual operator_compiler_plugin {
public:
  auto name() const -> std::string override {
    return "head";
  };

  auto signature() const -> operator_signature override {
    return {.transformation = true};
  }

  auto parse_operator(parser_interface& p) const -> operator_ptr override {
    auto parser = argument_parser{"head", "https://docs.tenzir.com/"
                                          "operators/head"};
    auto count = std::optional<uint64_t>{};
    parser.add(count, "<limit>");
    parser.parse(p);
    auto result = pipeline::internal_parse_as_operator(
      fmt::format("slice :{}", count.value_or(10)));
    if (not result) {
      diagnostic::error("failed to transform `head` into `slice` operator: {}",
                        result.error())
        .throw_();
    }
    return std::move(*result);
  }

  auto compile(ast::invocation inv, compile_ctx ctx) const
    -> failure_or<ir::operator_ptr> override {
    // TODO: Actual parsing.
    if (inv.args.size() > 1) {
      diagnostic::error("expected exactly one argument")
        .primary(inv.op)
        .emit(ctx);
      return failure::promise();
    }
    auto expr = inv.args.empty() ? ast::constant{10, location::unknown}
                                 : std::move(inv.args[0]);
    return std::make_unique<head_ir>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::head

TENZIR_REGISTER_PLUGIN(tenzir::plugins::head::plugin)
TENZIR_REGISTER_PLUGIN(tenzir::inspection_plugin<tenzir::ir::operator_base,
                                                 tenzir::plugins::head::head_ir>)
