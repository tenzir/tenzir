//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2023 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/argument_parser.hpp>
#include <tenzir/compile_ctx.hpp>
#include <tenzir/exec/operator.hpp>
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

class Head final : public Operator<table_slice, table_slice> {
public:
  explicit Head(int64_t count) : remaining_{count} {
  }

  auto process(table_slice input, Push<table_slice>& push, OpCtx& ctx)
    -> Task<void> override {
    // TODO: Do we want to guarantee this?
    TENZIR_ASSERT(remaining_ > 0);
    auto result = tenzir::head(input, remaining_);
    TENZIR_ASSERT(result.rows() <= remaining_);
    remaining_ -= result.rows();
    co_await push(std::move(result));
  }

  auto state() -> OperatorState override {
    if (remaining_ == 0) {
      // TODO: We also want to declare that we'll produce no more output and
      // that we are ready to shutdown.
      return OperatorState::done;
    }
    return OperatorState::unspecified;
  }

  auto snapshot(Serde& serde) -> void override {
    serde("remaining", remaining_);
  }

private:
  int64_t remaining_;
};

#if 0
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
#else
class head_exec : public exec::operator_base {
public:
  [[maybe_unused]] static constexpr auto name = "head";

  head_exec(exec::operator_actor::pointer self, uint64_t count)
    : exec::operator_base{self}, remaining_{count} {
  }

  auto on_start() -> caf::result<void> override {
    TENZIR_WARN("head got start");
    // TODO: Is this the right place do perform this check?
    if (remaining_ == 0) {
      finish();
    }
    return {};
  }

  void on_push(table_slice slice) override {
    if (remaining_ == 0) {
      TENZIR_WARN("head drops {} events", slice.rows());
      return;
    }
    TENZIR_WARN("head receives {} events", slice.rows());
    auto out = tenzir::head(std::move(slice), remaining_);
    remaining_ -= out.rows();
    push(std::move(out));
    if (remaining_ == 0) {
      // TODO: Then what?
      finish();
    }
  }

  void on_push(chunk_ptr chunk) override {
    TENZIR_WARN("??");
    TENZIR_TODO();
  }

  auto serialize() -> chunk_ptr override {
    // TODO: What would the checkpoint say here?
    TENZIR_INFO("head got checkpoint");
    auto buf = caf::byte_buffer{};
    auto ser = caf::binary_serializer{buf};
    auto success = ser.apply(remaining_);
    TENZIR_ASSERT(success);
    TENZIR_INFO("head serialized into {} bytes", buf.size());
    return chunk::make(std::move(buf));
  }

  void on_done() override {
    finish();
  }

  void on_pull(uint64_t items) override {
    pull(std::min(items, remaining_));
  }

#  if 0
  auto make_behavior() -> exec::operator_actor::behavior_type {
    return {
      /// @see operator_actor
      [this](exec::connect_t connect) -> caf::result<void> {
        TENZIR_WARN("connecting heads");
        connect_ = std::move(connect);
        return {};
      },
      [this](atom::start) -> caf::result<void> {
        TENZIR_WARN("head got start");
        // TODO: Is this the right place do perform this check?
        if (remaining_ == 0) {
          done();
        }
        return {};
      },
      [this](atom::commit) -> caf::result<void> {
        return {};
      },
      /// @see upstream_actor
      [this](atom::pull, uint64_t items) -> caf::result<void> {
        TENZIR_WARN("??");
        TENZIR_TODO();
      },
      [this](atom::stop) -> caf::result<void> {
        // TODO: Anything else?
        done();
        return {};
      },
      /// @see downstream_actor
      [this](atom::push, table_slice slice) -> caf::result<void> {
        if (remaining_ == 0) {
          TENZIR_WARN("head drops {} events", slice.rows());
          return {};
        }
        TENZIR_WARN("head receives {} events", slice.rows());
        auto out = tenzir::head(std::move(slice), remaining_);
        remaining_ -= out.rows();
        self_->mail(atom::push_v, std::move(out))
          .request(connect_.downstream, caf::infinite)
          .then([] {});
        if (remaining_ == 0) {
          // TODO: Then what?
          done();
        }
        return {};
      },
      [this](atom::push, chunk_ptr chunk) -> caf::result<void> {
        TENZIR_WARN("??");
        TENZIR_TODO();
      },
      [this](atom::persist, exec::checkpoint check) -> caf::result<void> {
        // TODO: What would the checkpoint say here?
        TENZIR_INFO("head got checkpoint");
        auto buf = caf::byte_buffer{};
        auto ser = caf::binary_serializer{buf};
        auto success = ser.apply(remaining_);
        TENZIR_ASSERT(success);
        TENZIR_INFO("head serialized into {} bytes", buf.size());
        self_->mail(check, chunk::make(std::move(buf)))
          .request(connect_.checkpoint_receiver, caf::infinite)
          .then([this, check = std::move(check)] {
            // Only forward afterwards.
            self_->mail(atom::persist_v, check)
              .request(connect_.downstream, caf::infinite)
              .then([] {});
          });
        return {};
      },
      [this](atom::done) -> caf::result<void> {
        // Since we immediately forward all events, we can declare that we are
        // also done when upstream is done.
        done();
        return {};
      },
    };
  }
#  endif

private:
  uint64_t remaining_;
};
#endif

class head_plan final : public plan::operator_base {
public:
  head_plan() = default;

  explicit head_plan(int64_t count) : count_{count} {
  }

  auto name() const -> std::string override {
    return "head_plan";
  }

  auto spawn(plan::operator_spawn_args args) const
    -> exec::operator_actor override {
    auto remaining = count_;
    if (args.restore) {
      // We always send things for checkpointing.
      TENZIR_ASSERT(args.restore->chunk);
      auto f = caf::binary_deserializer{caf::const_byte_span{
        args.restore->chunk->data(), args.restore->chunk->size()}};
      auto ok = f.apply(remaining);
      TENZIR_ASSERT(ok);
      // TODO: Assert that we are at the end?
    }
    return args.sys.spawn(caf::actor_from_state<head_exec>, remaining);
  }

  auto spawn() && -> AnyOperator override {
    return Head{count_};
  }

  friend auto inspect(auto& f, head_plan& x) -> bool {
    return f.apply(x.count_);
  }

private:
  int64_t count_;
};

class head_ir final : public ir::operator_base {
public:
  head_ir() = default;

  explicit head_ir(location self, ast::expression count)
    : self_{self}, count_{std::move(count)} {
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

  auto infer_type(element_type_tag input, diagnostic_handler& dh) const
    -> failure_or<std::optional<element_type_tag>> override {
    // TODO: Refactor.
    if (not input.is<table_slice>()) {
      diagnostic::error("expected events, got {}", input)
        .primary(main_location())
        .emit(dh);
      return failure::promise();
    }
    return element_type_tag{tag_v<table_slice>};
  }

  auto finalize(element_type_tag input,
                finalize_ctx ctx) && -> failure_or<plan::pipeline> override {
    TENZIR_UNUSED(ctx);
    TENZIR_ASSERT(input.is<table_slice>());
    return std::make_unique<head_plan>(as<int64_t>(count_));
  }

  auto main_location() const -> location override {
    return self_;
  }

  friend auto inspect(auto& f, head_ir& x) -> bool {
    return f.object(x).fields(f.field("self", x.self_),
                              f.field("count", x.count_));
  }

private:
  location self_;
  variant<ast::expression, int64_t> count_;
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
    return std::make_unique<head_ir>(inv.op.get_location(), std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::head

TENZIR_REGISTER_PLUGIN(tenzir::plugins::head::plugin)
TENZIR_REGISTER_PLUGIN(
  tenzir::inspection_plugin<tenzir::plan::operator_base,
                            tenzir::plugins::head::head_plan>)
TENZIR_REGISTER_PLUGIN(tenzir::inspection_plugin<tenzir::ir::operator_base,
                                                 tenzir::plugins::head::head_ir>)
