//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_table_slice.hpp>
#include <tenzir/fbs/aggregation.hpp>
#include <tenzir/flatbuffer.hpp>
#include <tenzir/logger.hpp>
#include <tenzir/tql2/eval.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::first_last {

namespace {

enum class mode {
  first,
  last,
};

template <mode Mode>
class first_last_instance final : public aggregation_instance {
public:
  explicit first_last_instance(ast::expression expr) : expr_{std::move(expr)} {
  }

  auto update(const table_slice& input, session ctx) -> void override {
    if (Mode == mode::first and not is<caf::none_t>(result_)) {
      return;
    }
    for (auto& arg : eval(expr_, input, ctx)) {
      if (is<null_type>(arg.type)) {
        continue;
      }
      if constexpr (Mode == mode::first) {
        for (int64_t i = 0; i < arg.array->length(); ++i) {
          if (arg.array->IsValid(i)) {
            result_ = materialize(view_at(*arg.array, i));
            break;
          }
        }
      } else {
        for (int64_t i = arg.array->length() - 1; i >= 0; --i) {
          if (arg.array->IsValid(i)) {
            result_ = materialize(view_at(*arg.array, i));
            break;
          }
        }
      }
    }
  }

  auto get() const -> data override {
    return result_;
  }

  auto save() const -> chunk_ptr override {
    auto fbb = flatbuffers::FlatBufferBuilder{};
    const auto fb_result = pack(fbb, result_);
    const auto fb_min_max = fbs::aggregation::CreateFirstLast(fbb, fb_result);
    fbb.Finish(fb_min_max);
    return chunk::make(fbb.Release());
  }

  auto restore(chunk_ptr chunk) noexcept -> bool override {
    constexpr auto name = Mode == mode::first ? "first" : "last";
    const auto fb
      = flatbuffer<fbs::aggregation::FirstLast>::make(std::move(chunk));
    if (not fb) {
      TENZIR_WARN("failed to restore `{}` aggregation instance: invalid "
                  "FlatBuffer",
                  name);
      return false;
    }
    const auto* fb_result = (*fb)->result();
    if (not fb_result) {
      TENZIR_WARN("failed to restore `{}` aggregation instance: missing field "
                  "`result`",
                  name);
      return false;
    }
    if (auto err = unpack(*fb_result, result_); err.valid()) {
      TENZIR_WARN("failed to restore `{}` aggregation instance: {}", name, err);
      return false;
    }
    return true;
  }

  auto reset() -> void override {
    result_ = {};
  }

private:
  ast::expression expr_ = {};
  data result_ = {};
};

template <mode Mode>
class plugin : public virtual aggregation_plugin {
public:
  auto name() const -> std::string override {
    return Mode == mode::first ? "first" : "last";
  };

  auto is_deterministic() const -> bool override {
    return true;
  }

  auto make_aggregation(function_invocation inv, session ctx) const
    -> failure_or<std::unique_ptr<aggregation_instance>> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name())
          .positional("x", expr, "any")
          .parse(inv, ctx));
    return std::make_unique<first_last_instance<Mode>>(std::move(expr));
  }
};

} // namespace

} // namespace tenzir::plugins::first_last

using namespace tenzir::plugins;

TENZIR_REGISTER_PLUGIN(first_last::plugin<first_last::mode::first>)
TENZIR_REGISTER_PLUGIN(first_last::plugin<first_last::mode::last>)
