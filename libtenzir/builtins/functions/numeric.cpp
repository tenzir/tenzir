//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/tql2/plugin.hpp>

#include <random>

namespace tenzir::plugins::numeric {

namespace {

using namespace tql2;

class round final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.round";
  }

  auto eval(const ast::function_call& self, size_t length,
            std::vector<series> args, diagnostic_handler& dh) const
    -> series override {
    if (args.size() != 1) {
      diagnostic::error("`round` expects exactly one argument")
        .primary(self.get_location())
        .emit(dh);
    }
    if (args.empty()) {
      auto b = arrow::Int64Builder{};
      (void)b.AppendNulls(detail::narrow<int64_t>(length));
      return series{int64_type{}, b.Finish().ValueOrDie()};
    }
    auto f = detail::overload{
      [](const arrow::Int64Array& arg) {
        return std::make_shared<arrow::Int64Array>(arg.data());
      },
      [&](const arrow::DoubleArray& arg) {
        auto b = arrow::Int64Builder{};
        for (auto row = int64_t{0}; row < detail::narrow<int64_t>(length);
             ++row) {
          if (arg.IsNull(row)) {
            (void)b.AppendNull();
          } else {
            // TODO: NaN, inf, ...
            auto result = std::llround(arg.Value(row));
            (void)b.Append(result);
          }
        }
        auto ret = std::shared_ptr<arrow::Int64Array>{};
        (void)b.Finish(&ret);
        return ret;
      },
      [&](const auto& arg) -> std::shared_ptr<arrow::Int64Array> {
        TENZIR_UNUSED(arg);
        diagnostic::warning("`round` expects `int64` or `double`, got `{}`",
                            args[0].type.kind())
          // TODO: Wrong location.
          .primary(self.get_location())
          .emit(dh);
        auto b = arrow::Int64Builder{};
        (void)b.AppendNulls(detail::narrow<int64_t>(length));
        auto ret = std::shared_ptr<arrow::Int64Array>{};
        (void)b.Finish(&ret);
        return ret;
      },
    };
    return series{int64_type{}, caf::visit(f, *args[0].array)};
  }
};

class sqrt final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.sqrt";
  }

  auto eval(const ast::function_call& self, size_t length,
            std::vector<series> args, diagnostic_handler& dh) const
    -> series override {
    if (args.size() != 1) {
      diagnostic::error("`sqrt` expects exactly one argument")
        .primary(self.get_location())
        .emit(dh);
    }
    if (args.empty()) {
      auto b = arrow::DoubleBuilder{};
      (void)b.AppendNulls(detail::narrow<int64_t>(length));
      return series{double_type{}, b.Finish().ValueOrDie()};
    }
    auto compute = [&](const arrow::DoubleArray& x) {
      auto b = arrow::DoubleBuilder{};
      (void)b.Reserve(x.length());
#if 0
      // TODO: This is probably UB.
      auto alloc = arrow::AllocateBuffer(x.length() * sizeof(double));
      TENZIR_ASSERT(alloc.ok());
      auto buffer = std::move(*alloc);
      TENZIR_ASSERT(buffer);
      auto target = new (buffer->mutable_data()) double[x.length()];
      auto begin = x.raw_values();
      auto end = begin + x.length();
      auto null_alloc = arrow::AllocateBuffer((x.length() + 7) / 8);
      TENZIR_ASSERT(null_alloc.ok());
      auto null_buffer = std::move(*null_alloc);
      TENZIR_ASSERT(null_buffer);
      auto null_target = null_buffer->mutable_data();
      if (auto nulls = x.null_bitmap()) {
        std::memcpy(null_target, nulls->data(), nulls->size());
      } else {
        std::memset(null_target, 0xFF, (x.length() + 7) / 8);
      }
      while (begin != end) {
        auto val = *begin;
        if (val < 0.0) [[unlikely]] {
          auto mask = 0xFF ^ (0x01 << ((begin - end) % 8));
          null_target[(begin - end) / 8] &= mask;
          // TODO: Emit warning/error?
        }
        *target = std::sqrt(*begin);
        ++begin;
        ++target;
      }
      return std::make_shared<arrow::DoubleArray>(x.length(), std::move(buffer),
                                                  std::move(null_buffer));
#else
      // TODO
      for (auto y : x) {
        if (not y) {
          // TODO: Warning?
          b.UnsafeAppendNull();
          continue;
        }
        auto z = *y;
        if (z < 0.0) {
          // TODO: Warning?
          b.UnsafeAppendNull();
          continue;
        }
        b.UnsafeAppend(std::sqrt(z));
      }
#endif
      auto result = std::shared_ptr<arrow::DoubleArray>{};
      (void)b.Finish(&result);
      return result;
    };
    auto f = detail::overload{
      [&](const arrow::DoubleArray& x) {
        return compute(x);
      },
      [&](const arrow::Int64Array& x) {
        // TODO: Conversation should be automatic (if not part of the kernel).
        auto b = arrow::DoubleBuilder{};
        (void)b.Reserve(x.length());
        for (auto y : x) {
          if (y) {
            b.UnsafeAppend(static_cast<double>(*y));
          } else {
            b.UnsafeAppendNull();
          }
        }
        auto conv = std::shared_ptr<arrow::DoubleArray>{};
        (void)b.Finish(&conv);
        return compute(*conv);
      },
      [&](const auto& arg) {
        TENZIR_UNUSED(arg);
        diagnostic::warning("`sqrt` expects `double`, got `{}`",
                            args[0].type.kind())
          // TODO: Wrong location.
          .primary(self.get_location())
          .emit(dh);
        auto b = arrow::DoubleBuilder{};
        (void)b.AppendNulls(detail::narrow<int64_t>(length));
        auto ret = std::shared_ptr<arrow::DoubleArray>{};
        (void)b.Finish(&ret);
        return ret;
      },
    };
    return series{double_type{}, caf::visit(f, *args[0].array)};
  }
};

class random final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.random";
  }

  auto eval(const ast::function_call& self, size_t length,
            std::vector<series> args, diagnostic_handler& dh) const
    -> series override {
    if (not args.empty()) {
      diagnostic::error("`random` expects no arguments")
        .primary(self.get_location())
        .emit(dh);
    }
    auto b = arrow::DoubleBuilder{};
    (void)b.Reserve(length);
    // TODO
    auto engine = std::default_random_engine{std::random_device{}()};
    auto dist = std::uniform_real_distribution<double>{0.0, 1.0};
    for (auto i = size_t{0}; i < length; ++i) {
      b.UnsafeAppend(dist(engine));
    }
    return {double_type{}, b.Finish().ValueOrDie()};
  }
};

} // namespace

} // namespace tenzir::plugins::numeric

TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::round)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::sqrt)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::random)
