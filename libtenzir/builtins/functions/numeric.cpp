//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/series_builder.hpp>
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

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    if (inv.args.size() != 1) {
      diagnostic::error("`round` expects exactly one argument")
        .primary(inv.self.get_location())
        .emit(dh);
    }
    if (inv.args.empty()) {
      auto b = arrow::Int64Builder{};
      (void)b.AppendNulls(detail::narrow<int64_t>(inv.length));
      return series{int64_type{}, b.Finish().ValueOrDie()};
    }
    auto f = detail::overload{
      [](const arrow::Int64Array& arg) {
        return std::make_shared<arrow::Int64Array>(arg.data());
      },
      [&](const arrow::DoubleArray& arg) {
        auto b = arrow::Int64Builder{};
        for (auto row = int64_t{0}; row < detail::narrow<int64_t>(inv.length);
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
                            inv.args[0].type.kind())
          // TODO: Wrong location.
          .primary(inv.self.get_location())
          .emit(dh);
        auto b = arrow::Int64Builder{};
        (void)b.AppendNulls(detail::narrow<int64_t>(inv.length));
        auto ret = std::shared_ptr<arrow::Int64Array>{};
        (void)b.Finish(&ret);
        return ret;
      },
    };
    return series{int64_type{}, caf::visit(f, *inv.args[0].array)};
  }
};

class sqrt final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.sqrt";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    if (inv.args.size() != 1) {
      diagnostic::error("`sqrt` expects exactly one argument")
        .primary(inv.self.get_location())
        .emit(dh);
    }
    if (inv.args.empty()) {
      auto b = arrow::DoubleBuilder{};
      (void)b.AppendNulls(detail::narrow<int64_t>(inv.length));
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
                            inv.args[0].type.kind())
          // TODO: Wrong location.
          .primary(inv.self.get_location())
          .emit(dh);
        auto b = arrow::DoubleBuilder{};
        (void)b.AppendNulls(detail::narrow<int64_t>(inv.length));
        auto ret = std::shared_ptr<arrow::DoubleArray>{};
        (void)b.Finish(&ret);
        return ret;
      },
    };
    return series{double_type{}, caf::visit(f, *inv.args[0].array)};
  }
};

class random final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.random";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    if (not inv.args.empty()) {
      diagnostic::error("`random` expects no arguments")
        .primary(inv.self.get_location())
        .emit(dh);
    }
    auto b = arrow::DoubleBuilder{};
    (void)b.Reserve(inv.length);
    // TODO
    auto engine = std::default_random_engine{std::random_device{}()};
    auto dist = std::uniform_real_distribution<double>{0.0, 1.0};
    for (auto i = int64_t{0}; i < inv.length; ++i) {
      b.UnsafeAppend(dist(engine));
    }
    return {double_type{}, b.Finish().ValueOrDie()};
  }
};

class sum_instance final : public aggregation_instance {
public:
  auto add(series values) -> std::string override {
    // TODO: values.type
    auto error = std::string{};
    auto f = detail::overload{
      [&](const arrow::Int64Array& array) {
        // Double => Double
        // UInt64 => Int64
        // Int64 => Int64
        TENZIR_UNUSED(array);
        TENZIR_TODO();
      },
      [&](const arrow::UInt64Array& array) {
        // Double => Double
        // UInt64 => UInt64Array
        // Int64 => Int64
        TENZIR_UNUSED(array);
        TENZIR_TODO();
      },
      [&](const arrow::DoubleArray& array) {
        // * => Double
        auto sum = sum_.match([](auto sum) {
          return static_cast<double>(sum);
        });
        for (auto row = int64_t{0}; row < array.length(); ++row) {
          if (array.IsNull(row)) {
            // TODO: What do we do here?
          } else {
            sum += array.Value(row);
          }
        }
        sum_ = sum;
      },
      [&](auto&) {
        error = fmt::format("expected integer or double, but got {}",
                            values.type.kind());
      },
    };
    caf::visit(f, *values.array);
    return error;
  }

  auto finish() -> data override {
    return sum_.match([](auto sum) {
      return data{sum};
    });
  }

private:
  variant<int64_t, uint64_t, double> sum_ = uint64_t{0};
};

class sum final : public tql2::aggregation_function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.sum";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    if (inv.args.size() != 1) {
      diagnostic::error("function `sum` expects exactly one argument")
        .primary(inv.self.get_location())
        .emit(dh);
      return series::null(null_type{}, inv.length);
    }
    auto& arg = inv.args[0];
    auto list = dynamic_cast<arrow::ListArray*>(&*arg.array);
    if (not list) {
      diagnostic::warning("function `sum` expects a list")
        .primary(inv.self.args[0].get_location())
        .emit(dh);
      return series::null(null_type{}, inv.length);
    }
    auto inner_type = caf::get<list_type>(arg.type).value_type();
    auto b = series_builder{};
    for (auto i = int64_t{0}; i < list->length(); ++i) {
      if (list->IsNull(i)) {
        b.null();
        continue;
      }
      auto instance = sum_instance{};
      auto err = instance.add(series{inner_type, list->value_slice(i)});
      if (not err.empty()) {
        diagnostic::warning("{}", err)
          .primary(inv.self.fn.get_location())
          .emit(dh);
      }
      b.data(instance.finish());
    }
    auto result = b.finish();
    // TODO: Can this happen? If so, what do we want to do? Do we need to adjust
    // the plugin interface?
    if (result.size() != 1) {
      diagnostic::warning("internal error in `sum`: unexpected array count: {}",
                          result.size())
        .primary(inv.self.fn.get_location())
        .emit(dh);
      return series::null(null_type{}, inv.length);
    }
    return std::move(result[0]);
  }

  auto make_aggregation() const
    -> std::unique_ptr<aggregation_instance> override {
    return std::make_unique<sum_instance>();
  }
};

class count_instance final : public aggregation_instance {
public:
  auto add(series values) -> std::string override {
    count += values.array->length() - values.array->null_count();
    return "";
  }

  auto finish() -> data override {
    return count;
  }

private:
  int64_t count = 0;
};

class count final : public tql2::aggregation_function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.count";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    diagnostic::error("this is currently only an aggregation function")
      .primary(inv.self.get_location())
      .emit(dh);
    // TODO
    auto b = arrow::NullBuilder{};
    (void)b.AppendNulls(inv.length);
    return {null_type{}, b.Finish().ValueOrDie()};
  }

  auto make_aggregation() const
    -> std::unique_ptr<aggregation_instance> override {
    return std::make_unique<count_instance>();
  }
};

} // namespace

} // namespace tenzir::plugins::numeric

TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::round)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::sqrt)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::random)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::sum)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::numeric::count)
