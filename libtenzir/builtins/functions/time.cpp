//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/concept/parseable/tenzir/time.hpp>
#include <tenzir/series_builder.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::time_ {

namespace {

using namespace tql2;

class time_ final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.time";
  }

  auto eval(const ast::function_call& self, size_t length,
            std::vector<series> args, diagnostic_handler& dh) const
    -> series override {
    TENZIR_ASSERT(args.size() == 1);
    if (auto time_array
        = std::dynamic_pointer_cast<arrow::TimestampArray>(args[0].array)) {
      return series{time_type{}, std::move(time_array)};
    }
    auto b = arrow::TimestampBuilder{
      std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO),
      arrow::default_memory_pool()};
    auto arg = caf::get_if<arrow::StringArray>(&*args[0].array);
    if (not arg) {
      // TODO
      diagnostic::warning("expected string argument, but got `{}`",
                          args[0].type.kind())
        .primary(self.args[0].get_location())
        .emit(dh);
      b.AppendNulls(length);
      return series{null_type{}, b.Finish().ValueOrDie()};
    }
    TENZIR_ASSERT(arg);
    (void)b.Reserve(arg->length());
    for (auto i = 0; i < arg->length(); ++i) {
      if (arg->IsNull(i)) {
        (void)b.UnsafeAppendNull();
        continue;
      }
      auto result = tenzir::time{};
      if (parsers::time(arg->GetView(i), result)) {
        b.UnsafeAppend(result.time_since_epoch().count());
      } else {
        // TODO: ?
        b.UnsafeAppendNull();
      }
    }
    return series{time_type{}, b.Finish().ValueOrDie()};
  }
};

class seconds_since_epoch final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.seconds_since_epoch";
  }

  auto eval(const ast::function_call& self, size_t length,
            std::vector<series> args, diagnostic_handler& dh) const
    -> series override {
    TENZIR_ASSERT(args.size() == 1);
    auto arg = caf::get_if<arrow::TimestampArray>(&*args[0].array);
    TENZIR_ASSERT(arg);
    auto& ty = caf::get<arrow::TimestampType>(*arg->type());
    TENZIR_ASSERT(ty.unit() == arrow::TimeUnit::NANO);
    TENZIR_ASSERT(ty.timezone().empty());
    auto factor = 1000 * 1000 * 1000;
    auto b = arrow::DoubleBuilder{};
    (void)b.Reserve(arg->length());
    for (auto i = 0; i < arg->length(); ++i) {
      if (arg->IsNull(i)) {
        (void)b.UnsafeAppendNull();
        continue;
      }
      auto val = arg->Value(i);
      auto pre = static_cast<double>(val / factor);
      auto post = static_cast<double>(val % factor) / factor;
      (void)b.UnsafeAppend(pre + post);
    }
    return series{double_type{}, b.Finish().ValueOrDie()};
  }
};

class now final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.now";
  }

  auto eval(const ast::function_call& self, size_t length,
            std::vector<series> args, diagnostic_handler& dh) const
    -> series override {
    if (not args.empty()) {
      // TODOs
      diagnostic::error("`now` does not expect any arguments")
        .primary(self.args.front().get_location())
        .emit(dh);
    }
    auto result = time{time::clock::now()};
    auto b = series_builder{type{time_type{}}};
    for (auto i = size_t{0}; i < length; ++i) {
      b.data(result);
    }
    return b.finish_assert_one_array();
  }
};

} // namespace

} // namespace tenzir::plugins::time_

TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::time_)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::seconds_since_epoch)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::time_::now)
