//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::path {

namespace {

class file_name final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.file_name";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto arg = basic_series<string_type>{};
    auto success
      = function_argument_parser{"file_name"}.add(arg, "<path>").parse(inv, dh);
    if (not success) {
      return series::null(string_type{}, inv.length);
    }
    auto b = arrow::StringBuilder{};
    for (auto row = int64_t{0}; row < arg.array->length(); ++row) {
      if (arg.array->IsNull(row)) {
        (void)b.AppendNull();
        continue;
      }
      auto path = arg.array->GetView(row);
      // TODO: We don't know whether this is a windows/posix path.
      // TODO: Also, btw, string might not be a good type for paths.
      auto pos = path.find_last_of("/\\");
      // TODO: Trailing sep.
      if (pos == std::string::npos) {
        (void)b.Append(path);
      } else {
        (void)b.Append(path.substr(pos + 1));
      }
    }
    return series{string_type{}, b.Finish().ValueOrDie()};
  }
};

class parent_dir final : public function_plugin {
public:
  auto name() const -> std::string override {
    return "tql2.parent_dir";
  }

  auto eval(invocation inv, diagnostic_handler& dh) const -> series override {
    auto arg = basic_series<string_type>{};
    auto success
      = function_argument_parser{"parent_dir"}.add(arg, "<path>").parse(inv, dh);
    if (not success) {
      return series::null(string_type{}, inv.length);
    }
    auto b = arrow::StringBuilder{};
    for (auto row = int64_t{0}; row < arg.array->length(); ++row) {
      if (arg.array->IsNull(row)) {
        (void)b.AppendNull();
        continue;
      }
      auto path = arg.array->GetView(row);
      // TODO: We don't know whether this is a windows/posix path.
      // TODO: Also, btw, string might not be a good type for paths.
      auto pos = path.find_last_of("/\\");
      // TODO: Trailing sep.
      if (pos == std::string::npos) {
        // TODO: Or what?
        (void)b.Append(path);
      } else {
        (void)b.Append(path.substr(0, pos));
      }
    }
    return series{string_type{}, b.Finish().ValueOrDie()};
  }
};

} // namespace

} // namespace tenzir::plugins::path

TENZIR_REGISTER_PLUGIN(tenzir::plugins::path::file_name)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::path::parent_dir)
