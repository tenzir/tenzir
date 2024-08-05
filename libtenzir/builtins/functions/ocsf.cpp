//    _   _____   __________
//   | | / / _ | / __/_  __/     Visibility
//   | |/ / __ |_\ \  / /          Across
//   |___/_/ |_/___/ /_/       Space and Time
//
// SPDX-FileCopyrightText: (c) 2024 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include <tenzir/arrow_utils.hpp>
#include <tenzir/concept/parseable/tenzir/si.hpp>
#include <tenzir/detail/narrow.hpp>
#include <tenzir/detail/string_literal.hpp>
#include <tenzir/tql2/plugin.hpp>

namespace tenzir::plugins::ocsf {

namespace {
struct ocsf_pair {
  std::string_view name;
  int64_t id;
};

// OCSF category name <-> ID
constexpr ocsf_pair category_map[] = {
  {"System Activity", 1},
  {"Findings", 2},
  {"Identity & Access Management", 3},
  {"Network Activity", 4},
  {"Discovery", 5},
  {"Application Activity", 6},
  {"Remediation", 7},
};
// OCSF class name <-> ID
constexpr ocsf_pair class_map[]{
  {"File System Activity", 1001},
  {"Kernel Extension Activity", 1002},
  {"Kernel Activity", 1003},
  {"Memory Activity", 1004},
  {"Module Activity", 1005},
  {"Scheduled Job Activity", 1006},
  {"Process Activity", 1007},
  {"Event Log Activity", 1008},

  {"Security Finding", 2001},
  {"Vulnerability Finding", 2002},
  {"Compliance Finding", 2003},
  {"Detection Finding", 2004},
  {"Incident Finding", 2005},
  {"Data Security Finding", 2006},

  {"Account Change", 3001},
  {"Authentication", 3002},
  {"Authorize Session", 3003},
  {"Entity Management", 3004},
  {"User Access Management", 3005},
  {"Group Management", 3006},

  {"Network Activity", 4001},
  {"HTTP Activity", 4002},
  {"DNS Activity", 4003},
  {"DHCP Activity", 4004},
  {"RDP Activity", 4005},
  {"SMB Activity", 4006},
  {"SSH Activity", 4007},
  {"FTP Activity", 4008},
  {"Email Activity", 4009},
  {"Network File Activity", 4010},
  {"Email File Activity", 4011},
  {"Email URL Activity", 4012},
  {"NTP Activity", 4013},
  {"Tunnel Activity", 4014},

  {"Device Inventory Info", 5001},
  {"Device Config State", 5002},
  {"User Inventory Info", 5003},
  {"Operating System Patch State", 5004},
  {"Kernel Object Query", 5006},
  {"File Query", 5007},
  {"Folder Query", 5008},
  {"Admin Group Query", 5009},
  {"Job Query", 5010},
  {"Module Query", 5011},
  {"Network Connection Query", 5012},
  {"Networks Query", 5013},
  {"Peripheral Device Query", 5014},
  {"Process Query", 5015},
  {"Service Query", 5016},
  {"User Session Query", 5017},
  {"User Query", 5018},
  {"Device Config State Change", 5019},
  {"Software Inventory Info", 5020},

  {"Web Resources Activity", 6001},
  {"Application Lifecycle", 6002},
  {"API Activity", 6003},
  {"Web Resource Access Activity", 6004},
  {"Datastore Activity", 6005},
  {"File Hosting Activity", 6006},
  {"Scan Activity", 6007},

  {"Remediation Activity", 7001},
  {"File Remediation Activity", 7002},
  {"Process Remediation Activity", 7003},
  {"Network Remediation Activity", 7004},
};

auto name_to_id(std::span<const ocsf_pair> lookup,
                std::string_view key) -> std::optional<int64_t> {
  for (const auto& [category, id] : lookup) {
    if (key == category) {
      return id;
    }
  }
  return std::nullopt;
};
auto id_to_name(std::span<const ocsf_pair> lookup,
                int64_t key) -> std::optional<std::string_view> {
  for (const auto& [category, id] : lookup) {
    if (key == id) {
      return category;
    }
  }
  return std::nullopt;
};

/// @brief generic mapping function that supports OCSF `Event Type String` <->
/// `OCSF UID`
/// @tparam Operation the mapping function to use
/// @tparam Output_Type the tenzir type the operation produces
/// @tparam Input_Types... The input types that the operation accepts
template <auto Operation, typename Output_Type, typename... Input_Types>
class generic_mapping_plugin final : public function_plugin {
public:
  auto name() const -> std::string override {
    return name_;
  }

  generic_mapping_plugin(std::string name, std::string input_meta,
                         const std::span<const ocsf_pair> map,
                         std::string warning_text)
    : name_{std::move(name)},
      input_meta_{std::move(input_meta)},
      map_{map},
      warning_text_{std::move(warning_text)} {
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(name_)
          .add(expr, fmt::format("<{}>", input_meta_))
          .parse(inv, ctx));
    return function_use::make(
      [&, expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto arg = eval(expr);
        auto f = detail::overload{
          [&](const arrow::NullArray& arg) {
            return series::null(Output_Type{}, arg.length());
          },
          [&](const type_to_arrow_array_t<Input_Types>& arg) {
            using input_data_type = type_to_data_t<Input_Types>;
            std::optional<input_data_type> warn_value;
            auto b = type_to_arrow_builder_t<Output_Type>{};
            check(b.Reserve(arg.length()));
            for (auto i = int64_t{0}; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto in = arg.GetView(i);
              // handle numeric bounds for uint inputs
              if constexpr (std::same_as<input_data_type, uint64_type>) {
                if (in > static_cast<uint64_t>(
                      std::numeric_limits<int64_t>::max())) {
                  warn_value = in;
                  check(b.AppendNull());
                  continue;
                }
              }
              auto out = Operation(map_, in);
              if (out) {
                check(b.Append(*out));
              } else {
                if (not warn_value) {
                  warn_value = in;
                }
                check(b.AppendNull());
              }
            }
            if (warn_value) {
              diagnostic::warning("invalid {}", warning_text_)
                .note("got `{}`", *warn_value)
                .primary(expr)
                .emit(ctx);
            }
            return series{Output_Type{}, finish(b)};
          }...,
          [&](const auto&) {
            diagnostic::warning("expected `{}`, but got `{}`", input_meta_,
                                arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(Output_Type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }

private:
  std::string name_;
  std::string input_meta_;
  std::span<const ocsf_pair> map_;
  std::string warning_text_;
};

using name_to_id_plugin
  = generic_mapping_plugin<name_to_id, tenzir::int64_type, tenzir::string_type>;

using id_to_name_plugin
  = generic_mapping_plugin<id_to_name, tenzir::string_type, tenzir::int64_type,
                           tenzir::uint64_type>;

} // namespace

} // namespace tenzir::plugins::ocsf

using tenzir::plugins::ocsf::category_map;

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::name_to_id_plugin{
  "ocsf_category_uid", "string", category_map, "OCSF category name"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::id_to_name_plugin{
  "ocsf_category_name", "int", category_map, "OCSF category ID"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::name_to_id_plugin{
  "ocsf_class_uid", "string", tenzir::plugins::ocsf::class_map,
  "OCSF class name"})
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::id_to_name_plugin{
  "ocsf_class_name", "int", tenzir::plugins::ocsf::class_map,
  "OCSF class ID"})
