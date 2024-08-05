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

template <typename T, typename... Ts>
concept any_of = (std::same_as<T, Ts> or ...);

auto name_to_id(std::span<const ocsf_pair> lookup,
                std::string_view key) -> int64_t {
  for (const auto& [category, id] : lookup) {
    if (key == category) {
      return id;
    }
  }
  return 0;
};
auto id_to_name(std::span<const ocsf_pair> lookup,
                int64_t key) -> std::string_view {
  for (const auto& [category, id] : lookup) {
    if (key == id) {
      return category;
    }
  }
  return {};
};

/// @brief generic mapping function that supports OCSF `Event Type String` <-> `OCSF UID`
/// @tparam Name the name of the mapping function
/// @tparam Input_Meta the `<meta>` for the input type
/// @tparam &Map The "map" aka array to use for lookup
/// @tparam Operation the mapping function to use
/// @tparam Output_Builder_Type the builder for the resulting type of the mapping
/// @tparam Output_Type the tenzir type the operation produces
/// @tparam Input_Types... The input type arrays that the operation accepts
template <detail::string_literal Name, detail::string_literal Input_Meta,
          auto& Map, auto Operation, typename Output_Builder_Type,
          typename Output_Type, typename... Input_Types>
class generic_mapping_function final : public function_plugin {
public:
  auto name() const -> std::string override {
    return std::string{Name.str()};
  }

  auto make_function(invocation inv,
                     session ctx) const -> failure_or<function_ptr> override {
    auto expr = ast::expression{};
    TRY(argument_parser2::function(std::string{Name.str()})
          .add(expr, std::string{Input_Meta.str()})
          .parse(inv, ctx));
    return function_use::make(
      [expr = std::move(expr)](evaluator eval, session ctx) -> series {
        auto arg = eval(expr);
        auto f = detail::overload{
          [&](const arrow::NullArray& arg) {
            return series::null(Output_Type{}, arg.length());
          },
          []<any_of<Input_Types...> T>(const T& arg) {
            auto b = Output_Builder_Type{};
            check(b.Reserve(arg.length()));
            for (auto i = int64_t{0}; i < arg.length(); ++i) {
              if (arg.IsNull(i)) {
                check(b.AppendNull());
                continue;
              }
              auto in = arg.GetView(i);
              auto out = Operation(std::span{Map}, in);
              check(b.Append(out));
            }
            return series{Output_Type{}, finish(b)};
          },
          [&](const auto&) {
            diagnostic::warning("`{}` expected `string`, but got `{}`",
                                Name.str(), arg.type.kind())
              .primary(expr)
              .emit(ctx);
            return series::null(Output_Type{}, arg.length());
          },
        };
        return caf::visit(f, *arg.array);
      });
  }
};

using category_uid
  = generic_mapping_function<"tql2.ocsf_category_uid", "<string>", category_map,
                   name_to_id, arrow::Int64Builder, tenzir::int64_type,
                   arrow::StringArray>;

using uid_category
  = generic_mapping_function<"tql2.ocsf_uid_category", "<int>", category_map, id_to_name,
                   arrow::StringBuilder, tenzir::string_type, arrow::Int64Array,
                   arrow::UInt64Array>;

using class_uid
  = generic_mapping_function<"tql2.ocsf_class_uid", "<string>", class_map, name_to_id,
                   arrow::Int64Builder, tenzir::int64_type, arrow::StringArray>;

using uid_class
  = generic_mapping_function<"tql2.ocsf_uid_class", "<int>", class_map, id_to_name,
                   arrow::StringBuilder, tenzir::string_type, arrow::Int64Array,
                   arrow::UInt64Array>;

} // namespace

} // namespace tenzir::plugins::ocsf

TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::category_uid)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::uid_category)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::class_uid)
TENZIR_REGISTER_PLUGIN(tenzir::plugins::ocsf::uid_class)
