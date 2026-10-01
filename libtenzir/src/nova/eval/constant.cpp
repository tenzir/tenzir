#include "tenzir/concepts.hpp"
#include "tenzir/data.hpp"
#include "tenzir/detail/assert.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/tql2/ast.hpp"

#include <cstdint>
#include <string>

namespace tenzir::nova {

namespace {

/// Converts a legacy `tenzir::data` value, or an `ast::constant::kind`, which
/// shares its alternatives minus `pattern`, into an owning nova value.
/// Constants never hold the legacy-only types.
auto to_nova_data(auto const& value) -> Data {
  return tenzir::match(
    value,
    [](caf::none_t) -> Data {
      return Null{};
    },
    [](tenzir::record const& x) -> Data {
      auto result = Record{};
      for (auto const& [name, field] : x) {
        result.emplace(name, to_nova_data(field));
      }
      return result;
    },
    [](tenzir::list const& x) -> Data {
      auto result = List{};
      result.reserve(x.size());
      for (auto const& element : x) {
        result.push_back(to_nova_data(element));
      }
      return result;
    },
    []<class T>(T const& x) -> Data
      requires concepts::one_of<T, bool, std::int64_t, std::uint64_t, double,
                                tenzir::duration, tenzir::time, std::string,
                                tenzir::blob, tenzir::ip, tenzir::subnet>
               {
                 return x;
               },
               []<class T>(T const&) -> Data
                 requires concepts::one_of<T, tenzir::pattern,
                                           tenzir::enumeration, tenzir::map,
                                           tenzir::secret>
    {
      TENZIR_UNREACHABLE();
    });
}

} // namespace

auto _::EvalRun::eval(const ast::constant& x, EvalFrame frame) -> Array<Data> {
  return repeat(to_nova_data(x.value), frame.length());
}

auto _::EvalRun::eval(const ast::pkg_dollar_var& x, EvalFrame frame)
  -> Array<Data> {
  // The value is const-evaluated and cached during resolution (see
  // `resolve_entities`); `Evaluator::make` rejects unresolved bindings.
  TENZIR_ASSERT(x.value);
  return repeat(to_nova_data(*x.value), frame.length());
}

auto _::EvalRun::eval(const ast::resolved_secret& x, EvalFrame frame)
  -> Array<Data> {
  return Array<Data>{Array<Secret>{
    storage::ConstantStorage<Secret, SecretView>{frame.length(), x.value}}};
}

} // namespace tenzir::nova
