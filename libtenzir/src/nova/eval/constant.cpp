#include "tenzir/data.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/data_array_builder.hpp"
#include "tenzir/nova/eval_internal.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/nova/storage.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/nova/union_array.hpp"
#include "tenzir/tql2/ast.hpp"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace tenzir::nova {

namespace {

/// Converts a legacy `tenzir::data` value (or `ast::constant::kind`, which
/// shares the same alternative set minus `pattern`) into a nova array
/// broadcast identically to every one of `length` rows selected by `mask`.
/// Shared between top-level constant evaluation and nested `record`/`list`
/// field conversion, so both go through the same set of supported types.
auto eval_data_constant(diagnostic_handler& dh, const auto& v,
                        storage::Index length, storage::BitMap mask)
  -> Array<Data> {
  return tenzir::match(
    v,
    [&](caf::none_t) -> Array<Data> {
      return Array<Data>{Array<Null>{storage::NullStorage{length}}};
    },
    [&](bool x) -> Array<Data> {
      return Array<Data>{Array<Bool>{storage::BitMap{length, x}}};
    },
    [&](std::int64_t x) -> Array<Data> {
      return Array<Data>{
        Array<Int>{storage::ConstantStorage<std::int64_t>{length, x}}};
    },
    [&](std::uint64_t x) -> Array<Data> {
      return Array<Data>{
        Array<UInt>{storage::ConstantStorage<std::uint64_t>{length, x}}};
    },
    [&](double x) -> Array<Data> {
      return Array<Data>{
        Array<Float>{storage::ConstantStorage<double>{length, x}}};
    },
    [&](tenzir::duration x) -> Array<Data> {
      return Array<Data>{
        Array<Duration>{storage::ConstantStorage<Duration>{length, x}}};
    },
    [&](tenzir::time x) -> Array<Data> {
      return Array<Data>{
        Array<Time>{storage::ConstantStorage<Time>{length, x}}};
    },
    [&](const std::string& x) -> Array<Data> {
      return Array<Data>{Array<String>{
        storage::ConstantStorage<std::string, std::string_view>{length, x}}};
    },
    [&](const tenzir::blob& x) -> Array<Data> {
      return Array<Data>{
        Array<Blob>{storage::ConstantStorage<Blob, BlobView>{length, x}}};
    },
    [&](tenzir::ip x) -> Array<Data> {
      return Array<Data>{Array<Ip>{storage::ConstantStorage<Ip>{length, x}}};
    },
    [&](tenzir::subnet x) -> Array<Data> {
      return Array<Data>{
        Array<Subnet>{storage::ConstantStorage<Subnet>{length, x}}};
    },
    [&](const tenzir::record& r) -> Array<Data> {
      auto base = Array<Record>::make_empty(length);
      auto fields
        = std::vector<std::pair<std::string_view, Array<Record>::MaskedArray>>{};
      fields.reserve(r.size());
      for (auto const& [name, field_value] : r) {
        auto field_eval = eval_data_constant(dh, field_value, length, mask);
        fields.emplace_back(
          name, MaskedArray<Array<Data>>{std::move(field_eval), mask});
      }
      return Array<Data>{std::move(base).with_fields(std::move(fields))};
    },
    [&](const tenzir::list& l) -> Array<Data> {
      // `Array<List>` has no constant/broadcast storage (unlike
      // `storage::ConstantStorage<T>` for scalars), so the same list
      // content is materialized once per row.
      auto builder = ArrayBuilder<List>{};
      for (auto i = storage::Index{0}; i < length; ++i) {
        if (not mask.get(i)) {
          builder.skip();
          continue;
        }
        auto lb = builder.list();
        for (auto const& elem : l) {
          append_legacy_data(lb, elem, dh);
        }
      }
      return Array<Data>{builder.finish()};
    },
    [&](const auto&) -> Array<Data> {
      diagnostic::warning("eval not implemented yet for this constant type")
        .emit(dh);
      return Array<Data>{Array<Null>{storage::NullStorage{length}}};
    });
}

} // namespace

auto _::EvalRun::eval(const ast::constant& x, EvalFrame frame) -> Array<Data> {
  return eval_data_constant(frame, x.value, frame.length(), frame.mask());
}

auto _::EvalRun::eval(const ast::resolved_secret& x, EvalFrame frame)
  -> Array<Data> {
  return Array<Data>{Array<Secret>{
    storage::ConstantStorage<Secret, SecretView>{frame.length(), x.value}}};
}

} // namespace tenzir::nova
