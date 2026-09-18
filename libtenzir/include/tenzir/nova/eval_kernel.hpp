//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/nova/allocator.hpp"
#include "tenzir/nova/array.hpp"
#include "tenzir/nova/bitmap.hpp"
#include "tenzir/nova/eval.hpp"
#include "tenzir/nova/type_system.hpp"
#include "tenzir/option.hpp"
#include "tenzir/tql2/ast.hpp"

#include <array>
#include <cstddef>
#include <functional>
#include <iterator>
#include <span>
#include <tuple>
#include <type_traits>
#include <vector>

namespace tenzir::nova {

constexpr auto kFundamentalCount = fundamental_type_list::size;
constexpr auto kDataTypeCount = data_type_list::size;

using KernelViewList
  = fundamental_view_list::join<structured_type_list::wrap<RowView>>;

/// Checks whether `Kernel` is invocable with a `diagnostic_handler&` plus the
/// concrete view types selected by `Digits` (one `KernelViewList`
/// index per argument). Note: this is `std::is_invocable_v`, so a kernel
/// overload for `Int` also accepts `Bool`/other types implicitly convertible
/// to it. Kernel authors who want exact-type matching only should constrain
/// their kernel with a `requires` clause (e.g. `requires std::same_as<T,
/// Int>`).
template <class Kernel, std::size_t... Digits>
consteval auto is_kernel_invocable_for() -> bool {
  return std::is_invocable_v<Kernel, diagnostic_handler&,
                             typename KernelViewList::at<Digits>...>;
}

/// Fills the flattened N^K acceptance table for `Kernel`, one entry per
/// K-tuple of data-type indices (row-major, most-significant digit =
/// argument 0). Recurses on the length of the accumulated `Prefix...` pack
/// (a compile-time count), introducing one new *genuine* template parameter
/// `D` per recursion level via `index_sequence` expansion — never converting
/// a runtime value into a template argument.
template <class Kernel, std::size_t K, std::size_t... Prefix>
consteval void fill_kernel_acceptance_table(auto& table, std::size_t base) {
  if constexpr (sizeof...(Prefix) == K) {
    table[base] = is_kernel_invocable_for<Kernel, Prefix...>();
  } else {
    [&]<std::size_t... D>(std::index_sequence<D...>) {
      (fill_kernel_acceptance_table<Kernel, K, Prefix..., D>(
         table, base * kDataTypeCount + D),
       ...);
    }(std::make_index_sequence<kDataTypeCount>());
  }
}

/// The full N^K joint-invocability table for `Kernel`, where `K` is the
/// kernel's arity (number of dynamic data-typed arguments). Flattened
/// row-major, with the most-significant index belonging to argument 0.
/// Identical shape for any K, including K=1 (a plain length-N table).
template <class Kernel, std::size_t K>
consteval auto accepted_kernel_types() {
  constexpr auto total = [] {
    auto t = std::size_t{1};
    for (auto i = std::size_t{0}; i < K; ++i) {
      t *= kDataTypeCount;
    }
    return t;
  }();
  auto table = std::array<bool, total>{};
  fill_kernel_acceptance_table<Kernel, K>(table, 0);
  // Clang cannot prove NRVO for dependent-typed locals in templates and warns
  // via `-Wnrvo`; the explicit move sidesteps that. This function is
  // `consteval`, so the move never runs at runtime anyway.
  return std::move(table);
}

/// The accepted type combinations of one kernel, with a query for whether a
/// partially resolved type tuple has any accepted completion. The acceptance
/// table is row-major, so every completion of a prefix occupies one contiguous
/// range.
template <class Kernel, std::size_t K>
struct KernelAcceptance {
  static constexpr auto table = accepted_kernel_types<Kernel, K>();

  template <std::size_t... Prefix>
  static consteval auto accepts_prefix() -> bool {
    static_assert(sizeof...(Prefix) <= K);
    auto begin = std::size_t{0};
    ((begin = begin * kDataTypeCount + Prefix), ...);
    auto suffix_size = std::size_t{1};
    for (auto i = sizeof...(Prefix); i < K; ++i) {
      suffix_size *= kDataTypeCount;
    }
    begin *= suffix_size;
    for (auto i = std::size_t{0}; i < suffix_size; ++i) {
      if (table[begin + i]) {
        return true;
      }
    }
    return false;
  }
};

/// Emits the diagnostic for one rejected runtime type combination. Keeping
/// this non-templated avoids duplicating the formatting machinery for every
/// tuple considered by a kernel.
inline auto
warn_rejected_kernel_types(diagnostic_handler& dh, std::string_view name,
                           location loc, std::span<std::size_t const> tags)
  -> void {
  static constexpr auto type_names
    = []<std::size_t... Is>(std::index_sequence<Is...>) {
        return std::array<std::string_view, kDataTypeCount>{
          Type<data_type_list::at<Is>>::static_name...,
        };
      }(std::make_index_sequence<kDataTypeCount>());
  if (tags.size() == 1) {
    diagnostic::warning("{} does not accept the argument type `{}`", name,
                        type_names[tags.front()])
      .primary(loc)
      .emit(dh);
    return;
  }
  auto names = std::vector<std::string_view>{};
  names.reserve(tags.size());
  for (auto tag : tags) {
    names.push_back(type_names[tag]);
  }
  diagnostic::warning("{} does not accept the argument type combination "
                      "`({})`",
                      name, fmt::join(names, ", "))
    .primary(loc)
    .emit(dh);
}

/// A lazily-constructed, randomly-settable result slot for fundamental type
/// `Tag`, used by `apply_kernel` as one of `kFundamentalCount`
/// parallel entries (one per possible result tag) that a per-row kernel
/// invocation can write into via `set`. An entry's own present-mask starts
/// all-`false` (a row is only ever *added* to a given tag's result, never
/// pre-assumed present) and is built up by `set` itself: a row is present in
/// this entry's result iff `set` was actually called for it. This matters
/// because a kernel's result tag can vary per row: each `Entry<Tag>` only
/// "owns" the subset of rows that actually resolved to `Tag`, which is not
/// the same subset for every entry, so no entry can borrow a single combined
/// present-mask up front. `Results::finish` backfills whatever the entries
/// leave uncovered.
template <fundamental_type Tag>
class Entry {
public:
  explicit Entry(storage::Index length) : present_{length} {
  }

  auto set(storage::Index i, typename Type<Tag>::ViewType v) -> void {
    present_.set(i, true);
    if (not mutable_) {
      mutable_.emplace(present_.length());
    }
    mutable_->set(i, v);
  }

  /// Whether `set` was ever called on this entry.
  auto engaged() const -> bool {
    return mutable_.is_some();
  }

  auto finish() && -> MaskedArray<Array<Tag>> {
    // No `set` call at all (e.g. zero rows, or no row resolving to this tag)
    // leaves `mutable_` unconstructed; build an empty one now so `finish`
    // always yields a validly-sized (if all-absent) array.
    if (not mutable_) {
      mutable_.emplace(present_.length());
    }
    return MaskedArray<Array<Tag>>{
      .data = Array<Tag>{std::move(*mutable_).finish()},
      .present = std::move(present_).finish(),
    };
  }

  storage::BitMap::Mutable present_;
  Option<typename Type<Tag>::PrimaryPhysicalStorage::Mutable> mutable_;
};

using EntriesTuple = fundamental_type_list::wrap<Entry>::apply<std::tuple>;

inline auto make_entries(storage::Index length) -> EntriesTuple {
  return [&]<std::size_t... TagIdx>(std::index_sequence<TagIdx...>) {
    return EntriesTuple{
      Entry<fundamental_type_list::at<TagIdx>>{length}...,
    };
  }(std::make_index_sequence<kFundamentalCount>());
}

/// Owns one `Entry<Tag>` per fundamental tag plus the `UnionArray`
/// alternative-index bookkeeping needed once a kernel's result tag can vary
/// per row. `set<Tag>` records both the value (via the matching `Entry`) and
/// the row's chosen alternative (via `alternative_indices_`) unconditionally
/// -- `finish` decides afterwards whether that bookkeeping was actually
/// needed. Today `apply_kernel` only ever calls `set` with a
/// single compile-time-fixed `Tag` for the whole call, so `finish` always
/// takes the single-alternative path below; the multi-alternative path
/// exists so a future per-row-varying caller needs no further changes here.
class Results {
public:
  explicit Results(storage::Index length)
    : alternative_indices_{length}, entries_{make_entries(length)} {
  }

  template <fundamental_type Tag>
  auto set(storage::Index i, typename Type<Tag>::ViewType v) -> void {
    constexpr auto tag_index = fundamental_type_list::unique_index_of<Tag>;
    std::get<tag_index>(entries_).set(i, v);
    alternative_indices_.set(i, tag_index);
  }

  /// Marks row `i` as an explicit `Null`, engaging the `Null` entry if this
  /// is its first `set_null` call. Also records `Null` as `i`'s alternative,
  /// exactly like `set<Tag>` does for its `Tag` — required for `finish` to
  /// build a correct `UnionArray` if some other row engages a different
  /// alternative.
  auto set_null(storage::Index i) -> void {
    constexpr auto null_index = fundamental_type_list::unique_index_of<Null>;
    auto& null_entry = std::get<null_index>(entries_);
    if (not null_entry.mutable_) {
      null_entry.mutable_.emplace(null_entry.present_.length());
    }
    null_entry.present_.set(i, true);
    alternative_indices_.set(i, null_index);
  }

  /// Marks every row selected by `mask` as an explicit `Null`.
  auto set_null(storage::BitMap const& mask) -> void {
    for (auto i = storage::Index{0}; i < alternative_indices_.length(); ++i) {
      if (mask.get(i)) {
        set_null(i);
      }
    }
  }

  /// Assembles the result for `requested`. Rows of `requested` that no `set`
  /// or `set_null` call ever touched become explicit `Null`s, which is what
  /// makes the result valid across the whole requested mask.
  auto finish(storage::BitMap const& requested) && -> Array<Data> {
    auto const length = alternative_indices_.length();
    auto engaged_count = std::size_t{0};
    auto engaged_index = std::size_t{0};
    [&]<std::size_t... Is>(std::index_sequence<Is...>) {
      (
        [&] {
          if (std::get<Is>(entries_).engaged()) {
            engaged_index = Is;
            ++engaged_count;
          }
        }(),
        ...);
    }(std::make_index_sequence<kFundamentalCount>());

    if (engaged_count == 0) {
      // No `set` call at all: every requested row is null.
      return Array<Data>{Array<Null>{storage::NullStorage{length}}};
    }
    if (engaged_count == 1) {
      // `alternative_indices_` is dropped unfinished: with only one
      // alternative ever engaged, every row that has a value refers to that
      // same alternative, so the index array carries no information.
      return [&]<std::size_t... Is>(std::index_sequence<Is...>) -> Array<Data> {
        auto result = Option<Array<Data>>{};
        (
          [&] {
            if (Is == engaged_index) {
              auto masked = std::move(std::get<Is>(entries_)).finish();
              result.emplace(Array<Data>{std::move(masked.data)}.null_where(
                requested.and_not(masked.present)));
            }
          }(),
          ...);
        return std::move(*result);
      }(std::make_index_sequence<kFundamentalCount>());
    }

    // More than one alternative engaged: build a `UnionArray` combining
    // every engaged entry. `fields` is compacted -- it only holds engaged
    // entries, indexed densely from 0 -- but `alternative_indices_` was
    // populated by `set`/`set_null` using each `Tag`'s raw
    // `fundamental_type_list` index (e.g. `Null` is 0, `Int` is 2), which
    // does not match `fields`' compacted position once any *earlier*
    // fundamental type in the list went unengaged. `remap` records, per
    // raw index, the compacted position its entry (if engaged) landed at,
    // so `alternative_indices_` can be rewritten into `fields`' index space
    // before use.
    auto fields = storage::Vector<UnionArray::MaskedArray>{};
    fields.reserve(engaged_count);
    auto remap = std::array<storage::Index, kFundamentalCount>{};
    [&]<std::size_t... Is>(std::index_sequence<Is...>) {
      (
        [&] {
          if (std::get<Is>(entries_).engaged()) {
            remap[Is] = static_cast<storage::Index>(fields.size());
            auto masked = std::move(std::get<Is>(entries_)).finish();
            fields.push_back(UnionArray::MaskedArray{
              .data = ErasedArray{std::move(masked.data)},
              .present = std::move(masked.present),
            });
          }
        }(),
        ...);
    }(std::make_index_sequence<kFundamentalCount>());
    // A row is present in the union iff it is present in *some* alternative:
    // the logical OR of each engaged entry's own present-mask.
    auto present = fields.front().present;
    for (auto it = std::next(fields.begin()); it != fields.end(); ++it) {
      present = std::move(present) | it->present;
    }
    auto raw_indices = std::move(alternative_indices_).finish();
    auto remapped_indices
      = storage::SparseStorage<storage::Index>::Mutable{length};
    for (auto row = storage::Index{0}; row < length; ++row) {
      remapped_indices.set(
        row, remap[static_cast<std::size_t>(raw_indices.get(row))]);
    }
    return Array<Data>{
      UnionArray{std::move(remapped_indices).finish(), std::move(fields)}}
      .null_where(requested.and_not(present));
  }

private:
  storage::SparseStorage<storage::Index>::Mutable alternative_indices_;
  EntriesTuple entries_;
};

/// Resolves the remaining argument tags after a prefix has no accepted
/// completion. Tags and the current argument position are runtime values, so
/// rejected suffixes do not create a typed Cartesian product. Union
/// alternatives still narrow the mask and produce the same full-tuple
/// diagnostics as the typed traversal.
template <std::size_t K>
inline auto
resolve_rejected_tags(std::array<Array<Data>, K> const& args, std::size_t pos,
                      std::array<std::size_t, K> tags,
                      storage::BitMap const& mask, auto&& reject) -> void {
  if (pos == K) {
    reject(tags, mask);
    return;
  }
  match(
    args[pos],
    [&]<data_type Tag>(Array<Tag> const&) -> void {
      auto next_tags = tags;
      next_tags[pos] = data_type_list::unique_index_of<Tag>;
      resolve_rejected_tags(args, pos + 1, std::move(next_tags), mask, reject);
    },
    [&](UnionArray const& u) -> void {
      for (auto const& field : u.fields()) {
        match(field.data, [&]<data_type Tag>(Array<Tag> const&) -> void {
          auto next_tags = tags;
          next_tags[pos] = data_type_list::unique_index_of<Tag>;
          auto const sub_mask = mask & field.present;
          resolve_rejected_tags(args, pos + 1, std::move(next_tags), sub_mask,
                                reject);
        });
      }
    });
}

/// Resolves each of `args`' concrete `Array<Tag>` via `match`, then recurses
/// while the accumulated tag prefix has an accepted completion. Once all `K`
/// arguments are resolved, calls `finish` with the tag pack, a tuple of the
/// concrete arrays, and the row-selection `mask` accumulated for this path.
/// A rejected prefix switches to `resolve_rejected_tags`, avoiding typed
/// instantiations for the rest of its Cartesian subtree.
///
/// If an argument is a `UnionArray`, loops over its engaged alternatives and
/// recurses into each independently, narrowing `mask` to that alternative's
/// rows (`mask & field.present`). Arrays are captured by value because
/// concrete alternatives of `Array<Data>` are reconstructed inside
/// `match`'s callback.
template <class Acceptance, std::size_t K, std::size_t... Resolved>
inline auto resolve_tags(std::array<Array<Data>, K> const& args, auto&& finish,
                         auto&& reject, storage::BitMap const& mask,
                         auto&&... resolved_arrays) -> void {
  constexpr auto pos = sizeof...(Resolved);
  if constexpr (not Acceptance::template accepts_prefix<Resolved...>()) {
    resolve_rejected_tags(args, pos, std::array<std::size_t, K>{Resolved...},
                          mask, reject);
  } else if constexpr (pos == K) {
    finish.template operator()<Resolved...>(
      std::tuple<decltype(resolved_arrays)...>{resolved_arrays...}, mask);
  } else {
    match(
      args[pos],
      [&]<data_type Tag>(Array<Tag> arr) -> void {
        constexpr auto tag_index = data_type_list::unique_index_of<Tag>;
        resolve_tags<Acceptance, K, Resolved..., tag_index>(
          args, finish, reject, mask, resolved_arrays..., arr);
      },
      [&](UnionArray const& u) -> void {
        for (auto const& field : u.fields()) {
          match(field.data, [&]<data_type Tag>(Array<Tag> arr) -> void {
            constexpr auto tag_index = data_type_list::unique_index_of<Tag>;
            auto const sub_mask = mask & field.present;
            resolve_tags<Acceptance, K, Resolved..., tag_index>(
              args, finish, reject, sub_mask, resolved_arrays..., arr);
          });
        }
      });
  }
}

template <data_type Tag>
auto kernel_arg(Array<Tag> const& array, storage::Index i) {
  if constexpr (fundamental_type<Tag>) {
    return *array.get(i);
  } else {
    return array.get(i);
  }
}

/// A latch for a diagnostic that a kernel may hit on many rows. `apply_kernel`
/// invokes the kernel once per row, so emitting directly would repeat the same
/// message for every offending row of the batch. Declare one of these outside
/// the kernel, capture it by reference, and emit through `warn_once` to report
/// the condition a single time per batch.
class WarnOnce {
public:
  /// Emits `builder` unless this latch has already fired.
  auto operator()(diagnostic_handler& dh, auto builder) -> void {
    if (std::exchange(fired_, true)) {
      return;
    }
    std::move(builder).emit(dh);
  }

private:
  bool fired_ = false;
};

/// Dispatches the generic `kernel` (a templated lambda or overload set) over
/// the `K` already-evaluated dynamic data-typed `args`, producing the rows of
/// `frame.mask()`. Diagnostics for rejected arguments/type-combinations are
/// attributed to `loc`. Determines at compile time, from `kernel` alone, which
/// K-tuples of data types it accepts (`accepted_kernel_types`), and rejects
/// (with a diagnostic and an all-null result) any resolved type-tuple that
/// isn't accepted — without ever instantiating `kernel`'s body for a rejected
/// tuple. Typed resolution stops at the first tag prefix with no accepted
/// completion, so rejected suffixes are walked at runtime by
/// `resolve_rejected_tags` instead of instantiating their Cartesian product.
/// One dispatch path for any arity `K` (1 for unary, 2 for binary, ...); no
/// special case per arity.
template <std::size_t K, class Kernel>
inline auto apply_kernel(EvalFrame frame, std::string_view name,
                         std::array<Array<Data>, K> args, location loc,
                         const Kernel& kernel) -> Array<Data> {
  auto const length = args[0].length();
  using Acceptance = KernelAcceptance<Kernel, K>;

  // Constructed here, outside the callbacks passed to `resolve_tags` below,
  // and only finished once that call has fully returned: `results` is the
  // single accumulator for this call's output, populated via `set` as a side
  // effect rather than threaded back through return values.
  auto results = Results{length};
  auto reject = [&](std::array<std::size_t, K> const& tags,
                    storage::BitMap const& rejected_mask) {
    if (not rejected_mask.any()) {
      return;
    }
    warn_rejected_kernel_types(frame, name, loc, tags);
    results.set_null(rejected_mask);
  };
  auto finish = [&]<std::size_t... Tags>(
                  auto arrays, storage::BitMap const& resolved_mask) -> void {
    // `resolve_tags` only reaches this callback for accepted type tuples, so
    // instantiating the kernel is valid here.
    using ResultOption = std::invoke_result_t<Kernel, diagnostic_handler&,
                                              KernelViewList::at<Tags>...>;
    using ResultView = typename ResultOption::value_type;
    constexpr auto result_tag_index = [] {
      if constexpr (fundamental_view_list::contains<ResultView>) {
        return fundamental_view_list::unique_index_of<ResultView>;
      } else {
        return fundamental_type_list::unique_index_of<ResultView>;
      }
    }();
    using ResultTag = fundamental_type_list::at<result_tag_index>;

    // Loop rows, invoking `kernel` with the concrete `ViewType`s and writing
    // each row's result into `results` — a `None` result marks that row
    // explicitly null instead. `resolved_mask` is `frame.mask()` narrowed to
    // the union alternatives that led to this accepted tuple; every argument
    // holds a materialized value at every row of it, so there is nothing
    // further to intersect.
    [&]<std::size_t... Pos>(std::index_sequence<Pos...>) {
      auto const n = std::get<0>(arrays).length();
      for (auto i = storage::Index{0}; i < n; ++i) {
        if (resolved_mask.get(i)) {
          auto result = kernel(frame, kernel_arg(std::get<Pos>(arrays), i)...);
          if (result.is_some()) {
            results.template set<ResultTag>(i, *std::move(result));
          } else {
            results.set_null(i);
          }
        }
      }
    }(std::make_index_sequence<K>());
  };
  resolve_tags<Acceptance, K>(args, finish, reject, frame.mask());
  return std::move(results).finish(frame.mask());
}

/// Dispatches `kernel` over already-evaluated argument values. This is the
/// overload function implementations use: their arguments arrive as
/// `ValueArgument`s.
template <std::size_t K, class Kernel>
inline auto apply_kernel(EvalFrame frame, std::string_view name,
                         std::array<ValueArgument, K> const& args, location loc,
                         const Kernel& kernel) -> Array<Data> {
  auto arrays = [&]<std::size_t... Pos>(std::index_sequence<Pos...>) {
    return std::array{args[Pos].data...};
  }(std::make_index_sequence<K>());
  return apply_kernel<K>(frame, name, std::move(arrays), std::move(loc),
                         kernel);
}

/// Evaluates `exprs` under `frame` and dispatches `kernel` over the results.
/// Used by expression nodes, which hold their operands as AST rather than as
/// already-evaluated arrays.
template <std::size_t K, class Kernel>
inline auto
apply_kernel(EvalFrame frame, std::string_view name,
             std::array<std::reference_wrapper<const ast::expression>, K> exprs,
             location loc, const Kernel& kernel) -> Array<Data> {
  auto args = [&]<std::size_t... Pos>(std::index_sequence<Pos...>) {
    return std::array{frame.eval(exprs[Pos].get())...};
  }(std::make_index_sequence<K>());
  return apply_kernel<K>(frame, name, std::move(args), std::move(loc), kernel);
}

} // namespace tenzir::nova
