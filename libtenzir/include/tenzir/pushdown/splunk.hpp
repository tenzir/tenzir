//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#pragma once

#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/nova/events.hpp"
#include "tenzir/option.hpp"
#include "tenzir/pushdown/pipe.hpp"
#include "tenzir/pushdown/renderer.hpp"
#include "tenzir/result.hpp"

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

/// Pushdown into a user's Splunk search in the Search Processing Language
/// (SPL), as run by `from_splunk`.
///
/// Splunk returns every field value as a string, and `from_splunk` infers
/// numbers, booleans, IP addresses, and timestamps from these strings. Every
/// column is therefore a `DynamicType`, translation yields prefilters only, and
/// the source evaluates every predicate locally. The pushed SPL only needs to
/// keep a superset of the rows that a predicate keeps.
///
/// Prefilters use the `search` command, whose comparisons suit supersets:
/// - `field=value` matches case-insensitively, so it keeps every value that
///   TQL's case-sensitive equality keeps. A `*` in a value is a wildcard, which
///   only keeps more values.
/// - A comparison with a number literal compares numerically when the field's
///   value parses as a number, as TQL does after inference, so that `n>6`
///   matches `"007"` and `"50"`.
/// - `field=*` holds for every value, including the empty string, but not for
///   a missing field.
///
/// Splunk and Tenzir may still disagree on what parses as a number, on
/// whitespace around values, and on non-ASCII case folding. These gaps are
/// accepted: the goal is that the common cases work, and a user who needs
/// different semantics writes the SPL directly.
namespace tenzir::pushdown {

/// Spells IR prefilters as terms of the SPL `search` command, and negations as
/// expressions of a `where` stage.
///
/// Supported shapes:
/// - `==`, `<`, `<=`, `>`, `>=` between a top-level field and a number,
/// - `==` between a top-level field and a string or a boolean,
/// - comparisons of `_time` with a timestamp, in whole epoch seconds rounded
///   outwards,
/// - `in` of a field in a list of string, number, or boolean literals,
/// - `starts_with`, `ends_with`, substring membership, and their
///   case-insensitive forms with ASCII needles, as wildcard patterns,
/// - `!= null` of a field,
/// - `!=`, `not ==`, and `not in` with literals, as expressions of a trailing
///   `where` stage, such as
///   `isnull(f) OR mvcount(f)>1 OR if(f=="v", false(), true())`, which keep
///   events without the field and with several values, as TQL does. The
///   `search` command cannot: `NOT f="v"` drops an event if any value of `f`
///   matches,
/// - IP equality and subnet membership, also with `ip()`, as the canonical
///   spelling of the address or network, and for IPv4 also its IPv4-mapped
///   IPv6 spelling,
/// - `and` and `or` of these, where `and` drops operands that do not render.
///
/// Everything else stays local, notably negation of other predicates, nested
/// field paths, ordering on strings and IP addresses, regular expressions, and
/// timestamps of fields other than `_time`.
class SplRenderer final : public PrefilterRenderer {
public:
  struct Settings {
    /// Whether `_time` holds epoch seconds, so that comparisons of `_time`
    /// with timestamps can go into the search.
    bool push_time = true;
    /// Whether the source infers numbers, booleans, and other types from
    /// Splunk's strings. Negations depend on it: without inference, `"5"`
    /// stays a string, which TQL considers different from the number `5`.
    bool inference = true;
    /// Whether the search formats the values of its results, as with
    /// `fieldformat`. The results then show other values than those that a
    /// `search` or `where` stage compares, so every predicate stays local.
    bool formatted = false;
  };

  explicit SplRenderer(Settings settings);

  auto prefilter_options() const -> PrefilterOptions override;

protected:
  auto render_atom(Expr const& expr) const -> Option<std::string> override;

  auto render_composite(Expr const& expr) const -> Option<std::string> override;

  auto render_junction(Junction junction,
                       std::span<std::string const> operands) const
    -> Option<std::string> override;

private:
  auto render_negation(Expr const& subject, Expr const& negated) const
    -> Option<std::string>;

  auto render_negated_value(Literal const& literal) const
    -> Option<std::string>;

  Settings settings_;
};

/// What appending stages to an SPL search allows.
struct SplStages {
  /// Whether stages appended to the search apply to its results without
  /// changing whether the search parses.
  bool accepted = false;
  /// Whether a `head` stage may follow the search. Splunk could then stop the
  /// search early, so a search with a command that has side effects, such as
  /// `outputlookup` or `collect`, or with a macro, which may hide one, gets
  /// none. This does not keep the search running once the pipeline ends, for
  /// example after a downstream `head`: closing the request may end it too.
  bool limitable = false;
  /// Whether the search formats the values of its results with `fieldformat`,
  /// or has a macro, which may hide it. The results then show the formatted
  /// values, but a stage after the search, such as `search`, `where`, or
  /// `stats`, sees the original ones.
  bool formatted = false;
  /// Whether the results of the search keep a field, which Splunk's export
  /// endpoint needs to return results at all. It returns none if the results
  /// have no fields, while a `stats` stage still counts them. A result
  /// without values among results with fields comes back as an empty object,
  /// also if `eval` assigned `null()` to all of its fields.
  ///
  /// This holds if the search starts with `search`, whose events have
  /// internal fields such as `_cd`, or with `makeresults` with no options but
  /// `count`, `annotate`, `splunk_server`, and `splunk_server_group`, whose
  /// results have `_time`, unlike those of `format` and `data`. It must then
  /// continue only with commands that keep these fields, such as `eval`,
  /// `rex`, `where`, `lookup`, or `fields` without `-`, but not `fields -`,
  /// `table`, `rename`, which may hide fields, or a transforming command. A
  /// macro may hide any command.
  bool keeps_fields = false;
};

/// Prepends the `search` command to a search that starts with neither a
/// command nor `|`, as Splunk Web does, so that a search such as `index=main`
/// runs through the REST API as well. Other searches stay unchanged.
auto spl_search_command(std::string_view search) -> std::string;

/// Checks whether stages can be appended to the SPL `search`.
///
/// The check lexes the search without parsing it. It rejects a search that it
/// cannot lex, such as one with an unterminated string, comment, or subsearch,
/// and a search that is empty or ends in `|`.
auto spl_stages(std::string_view search) -> SplStages;

/// Appends optimizer hints to a user's SPL `search` as trailing stages, in the
/// order `search`, `where`, `fields`, and `head`, so that they apply to the
/// results of `search`. See `plan_trailing_stages`.
///
/// Splunk's export endpoint returns only the fields that a search references,
/// besides a few default fields. Without a projection, the planner therefore
/// appends `fields *`, so that every result carries all of its fields, also
/// those that the predicates kept locally read.
///
/// If `search` mentions `_time`, comparisons of `_time` with timestamps stay
/// local, since the search may assign `_time` a value other than epoch
/// seconds. `inference` states whether the source infers types from Splunk's
/// strings, as `SplRenderer::Settings` describes.
///
/// The `head` stage is appended only for an empty `filter` and only if
/// `spl_stages` allows it. The source must enforce `limit` locally in any case.
///
/// Returns `search` unchanged if `spl_stages` rejects it.
auto plan_spl_search(std::string_view search, ir::OptimizeFilter const& filter,
                     Option<ir::OptimizeProjection> const& projection,
                     Option<uint64_t> limit, bool inference,
                     registry const& reg) -> PipePlan;

/// Spells the search for the events of `indexes`, which must not be empty, or
/// returns `None` if an index name has no spelling.
auto spl_index_search(std::span<std::string const> indexes)
  -> Option<std::string>;

/// Generates a search for the events of `indexes` and adds optimizer hints to
/// it, as `plan_spl_search` does. Since Tenzir writes the search, prefilters
/// extend its base `search` command instead of following in a stage of their
/// own, and a limit becomes a `head` stage whenever the filter is empty.
///
/// Returns `None` if `spl_index_search` does.
auto plan_spl_indexes(std::span<std::string const> indexes,
                      ir::OptimizeFilter const& filter,
                      Option<ir::OptimizeProjection> const& projection,
                      Option<uint64_t> limit, bool inference,
                      registry const& reg) -> Option<PipePlan>;

/// Appends stages to the SPL `search` that compute `aggregation` with `stats`,
/// so that Splunk returns one row per group instead of the results of
/// `search`. `raw` tells whether `from_splunk` keeps the values of these
/// results as strings.
///
/// Returns `None` if `spl_stages` rejects `search` or finds that it may have
/// side effects, which a second search without the stages would repeat, that
/// it formats values, or that its results may lose all fields, or if
/// Splunk cannot compute a column as `summarize` computes it from the results
/// that `from_splunk` emits. Splunk computes these columns:
///
/// - Group keys on top-level fields. An `eval` stage spells the key of a
///   result as `n` for a missing field, as `m` for several values, as `z` for
///   a value with a NUL character, at which `stats` would cut the value off,
///   and as `v` and the value otherwise. Unlike a field of `stats ... by`,
///   the key then keeps the results without the field, and a result with
///   several values joins a group of its own rather than one per value.
/// - `count()`, which every group computes once, since `stats` computes a
///   repeated function only once and names it after its last alias.
/// - `count(x)` as the number of results with the field `x`, where `stats`
///   would count its values.
/// - `min(x)` and `max(x)` without `raw`, over values that are integers of up
///   to 15 digits, which `from_splunk` infers as `int64` and Splunk compares
///   exactly as numbers. The stages compare only these values, and check per
///   group that `x` has no other values.
///
/// The names of fields that the stages read must be plain enough for `eval`
/// to read them as written. Names that start with `_` or `tenzir_` are
/// rejected, which includes Splunk's internal fields, such as `_time`, and the
/// fields that the stages write.
///
/// Since Splunk answers a search whose `eval` fails with an empty response, a
/// last row counts the groups, which `decode_spl_aggregation` requires.
auto plan_spl_aggregation(std::string_view search,
                          ir::OptimizeAggregation const& aggregation, bool raw)
  -> Option<std::string>;

/// Builds the output events of `aggregation` from the result `rows` of the
/// search from `plan_spl_aggregation`, whose values must be strings. `raw`
/// disables type inference of the group keys, as for the results of a search.
///
/// Fails if the result may differ from what `summarize` computes from the
/// results of the search, in which case the source aggregates them locally.
/// In particular, it fails if:
///
/// - The last row does not count the other rows. The search then failed.
/// - A group key has several values, which `summarize` groups as a list, or
///   contains a NUL character.
/// - Type inference makes two group keys equal, such as `5` and `005`, which
///   Splunk groups apart.
/// - A group has a value for `min` or `max` that is not such an integer.
auto decode_spl_aggregation(std::span<nova::Array<nova::Record> const> rows,
                            ir::OptimizeAggregation const& aggregation,
                            bool raw, diagnostic_handler& dh)
  -> Result<std::vector<nova::Events>, std::string>;

} // namespace tenzir::pushdown
