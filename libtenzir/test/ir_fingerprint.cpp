//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

/// A compiled pipeline's fingerprint changes exactly when what it does, or
/// where its diagnostics point, does.

#include "tenzir/compile_ctx.hpp"
#include "tenzir/diagnostics.hpp"
#include "tenzir/ir.hpp"
#include "tenzir/package.hpp"
#include "tenzir/session.hpp"
#include "tenzir/source.hpp"
#include "tenzir/test/registry.hpp"
#include "tenzir/test/test.hpp"
#include "tenzir/tql2/parser.hpp"
#include "tenzir/tql2/registry.hpp"
#include "tenzir/tql2/tokens.hpp"

#include <string>
#include <string_view>
#include <vector>

using namespace tenzir;

namespace {

/// Compiles `text` the way a deployment does, from a source of its own, and
/// returns its fingerprint.
auto fingerprint_of(std::string_view text, registry const& reg
                                           = test::builtin_registry())
  -> std::string {
  auto dh = collecting_diagnostic_handler{};
  auto provider = session_provider::make(dh, reg);
  auto ctx = provider.as_session();
  auto source = Source::new_source(std::string{text}, "<test>", false);
  auto sources = SourceMap{};
  sources.add_source(source);
  auto tokens = tokenize_permissive(source->text);
  auto parsed = parse(tokens, *source, ctx);
  REQUIRE(parsed);
  auto root = compile_ctx::make_root(base_ctx{ctx.dh(), ctx.reg()}, sources);
  auto compiled = std::move(*parsed).compile(root);
  REQUIRE(compiled);
  return ir::fingerprint(*compiled, sources);
}

/// The builtins with a package `acme`, whose operator `double` is `body` and
/// whose `$base` is `base`.
auto with_acme(std::string body, std::string base) -> Arc<registry const> {
  auto files = package_files{
    {"package.yaml", "name: Acme\n"},
    {"operators/double.tql", std::move(body)},
    {"constants.tql", fmt::format("let $base = {}", base)},
  };
  auto dh = collecting_diagnostic_handler{};
  auto pkg = package::load(files, "packages/acme", "acme", dh, true);
  REQUIRE(pkg);
  auto reg = test::builtin_registry().clone();
  auto packages = std::vector<package>{std::move(*pkg)};
  REQUIRE(add_packages(*reg, packages, dh));
  return Arc<registry const>::from_non_null(std::move(reg));
}

} // namespace

TEST("compiling a pipeline twice gives one fingerprint") {
  for (auto text : {
         "from {x: 1, y: \"a\"} | where x > 0 | set z = x * 2 | select x, z | "
         "sort -x | head 5 | discard",
         "from {x: 1, y: \"a\"} | summarize n=count(), s=sum(x), y | discard",
         "every 1h { from {x: now()} } | discard",
         "from {x: 1} | if x > 0 { set y = 1 } else { set y = 2 } | discard",
         "from {x: 1} | fork { discard } | discard",
         "let $t = now()\nfrom {x: $t} | discard",
         "from {xs: [1, 2]} | unroll xs | drop xs | discard",
         "from {s: \"{}\"} | parsed = s.parse_json() | discard",
         "from {x: 1} | deduplicate x | top x | discard",
       }) {
    CHECK_EQUAL(fingerprint_of(text), fingerprint_of(text));
  }
}

TEST("where a diagnostic would point enters a pipeline's fingerprint") {
  CHECK_NOT_EQUAL(fingerprint_of("from {x: 1} | where x > 0 | discard"),
                  fingerprint_of("from {x: 1}\n// keep the positive ones\n"
                                 "|   where x>0\n| discard"));
  // An edit that moves nothing a location points at does not: the
  // diagnostics point where they did.
  CHECK_EQUAL(fingerprint_of("from {x: 1} | where x > 0 | discard // a"),
              fingerprint_of("from {x: 1} | where x > 0 | discard // b"));
}

TEST("what a pipeline does enters its fingerprint") {
  CHECK_NOT_EQUAL(fingerprint_of("from {x: 1} | where x > 0 | discard"),
                  fingerprint_of("from {x: 1} | where x > 1 | discard"));
  CHECK_NOT_EQUAL(fingerprint_of("from {x: 1} | head 1 | discard"),
                  fingerprint_of("from {x: 1} | head 2 | discard"));
}

TEST("a pipeline's fingerprint follows the packages it uses") {
  auto text = "from {x: acme::$base} | acme::double | discard";
  auto before = with_acme("x = x * 2", "21");
  auto same = with_acme("x = x * 2", "21");
  // Two registries built from the same package agree, although each built
  // sources of its own for the package's operators.
  CHECK_EQUAL(fingerprint_of(text, *before), fingerprint_of(text, *same));
  // Reformatting the package moves what a diagnostic from inside its
  // operator points at.
  auto reformatted = with_acme("// doubles\nx  =  x*2", "21");
  CHECK_NOT_EQUAL(fingerprint_of(text, *before),
                  fingerprint_of(text, *reformatted));
  // So does moving the call, which a diagnostic from inside points at too.
  CHECK_NOT_EQUAL(
    fingerprint_of("from {x: acme::$base} | acme::double | discard", *before),
    fingerprint_of("from {x: acme::$base} |  acme::double| discard", *before));
  // A changed operator or binding does.
  auto other_body = with_acme("x = x * 3", "21");
  CHECK_NOT_EQUAL(fingerprint_of(text, *before),
                  fingerprint_of(text, *other_body));
  auto other_base = with_acme("x = x * 2", "22");
  CHECK_NOT_EQUAL(fingerprint_of(text, *before),
                  fingerprint_of(text, *other_base));
  // A pipeline that does not use the package does not care.
  auto unrelated = "from {x: 1} | discard";
  CHECK_EQUAL(fingerprint_of(unrelated, *before),
              fingerprint_of(unrelated, *other_body));
}
