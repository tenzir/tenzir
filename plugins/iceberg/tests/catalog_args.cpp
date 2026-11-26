//
//  ▀▀█▀▀ █▀▀▀ █▄  █ ▀▀▀█▀ ▀█▀ █▀▀▄
//    █   █▀▀  █ ▀▄█  ▄▀    █  █▀▀▄
//    ▀   ▀▀▀▀ ▀   ▀ ▀▀▀▀▀ ▀▀▀ ▀  ▀
//
// SPDX-FileCopyrightText: (c) 2026 The Tenzir Contributors
// SPDX-License-Identifier: BSD-3-Clause

#include "tenzir/plugins/iceberg/catalog_args.hpp"

#include "tenzir/plugins/iceberg/catalog.hpp"

#include <tenzir/diagnostics.hpp>
#include <tenzir/test/test.hpp>

#include <fmt/format.h>

#include <string>
#include <utility>
#include <vector>

namespace tenzir::plugins::iceberg {

namespace {

TEST("split_table_id splits single-level namespaces") {
  auto const [ns, name] = split_table_id("db.events");
  CHECK_EQUAL(ns, (std::vector<std::string>{"db"}));
  CHECK_EQUAL(name, "events");
}

TEST("split_table_id splits nested namespaces") {
  auto const [ns, name] = split_table_id("org.prod.security.events");
  CHECK_EQUAL(ns, (std::vector<std::string>{"org", "prod", "security"}));
  CHECK_EQUAL(name, "events");
}

TEST("split_table_id handles unqualified and empty names defensively") {
  auto const [ns1, name1] = split_table_id("events");
  CHECK(ns1.empty());
  CHECK_EQUAL(name1, "events");
  auto const [ns2, name2] = split_table_id("");
  CHECK(ns2.empty());
  CHECK_EQUAL(name2, "");
}

TEST("is_google_catalog_url matches https googleapis.com hosts") {
  CHECK(is_google_catalog_url("https://biglake.googleapis.com/iceberg/v1/"
                              "restcatalog"));
  CHECK(is_google_catalog_url("https://googleapis.com"));
  CHECK(is_google_catalog_url("https://googleapis.com/"));
  CHECK(is_google_catalog_url("https://googleapis.com."));
  CHECK(is_google_catalog_url("https://biglake.googleapis.com./iceberg/v1/"
                              "restcatalog"));
  CHECK(
    is_google_catalog_url("https://a.b.googleapis.com/iceberg/v1/restcatalog"));
  CHECK(is_google_catalog_url("HTTPS://BIGLAKE.GOOGLEAPIS.COM/iceberg/v1/"
                              "restcatalog"));
  CHECK(is_google_catalog_url("https://biglake.googleapis.com:443/iceberg/v1/"
                              "restcatalog"));
}

TEST("is_google_catalog_url rejects non-https, spoofed, and invalid URLs") {
  CHECK(not is_google_catalog_url("http://biglake.googleapis.com/iceberg/v1/"
                                  "restcatalog"));
  CHECK(not is_google_catalog_url("http://googleapis.com"));
  CHECK(not is_google_catalog_url("https://evilgoogleapis.com/iceberg/v1/"
                                  "restcatalog"));
  CHECK(not is_google_catalog_url("https://googleapis.com.evil.com/iceberg/v1/"
                                  "restcatalog"));
  CHECK(not is_google_catalog_url("https://.googleapis.com/iceberg/v1/"
                                  "restcatalog"));
  CHECK(not is_google_catalog_url("https://biglake..googleapis.com/iceberg/v1/"
                                  "restcatalog"));
  CHECK(not is_google_catalog_url("https://biglake.googleapis.com../iceberg/v1/"
                                  "restcatalog"));
  CHECK(not is_google_catalog_url("https://localhost:8181"));
  CHECK(
    not is_google_catalog_url("https://glue.us-east-1.amazonaws.com/iceberg"));
  CHECK(not is_google_catalog_url(""));
  CHECK(not is_google_catalog_url("not a url"));
  CHECK(not is_google_catalog_url("://biglake.googleapis.com"));
  CHECK(not is_google_catalog_url("https://"));
}

TEST("validate_catalog_args checks table name syntax") {
  auto valid_dh = collecting_diagnostic_handler{};
  auto valid = catalog_args_detail::CatalogArgValidation{};
  valid.table = located<std::string>{"ns.events", location::unknown};
  catalog_args_detail::validate_catalog_args(valid, "from_iceberg", valid_dh);
  CHECK(valid_dh.empty());
  for (auto const* bad : {"events", ".events", "ns.", "ns..events"}) {
    auto dh = collecting_diagnostic_handler{};
    auto input = catalog_args_detail::CatalogArgValidation{};
    input.table = located<std::string>{bad, location::unknown};
    catalog_args_detail::validate_catalog_args(input, "from_iceberg", dh);
    CHECK(not dh.empty());
  }
}

TEST("validate_catalog_args checks s3_path_style dependency") {
  auto err_dh = collecting_diagnostic_handler{};
  auto err_input = catalog_args_detail::CatalogArgValidation{};
  err_input.s3_path_style_loc = location::unknown;
  catalog_args_detail::validate_catalog_args(err_input, "from_iceberg", err_dh);
  CHECK(not err_dh.empty());
  auto ok_dh = collecting_diagnostic_handler{};
  auto ok_input = catalog_args_detail::CatalogArgValidation{};
  ok_input.s3_endpoint_loc = location::unknown;
  ok_input.s3_path_style_loc = location::unknown;
  catalog_args_detail::validate_catalog_args(ok_input, "from_iceberg", ok_dh);
  CHECK(ok_dh.empty());
}

TEST("validate_catalog_args checks auth mutual exclusion and gcp_project") {
  // Invalid AWS catalog service.
  auto bad_aws_dh = collecting_diagnostic_handler{};
  auto bad_aws = catalog_args_detail::CatalogArgValidation{};
  bad_aws.catalog_aws_service
    = located<std::string>{"invalid", location::unknown};
  bad_aws.catalog_aws_service_loc = location::unknown;
  catalog_args_detail::validate_catalog_args(bad_aws, "to_iceberg", bad_aws_dh);
  CHECK(not bad_aws_dh.empty());
  // Valid AWS catalog service alone.
  auto ok_aws_dh = collecting_diagnostic_handler{};
  auto ok_aws = catalog_args_detail::CatalogArgValidation{};
  ok_aws.catalog_aws_service = located<std::string>{"glue", location::unknown};
  ok_aws.catalog_aws_service_loc = location::unknown;
  catalog_args_detail::validate_catalog_args(ok_aws, "to_iceberg", ok_aws_dh);
  CHECK(ok_aws_dh.empty());
  // AWS catalog service + token conflicts.
  auto aws_token_dh = collecting_diagnostic_handler{};
  auto aws_token = catalog_args_detail::CatalogArgValidation{};
  aws_token.catalog_aws_service
    = located<std::string>{"glue", location::unknown};
  aws_token.catalog_aws_service_loc = location::unknown;
  aws_token.token_loc = location::unknown;
  catalog_args_detail::validate_catalog_args(aws_token, "to_iceberg",
                                             aws_token_dh);
  CHECK(not aws_token_dh.empty());
  // AWS catalog service + gcp_project conflicts.
  auto aws_gcp_dh = collecting_diagnostic_handler{};
  auto aws_gcp = catalog_args_detail::CatalogArgValidation{};
  aws_gcp.catalog_aws_service
    = located<std::string>{"s3tables", location::unknown};
  aws_gcp.catalog_aws_service_loc = location::unknown;
  aws_gcp.gcp_project_loc = location::unknown;
  catalog_args_detail::validate_catalog_args(aws_gcp, "to_iceberg", aws_gcp_dh);
  CHECK(not aws_gcp_dh.empty());
  // token + gcp_service_account_key conflicts.
  auto token_key_dh = collecting_diagnostic_handler{};
  auto token_key = catalog_args_detail::CatalogArgValidation{};
  token_key.token_loc = location::unknown;
  token_key.gcp_service_account_key_loc = location::unknown;
  catalog_args_detail::validate_catalog_args(token_key, "to_iceberg",
                                             token_key_dh);
  CHECK(not token_key_dh.empty());
  // token + gcp_project conflicts.
  auto token_project_dh = collecting_diagnostic_handler{};
  auto token_project = catalog_args_detail::CatalogArgValidation{};
  token_project.token_loc = location::unknown;
  token_project.gcp_project_loc = location::unknown;
  catalog_args_detail::validate_catalog_args(token_project, "to_iceberg",
                                             token_project_dh);
  CHECK(not token_project_dh.empty());
  // gcp_project alone implies Google authentication (no separate flag needed).
  auto project_only_dh = collecting_diagnostic_handler{};
  auto project_only = catalog_args_detail::CatalogArgValidation{};
  project_only.gcp_project_loc = location::unknown;
  catalog_args_detail::validate_catalog_args(project_only, "from_iceberg",
                                             project_only_dh);
#ifdef TENZIR_ICEBERG_GCS
  CHECK(project_only_dh.empty());
#else
  CHECK(not project_only_dh.empty());
#endif
}

TEST("aws_catalog_hint names the service that fits an AWS catalog host") {
  auto const glue = aws_catalog_hint(
    "https://glue.eu-central-1.amazonaws.com/iceberg", "", "");
  REQUIRE(glue);
  CHECK(glue->find("`catalog_aws_service=\"glue\"`") != std::string::npos);
  CHECK(glue->find("region `eu-central-1`") != std::string::npos);
  // The URL can come from a secret: the hint names service and region only.
  CHECK(glue->find("amazonaws") == std::string::npos);
  auto const s3tables = aws_catalog_hint(
    "HTTPS://S3Tables.us-east-1.amazonaws.com:443/iceberg", "", "us-east-1");
  REQUIRE(s3tables);
  CHECK(s3tables->find("`catalog_aws_service=\"s3tables\"`")
        != std::string::npos);
  auto const fips = aws_catalog_hint(
    "https://glue-fips.us-gov-west-1.amazonaws.com/iceberg", "", "");
  REQUIRE(fips);
  CHECK(fips->find("`catalog_aws_service=\"glue\"`") != std::string::npos);
  auto const china = aws_catalog_hint(
    "https://glue.cn-north-1.amazonaws.com.cn/iceberg", "", "");
  REQUIRE(china);
  CHECK(china->find("`catalog_aws_service=\"glue\"`") != std::string::npos);
}

TEST("aws_catalog_hint reports a mismatched service or region") {
  auto const service
    = aws_catalog_hint("https://glue.eu-central-1.amazonaws.com/iceberg",
                       "s3tables", "eu-central-1");
  REQUIRE(service);
  CHECK(service->find("`catalog_aws_service=\"s3tables\"` does not fit")
        != std::string::npos);
  CHECK(service->find("use `catalog_aws_service=\"glue\"`")
        != std::string::npos);
  CHECK(service->find("different region") == std::string::npos);
  CHECK(service->find("amazonaws") == std::string::npos);
  auto const region = aws_catalog_hint(
    "https://glue.us-east-1.amazonaws.com/iceberg", "glue", "eu-central-1");
  REQUIRE(region);
  CHECK(region->find("signed for a different region") != std::string::npos);
  CHECK(region->find("the catalog URL names, `us-east-1`")
        != std::string::npos);
  // The configured region can come from a secret and must not appear.
  CHECK(region->find("eu-central-1") == std::string::npos);
  CHECK(region->find("amazonaws") == std::string::npos);
  CHECK(region->find("does not fit") == std::string::npos);
  auto const both = aws_catalog_hint(
    "https://glue.us-east-1.amazonaws.com/iceberg", "s3tables", "eu-central-1");
  REQUIRE(both);
  CHECK(both->find("does not fit") != std::string::npos);
  CHECK(both->find("signed for a different region") != std::string::npos);
  CHECK(both->find("eu-central-1") == std::string::npos);
}

TEST("aws_catalog_hint stays silent for fitting settings and other hosts") {
  CHECK(not aws_catalog_hint("https://glue.eu-central-1.amazonaws.com/iceberg",
                             "glue", "eu-central-1"));
  // An unknown signing region is not a mismatch.
  CHECK(not aws_catalog_hint("https://glue.eu-central-1.amazonaws.com/iceberg",
                             "glue", ""));
  CHECK(not aws_catalog_hint("http://localhost:8181", "", ""));
  CHECK(not aws_catalog_hint("https://catalog.example.com/iceberg", "", ""));
  // Private endpoints and other AWS services are not recognized.
  CHECK(not aws_catalog_hint(
    "https://vpce-0123-abcd.glue.eu-central-1.vpce.amazonaws.com/iceberg", "",
    ""));
  CHECK(not aws_catalog_hint("https://s3.eu-central-1.amazonaws.com/bucket", "",
                             ""));
  CHECK(not aws_catalog_hint("https://glue.amazonaws.com/iceberg", "", ""));
  CHECK(not aws_catalog_hint("https://glue.eu-central-1.amazonaws.com.evil.com",
                             "", ""));
  CHECK(not aws_catalog_hint("glue.eu-central-1.amazonaws.com", "", ""));
  CHECK(not aws_catalog_hint("", "", ""));
}

TEST("translate_error retries REST errors unless the status is a permanent "
     "client error") {
  auto const translate_rest = [](std::string message) {
    return translate_error(ice::Error{
      .kind = ice::ErrorKind::kRestError,
      .message = std::move(message),
    });
  };
  for (auto const status : {400, 401, 403, 404, 405, 406, 410, 422}) {
    auto const error = translate_rest(fmt::format(
      "Unable to process (code: {}, type: RESTException): reason", status));
    CHECK(error.kind == Error::Kind::permanent);
  }
  // No response (0), timeouts, conflicts, throttling, and server errors.
  for (auto const status : {0, 408, 409, 421, 425, 429, 500, 502, 503, 504}) {
    auto const error = translate_rest(fmt::format(
      "Unable to process (code: {}, type: RESTException): reason", status));
    CHECK(error.kind == Error::Kind::transient);
  }
  CHECK(translate_rest("Unable to process").kind == Error::Kind::transient);
  CHECK(translate_rest("Unable to process (code: x, type: null): null").kind
        == Error::Kind::transient);
  CHECK(translate_rest("").kind == Error::Kind::transient);
}

TEST("translate_error keeps the kinds of dedicated iceberg errors") {
  auto const translate_kind = [](ice::ErrorKind kind) {
    return translate_error(ice::Error{.kind = kind, .message = "message"}).kind;
  };
  CHECK(translate_kind(ice::ErrorKind::kForbidden) == Error::Kind::permanent);
  CHECK(translate_kind(ice::ErrorKind::kNotAuthorized)
        == Error::Kind::permanent);
  CHECK(translate_kind(ice::ErrorKind::kServiceUnavailable)
        == Error::Kind::transient);
  CHECK(translate_kind(ice::ErrorKind::kInternalServerError)
        == Error::Kind::transient);
  CHECK(translate_kind(ice::ErrorKind::kNoSuchTable) == Error::Kind::not_found);
}

} // namespace

} // namespace tenzir::plugins::iceberg
