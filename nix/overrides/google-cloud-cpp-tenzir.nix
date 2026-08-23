{
  lib,
  stdenv,
  fetchFromGitHub,
  google-cloud-cpp,
}:
# The four APIs the plugins look for, and no more: `storage` for gcs and
# iceberg, `pubsub` for google-cloud-pubsub, `logging` for
# to_google_cloud_logging, and `oauth2` for to_google_secops. A plugin whose API
# is missing disables itself with nothing but a CMake warning, so the list has
# to be complete on every platform - the nixpkgs default carries no `oauth2`,
# and the static list used to carry no `logging`. It matches
# GOOGLE_CLOUD_CPP_ENABLE in scripts/debian/build-google-cloud-cpp-package.sh.
(google-cloud-cpp.override {
  apis = [
    "logging"
    "oauth2"
    "pubsub"
    "storage"
  ];
}).overrideAttrs
  (orig: {
    version = "2.46.0";

    src = fetchFromGitHub {
      owner = "googleapis";
      repo = "google-cloud-cpp";
      tag = "v2.46.0";
      hash = "sha256-ylqio6wCW5Bl1XBeNZuNWPhWpmKJDoQnNg1FYAZ2pVo=";
    };
    installCheckPhase =
      let
        disabledTests = ''
          bigtable_internal_data_connection_impl_test
        ''
        + lib.optionalString stdenv.hostPlatform.isDarwin ''
          common_internal_async_connection_ready_test
          bigtable_async_read_stream_test
          bigtable_metadata_update_policy_test
          bigtable_bigtable_benchmark_test
          bigtable_embedded_server_test
          spanner_interval_test
        '';
      in
      ''
        runHook preInstallCheck

        # Disable any integration tests, which need to contact the internet.
        ctest \
          --label-exclude integration-test \
          --exclude-from-file <(echo '${disabledTests}')

        runHook postInstallCheck
      '';
  })
