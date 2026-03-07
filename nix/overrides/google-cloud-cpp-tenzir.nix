{
  lib,
  stdenv,
  fetchFromGitHub,
  google-cloud-cpp,
}:
(
  if stdenv.hostPlatform.isStatic then
    google-cloud-cpp.override {
      apis = [
        "pubsub"
        "storage"
      ];
    }
  else
    google-cloud-cpp
).overrideAttrs
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
