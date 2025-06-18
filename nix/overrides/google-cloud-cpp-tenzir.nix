{
  stdenv,
  google-cloud-cpp,
}:
if stdenv.hostPlatform.isStatic then
  google-cloud-cpp.override {
    apis = [
      "pubsub"
      "storage"
    ];
  }
else
  google-cloud-cpp
