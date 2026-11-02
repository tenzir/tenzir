This release removes trust in eMudhra intermediate certificate authorities whose private keys leaked. The container image now ships the NSS 3.131.1 root store, which gRPC and LibreSSL also use.

## 🐞 Bug fixes

### Distrust the leaked eMudhra CA keys

The container image no longer trusts certificates issued by eMudhra intermediate CAs whose private keys leaked. It now ships the NSS 3.131.1 root store, and the root stores bundled with gRPC and LibreSSL point at that same store.

*By @lava.*
