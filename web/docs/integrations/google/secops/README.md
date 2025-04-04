# SecOps

[Google Security Operations](https://cloud.google.com/security/products/security-operations)
is Google's security operations platform that enables detection, investigation
and response to incidents.

![Google Security Operations](secops.svg)

[operator-docs]: ../../../tql2/operators/to_google_secops.md

The [`to_google_secops`][operator-docs] operator makes it possible to send events
to Google SecOps using the[unstructured logs ingestion API]
(https://cloud.google.com/chronicle/docs/reference/ingestion-api#unstructuredlogentries).

## Configuration

You need to configure appropriate credentials using Google's [Application
Default Credentials](https://google.aip.dev/auth/4110).

## Examples

### Send a Single Event

```tql
from {log: "31-Mar-2025 01:35:02.187 client 0.0.0.0#4238: query: tenzir.com IN A + (255.255.255.255)"}
to_google_secops \
  customer_id="00000000-0000-0000-00000000000000000",
  private_key=secret("my_secops_key"),
  client_email="somebody@example.com",
  log_text=log,
  log_type="BIND_DNS",
  region="europe"
```
