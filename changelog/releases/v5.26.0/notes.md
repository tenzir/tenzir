This release adds link-based HTTP pagination for the `from_http` and `http` operators and introduces optional field parameters and secret-typed parameters for user-defined operators. It also performs recursive deep merging in the `merge()` function and improves `write_lines` performance.

## 🚀 Features

### Link header pagination for HTTP operators

The `paginate` parameter for the `from_http` and `http` operators now supports link-based pagination via the `Link` HTTP header.

Previously, pagination was only available through a lambda function that extracted the next URL from response data. Now you can use `paginate="link"` to automatically follow pagination links specified in the response's `Link` header, following RFC 8288. This is useful for APIs that use HTTP header-based pagination instead of embedding next URLs in the response body.

The operator parses the `Link` header and follows the `rel=next` relation to automatically fetch the next page of results.

Example:

```
from_http "https://api.example.com/data", paginate="link"
```

If an invalid pagination mode is provided (neither a lambda nor `"link"`), the operator now reports a clear error message.

*By @mavam and @claude.*

### Optional field parameters for user-defined operators

User-defined operators in packages can now declare optional field-type parameters with `null` as the default value. This allows operators to accept field selectors that are not required to be provided.

When a field parameter is declared with `type: field` and `default: null`, you can omit the argument when calling the operator, and the parameter will receive a `null` value instead. You can then check whether a field was provided by comparing the parameter to `null` within the operator definition.

Example:

In your package's operator definition, declare an optional field parameter:

```yaml
args:
  named:
    - name: selector
      type: field
      default: null
```

In the operator implementation, check if the field was provided:

```tql
set result = if $selector != null then "field provided" else "field omitted"
```

When calling the operator, the field argument becomes optional:

```tql
my_operator                    # field is null
my_operator selector=x.y       # field is x.y
```

Only `null` is allowed as the default value for field parameters. Non-null defaults are rejected with an error during package loading.

*By @mavam and @claude in #5753.*

## 🔧 Changes

### Secret type support for user-defined operator parameters

User-defined operators in packages can now declare parameters with the `secret` type to ensure that secret values are properly handled as secret expressions:

```
args:
  positional:
    - name: api_key
      type: secret
      description: "API key to use for authentication"
```

*By @mavam and @claude in #5752.*

## 🐞 Bug fixes

### Improve write_lines operator performance

We have significantly improved the performance of the `write_lines` operator.

*By @IyeOnline.*

### merge() function recursive deep merge for nested records

The `merge()` function now performs a recursive deep merge when merging two records. Previously, nested fields were dropped when merging, so `merge({hw: {sn: "XYZ123"}}, {hw: {model: "foobar"}})` would incorrectly produce `{hw: {model: "foobar"}}` instead of recursively merging the nested fields. The function now correctly produces `{hw: {sn: "XYZ123", model: "foobar"}}` by materializing both input records and performing a deep merge on them.

*By @mavam and @claude in #5728.*
