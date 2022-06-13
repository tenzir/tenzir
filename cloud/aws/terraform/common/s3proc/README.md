# New object processing on S3 using Lambda

This modules triggers `vast_lambda_name` each time a new object is created in
the `source_bucket_name`.

The `vast_lambda_name` is triggered with an event of the form:
```json
{
  "cmd": "base64(import_cmd)",
  "env": {
      "SRC_KEY": "key/of/the/new/object"
  }
}
```
