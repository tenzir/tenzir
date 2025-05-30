The JSON printer previously printed invalid JSON for `inf` and `nan`, which
means that `serve` could sometimes emit invalid JSON, which is not handled well
by platform/app. Instead, we now emit `null`.
