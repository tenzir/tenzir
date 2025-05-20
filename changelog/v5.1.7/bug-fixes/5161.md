For configured pipelines, the `tenzir.pipelines.<pipeline>.disabled`
configuration option was silently ignored unless the pipeline was part of a
package. This no longer happens, and disabling the pipelines through the option
now works correctly.
