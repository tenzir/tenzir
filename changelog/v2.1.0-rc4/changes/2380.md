VAST will from now on always format `time` and `timestamp` values with six
decimal places (microsecond precision) instead of the old value dependent
dynamic precision. This may require action for downstream tooling like metrics
collectors that expect nanosecond granularity.
