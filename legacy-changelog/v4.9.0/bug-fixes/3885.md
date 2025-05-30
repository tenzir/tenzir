The `/serve` API sometimes returned an empty string for the next continuation
token instead of `null` when there are no further results to fetch. It now
consistently returns `null`.
