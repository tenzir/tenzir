The `/serve` endpoint now gracefully handles retried requests with the same
continuation token, returning the same result for each request.
