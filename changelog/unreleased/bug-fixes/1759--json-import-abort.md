VAST does not abort an import of JSON data anymore when encountering
something other than a JSON object (ie. a number or a string).
Instead, the offending line is skipped.
