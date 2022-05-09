Fixed an issue with the query optimizer where
queries that contained conjunctions or disjunctions
with several operands testing against the same string
value would be incorrectly transformed, leading to
missing results.
