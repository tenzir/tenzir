Queries that resulted in zero results sometimes caused query workers to become
stuck, possibly resulting in a deadlock when this affected all query workers.
This now works as expected again.
