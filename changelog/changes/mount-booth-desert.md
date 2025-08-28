---
title: "Misleading `from_file remove=true` warning"
type: bugfix
authors: jachris
pr: 5438
---

The `from_file` operator emits a warning when using `remove=true` if the file
could not be removed. When deleting the last file inside an S3 directory, we
keep that directory around by inserting a zero-sized object. However, this
failed when the necessary `PutObject` permissions were not granted, thus
emitting a warning even though the file was removed successfully. For this
specific case, we thus no longer emit a warning. Other issues during file
deletion are still reported.
