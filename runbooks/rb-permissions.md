---
id: rb-permissions
title: Permission denied on a file, bucket or table
root_cause: permissions
symptoms:
  - PermissionError Errno 13 Permission denied
  - permission denied for table from the database
  - AccessDenied / 403 Forbidden from object storage
  - Task can connect but cannot read or write the resource
---
# Permission denied on a file, bucket or table

The task authenticated fine but isn't allowed to touch the resource. This often follows
a role, grant or ownership change.

## Diagnosis
1. Find the resource and the operation (read/write) that was denied in the task log.
2. Check which identity the task runs as and what grants that identity has.
3. Check recent IAM / GRANT / chown changes.

## Fix
1. Restore the missing grant for the pipeline's identity (ask the resource owner).
2. Rerun the failed task.
