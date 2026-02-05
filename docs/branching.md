# NetBox Branching Integration

TurboBulk provides full integration with [netbox-branching](https://github.com/netbox-community/netbox-branching), enabling branch-aware bulk operations. This allows you to stage large data changes in isolation before merging to production.

## Overview

NetBox Branching provides a Git-like workflow for NetBox data: create a branch, make changes, review, and merge. TurboBulk extends this by enabling high-performance bulk operations within branches.

**Key Benefits:**
- Load thousands of objects into a branch for review
- Isolated testing of bulk data changes
- Full conflict detection with main schema
- Audit trail preserved through merge
- Rollback by simply deleting the branch

## Prerequisites

Before using TurboBulk with branching:

- **NetBox Branching enabled** - Your NetBox Cloud or Enterprise instance must have NetBox Branching enabled. Contact NetBox Labs support if you need this feature.
- **API token** with appropriate permissions for both target models and branching operations

## Quick Start

### 1. Create a Branch

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "datacenter-migration", "description": "Migrating DC1 to DC2"}' \
  "https://your-instance-name.cloud.netboxapp.com/api/plugins/netbox-branching/branches/"
```

Wait for the branch status to become `ready`:
```bash
curl -H "Authorization: Bearer $TOKEN" \
  "https://your-instance-name.cloud.netboxapp.com/api/plugins/netbox-branching/branches/?name=datacenter-migration"
```

### 2. Bulk Load to Branch

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=insert" \
  -F "branch=datacenter-migration" \
  -F "file=@devices.parquet" \
  "https://your-instance-name.cloud.netboxapp.com/api/plugins/turbobulk/load/"
```

### 3. Review Changes

In NetBox UI, navigate to the branch to view:
- All objects created/modified/deleted
- Conflict status with main
- Diff view showing before/after

### 4. Merge to Main

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  "https://your-instance-name.cloud.netboxapp.com/api/plugins/netbox-branching/branches/{branch_id}/merge/"
```

## How It Works

### Schema Routing

When you specify a `branch` parameter, TurboBulk:

1. **Validates the branch** - Ensures it exists and is in READY state
2. **Activates branch context** - Routes all operations to the branch
3. **Routes operations to branch** - All bulk operations target the branch in isolation
4. **Generates ChangeDiffs** - Creates records for merge conflict detection

### ObjectChange Records

ObjectChange records are created in the branch schema, not main:
- They're visible when viewing the branch
- They survive the merge process
- They provide full audit trail of what changed

### Change Tracking

TurboBulk automatically tracks all changes made to branch data:
- Each created, updated, or deleted object is recorded
- Original and modified states are captured for the diff view
- This information powers NetBox's branch comparison and merge UI

### Conflict Detection

TurboBulk automatically detects conflicts with main after generating ChangeDiffs.

Conflicts are flagged when:
- An object you modified in the branch was also modified in main
- An object you deleted in the branch was modified in main
- An object you created has a conflicting unique constraint in main

## Supported Operations

### Bulk Insert to Branch

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=insert" \
  -F "branch=my-branch" \
  -F "file=@devices.parquet" \
  "https://your-instance-name.cloud.netboxapp.com/api/plugins/turbobulk/load/"
```

Creates new objects in the branch. ChangeDiffs are created with `action=create`.

### Bulk Upsert to Branch

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "mode=upsert" \
  -F "conflict_fields=name" \
  -F "branch=my-branch" \
  -F "file=@devices.parquet" \
  "https://your-instance-name.cloud.netboxapp.com/api/plugins/turbobulk/load/"
```

Inserts new objects and updates existing ones. ChangeDiffs are created with:
- `action=create` for new objects
- `action=update` for modified objects (includes original_data)

### Bulk Delete from Branch

```bash
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -F "model=dcim.device" \
  -F "branch=my-branch" \
  -F "file=@device_ids.parquet" \
  "https://your-instance-name.cloud.netboxapp.com/api/plugins/turbobulk/delete/"
```

Deletes objects from the branch. ChangeDiffs are created with `action=delete` and include `original_data`.

## Workflow Examples

### Example 1: New Datacenter Provisioning

1. **Create branch for the new datacenter**
   ```bash
   curl -X POST ... -d '{"name": "dc2-provisioning"}'
   ```

2. **Load sites, racks, and devices**
   ```bash
   # Sites
   curl -X POST ... -F "branch=dc2-provisioning" -F "file=@sites.parquet" ...

   # Racks
   curl -X POST ... -F "branch=dc2-provisioning" -F "file=@racks.parquet" ...

   # Devices
   curl -X POST ... -F "branch=dc2-provisioning" -F "file=@devices.parquet" ...
   ```

3. **Review in NetBox UI** - Check all objects are correct

4. **Merge to main** - Datacenter goes live

### Example 2: CMDB Sync with Review

1. **Create a sync branch**
   ```bash
   curl -X POST ... -d '{"name": "cmdb-sync-2024-01"}'
   ```

2. **Export current state from NetBox**
   ```bash
   curl -X POST ... -d '{"model": "dcim.device"}'
   # Download and compare with CMDB
   ```

3. **Bulk upsert changes to branch**
   ```bash
   curl -X POST ... -F "mode=upsert" -F "branch=cmdb-sync-2024-01" ...
   ```

4. **Bulk delete removed devices**
   ```bash
   curl -X POST ... -F "branch=cmdb-sync-2024-01" -F "file=@deleted.parquet" ...
   ```

5. **Review diff** - Verify changes match expected CMDB delta

6. **Merge or discard** - Merge if correct, delete branch if issues found

### Example 3: Rollback via Branch Delete

If something goes wrong with a branch load:

```bash
# Check branch status
curl ... "https://your-instance-name.cloud.netboxapp.com/api/plugins/netbox-branching/branches/?name=my-branch"

# Delete the branch (discards all changes)
curl -X DELETE ... "https://your-instance-name.cloud.netboxapp.com/api/plugins/netbox-branching/branches/{branch_id}/"
```

All changes are discarded - main schema is unchanged.

## Error Handling

### Branch Validation Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `Branch 'xyz' not found` | Branch doesn't exist | Create the branch first |
| `Branch 'xyz' is not in READY state` | Branch is provisioning | Wait for status=ready |
| `netbox-branching plugin not installed` | Feature not enabled | Contact support to enable branching |

### Operation Errors

Branch operations can fail for the same reasons as main operations:
- Foreign key violations
- Unique constraint violations
- Permission denied

The difference is these errors only affect the branch - main remains unchanged.

## Performance Considerations

### Branch Overhead

Operating in a branch adds minimal overhead compared to main schema operations.

### Large Branch Merges

When merging a branch with many TurboBulk changes:
- Merge time scales with the number of changes in the branch
- Consider breaking very large changes into multiple branches
- Review changes before merging to catch issues early

## Limitations

### Current Limitations

1. **Branch must exist before TurboBulk operation**
   - TurboBulk doesn't create branches
   - Create via NetBox UI or branching API

2. **Branch must be in READY state**
   - Can't load to PROVISIONING or MERGING branches
   - Poll status until ready

3. **No cross-branch operations**
   - Can't load to multiple branches in one request
   - Submit separate requests per branch

4. **Export doesn't support branch parameter**
   - Export always reads from the schema context
   - Set branch header in request for branch export

### Edge Cases

1. **Branch sync during TurboBulk operation**
   - If main schema syncs to branch during operation, results undefined
   - Avoid syncing while bulk operations are in progress

2. **FK references across schemas**
   - FKs must reference objects in the same schema
   - Load parent objects to branch before children

## Troubleshooting

### Branch Not Found

```
{"branch": ["Branch 'my-branch' not found"]}
```

**Resolution:** Create the branch first, or check the name spelling.

### Branch Not Ready

```
{"branch": ["Branch 'my-branch' is not in READY state (status: provisioning)"]}
```

**Resolution:** Wait for branch provisioning to complete. Poll status:
```bash
watch -n 5 "curl -s -H 'Authorization: Bearer $TOKEN' \
  'http://netbox/api/plugins/netbox-branching/branches/?name=my-branch' | jq '.[0].status'"
```

### Branching Not Available

```
{"branch": ["netbox-branching plugin not installed"]}
```

**Resolution:** NetBox Branching may not be enabled on your instance. Contact NetBox Labs support to enable this feature.

### Merge Conflicts

If merge fails due to conflicts:
1. View conflicts in NetBox branch UI
2. Resolve conflicts (choose branch or main version)
3. Retry merge

For TurboBulk-generated changes, conflicts typically occur when:
- Same object was manually edited in main
- Unique constraint would be violated after merge
