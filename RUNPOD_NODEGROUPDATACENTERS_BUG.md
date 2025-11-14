# RunPod API Bug: nodeGroupDatacenters is Unreliable

**Date Discovered:** 2025-01-14

## Issue

The `nodeGroupDatacenters` field on `gpuTypes` is **unreliable and incomplete**. It returns empty arrays for GPUs that are actually available in multiple datacenters.

## Evidence

### RTX 4090 Example

**nodeGroupDatacenters response:**
```json
{
  "id": "NVIDIA GeForce RTX 4090",
  "displayName": "RTX 4090",
  "nodeGroupDatacenters": []
}
```

**Actual availability via lowestPrice query:**
RTX 4090 is available in **7 datacenters**:
- EU-RO-1: High stock, $0.34/hr
- EUR-IS-1: Medium stock, $0.34/hr
- EUR-IS-2: Medium stock, $0.34/hr
- EUR-NO-1: Low stock, $0.34/hr
- US-IL-1: High stock, $0.34/hr
- US-NC-1: Low stock, $0.34/hr
- US-TX-3: Low stock, $0.34/hr

### Impact

If we rely on `nodeGroupDatacenters` for optimization:
- **RTX 4090**: Shows 0 datacenters, actually available in 7 (100% miss rate)
- **Other GPUs**: May also have incomplete data

This means using `nodeGroupDatacenters` to filter which datacenters to query will **hide available inventory**.

## Correct Approach

**DO:**
1. Query `dataCenters` to get all datacenters (39 total)
2. Query pricing for ALL GPU types across ALL datacenters
3. Filter client-side based on non-null `stockStatus` in pricing responses

**DON'T:**
- Use `nodeGroupDatacenters` for filtering which datacenters to query
- Assume empty `nodeGroupDatacenters` means no availability

## Query Pattern

```graphql
# Get all datacenters
query {
  dataCenters {
    id
    name
  }
}

# Get pricing for all GPU types across all datacenters
query {
  gpuTypes {
    id
    displayName
    memoryInGb
    # Alias for each datacenter
    eu_ro_1: lowestPrice(input: { dataCenterId: "EU-RO-1", gpuCount: 1 }) {
      stockStatus
      uninterruptablePrice
      minimumBidPrice
    }
    # ... repeat for all 39 datacenters
  }
}
```

**Result:** 41 GPU types × 39 datacenters = 1,599 combinations in ONE API call.

## Performance Consideration

While this queries more combinations than seem necessary:
- It's a single GraphQL query (not 1,599 separate requests)
- GraphQL is designed to handle this efficiently
- The alternative (using `nodeGroupDatacenters`) hides available inventory
- Completeness > false optimization

## Conclusion

The `nodeGroupDatacenters` field appears to be a metadata field that may not be kept in sync with actual availability. Always query pricing directly to discover true availability.
