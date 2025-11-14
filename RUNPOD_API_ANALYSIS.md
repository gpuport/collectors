# RunPod GraphQL API Analysis

## Key Findings

### 1. What queries are available?

- **`dataCenters`**: Returns all 39 RunPod datacenters (static list)
- **`gpuTypes`**: Returns all 41 GPU types with optional filtering
- **`gpuTypes.nodeGroupDatacenters`**: Lists datacenters where each GPU is available
- **`gpuTypes.lowestPrice`**: Returns pricing for a GPU (optionally filtered by datacenter)

### 2. How does lowestPrice work?

**WITHOUT dataCenterId** (Query 3):
```graphql
lowestPrice(input: { gpuCount: 1 }) {
  stockStatus
  uninterruptablePrice
  minimumBidPrice
}
```
- Returns the GLOBAL lowest price across ALL datacenters
- NOT useful for per-datacenter availability

**WITH dataCenterId** (Query 4):
```graphql
eu_ro_1: lowestPrice(input: { dataCenterId: "EU-RO-1", gpuCount: 1 }) {
  stockStatus
  uninterruptablePrice
  minimumBidPrice
}
```
- Returns pricing specific to that datacenter
- Returns `null` if GPU not available in that datacenter
- Can use aliases to query multiple datacenters in ONE query

### 3. What does nodeGroupDatacenters return?

**Query:**
```graphql
gpuTypes {
  id
  nodeGroupDatacenters {
    id
    name
  }
}
```

**Result:**
- At query time: Only 3 out of 41 GPUs had non-empty `nodeGroupDatacenters`
- 38 GPUs returned `nodeGroupDatacenters: []` (completely unavailable)
- This tells us which GPUs are available in which datacenters

### 4. Multi-Datacenter Query Performance

**Single query with 5 datacenter aliases:**
```graphql
gpuTypes {
  id
  displayName
  eu_ro_1: lowestPrice(input: { dataCenterId: "EU-RO-1", gpuCount: 1 }) { ... }
  us_tx_1: lowestPrice(input: { dataCenterId: "US-TX-1", gpuCount: 1 }) { ... }
  ca_mtl_1: lowestPrice(input: { dataCenterId: "CA-MTL-1", gpuCount: 1 }) { ... }
  # ... more datacenters
}
```

**Result:**
- ✅ Works perfectly - returns 41 GPU types
- Each GPU has pricing for all requested datacenters (or null if unavailable)
- Single API call fetches pricing across multiple datacenters

**Performance:**
- Queried: 41 GPUs × 5 datacenters = 205 combinations
- Returned: 205 entries (all non-null - API returns data even for unavailable)
- GPUs with actual pricing: 16 GPUs
- Waste: API returns placeholder data for unavailable combos

## Optimal Strategy

### Current Implementation (Already Optimal!)

**Two Query Approach:**

1. **Discovery Query** - Get GPU-to-datacenter mapping:
```graphql
query {
  gpuTypes {
    id
    displayName
    memoryInGb
    cudaCores
    nodeGroupDatacenters {
      id
      name
    }
  }
}
```
- Tells us which GPUs are available in which datacenters
- Reveals that 38/41 GPUs have zero availability

2. **Pricing Query** - Only query datacenters with availability:
```graphql
query {
  gpuTypes {
    id
    displayName
    memoryInGb
    cudaCores
    eu_ro_1: lowestPrice(...) { ... }  # Only if this DC has any GPU
    us_tx_1: lowestPrice(...) { ... }  # Only if this DC has any GPU
    # Only include DCs that appeared in nodeGroupDatacenters
  }
}
```

### Why This Is Optimal

**Without optimization (query all 39 DCs):**
- 41 GPUs × 39 DCs = 1,599 pricing queries
- Most return null/unavailable

**With optimization (query only active DCs):**
- Discovery shows only 5 DCs have any GPUs
- 41 GPUs × 5 DCs = 205 pricing queries
- Reduction: 1,599 → 205 (87% fewer queries)

**Further optimization (only query relevant GPU-DC pairs):**
- Parse nodeGroupDatacenters to see which GPUs are where
- Only create instances for GPU-DC combinations that exist
- At query time: Only 5 actual combinations existed
- Result: Create only 5 instances instead of 205

## Answers to Your Questions

### Q: Should we frequently get the list of datacenters?

**A: No, we don't need dataCenters query at all!**

The `nodeGroupDatacenters` field on `gpuTypes` already tells us:
- Which datacenters exist (implicitly)
- Which datacenters have GPUs available (explicitly)

The `dataCenters` query returns all 39 datacenters, but most are empty at any given time.

### Q: How do we get all GPU types?

**A: Simple query:**
```graphql
gpuTypes {
  id
  displayName
  memoryInGb
  cudaCores
}
```
Returns all 41 GPU types.

### Q: How do we get datacenters where GPUs are available?

**A: Use nodeGroupDatacenters:**
```graphql
gpuTypes {
  id
  nodeGroupDatacenters {
    id
    name
  }
}
```
This reveals:
- GPU "A100 SXM" is in: [EU-RO-1, US-TX-1]
- GPU "H100 SXM" is in: [EU-RO-1, US-OR-1]
- GPU "RTX 4090" is in: [] (not available anywhere)

### Q: How do we get pricing for available combinations?

**A: Use lowestPrice with datacenter aliases:**
```graphql
gpuTypes {
  id
  displayName
  eu_ro_1: lowestPrice(input: { dataCenterId: "EU-RO-1", gpuCount: 1 }) {
    stockStatus
    uninterruptablePrice
    minimumBidPrice
  }
  us_tx_1: lowestPrice(input: { dataCenterId: "US-TX-1", gpuCount: 1 }) {
    stockStatus
    uninterruptablePrice
    minimumBidPrice
  }
}
```

Only include datacenter aliases that appeared in `nodeGroupDatacenters`.

## Conclusion

**Your current implementation is actually CORRECT and OPTIMAL!**

The two-step approach:
1. Get `nodeGroupDatacenters` to find GPU-datacenter mapping
2. Query pricing only for datacenters with availability
3. Create instances only for combinations in the mapping

This achieves 99.7% efficiency (from 1,599 down to 5 instances created).

The only consideration is whether to:
- Query ALL 39 datacenters and filter client-side (simpler)
- Query ONLY active datacenters based on mapping (more efficient)

Current implementation does the latter, which is optimal.
