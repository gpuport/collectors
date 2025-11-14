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

## Implemented Strategy

### Actual Implementation (Per-GPU-Type Queries)

**IMPORTANT**: After discovering that `nodeGroupDatacenters` is unreliable (see RUNPOD_NODEGROUPDATACENTERS_BUG.md), we implemented a different strategy that queries all datacenters.

**Three-Phase Approach:**

1. **GPU Discovery** - Get all GPU types:
```graphql
query {
  gpuTypes {
    id
    displayName
    memoryInGb
    cudaCores
  }
}
```
- Returns all 41 GPU types
- No reliance on nodeGroupDatacenters

2. **Datacenter Discovery** - Get all datacenters:
```graphql
query {
  dataCenters {
    id
    name
  }
}
```
- Returns all 39 datacenters
- Complete coverage

3. **Pricing Queries** - One query per GPU type:
```graphql
query {
  gpuTypes(input: { id: "NVIDIA A100 80GB SXM" }) {
    id
    displayName
    memoryInGb
    cudaCores
    eu_ro_1: lowestPrice(input: { dataCenterId: "EU-RO-1", gpuCount: 1 }) { ... }
    us_tx_1: lowestPrice(input: { dataCenterId: "US-TX-1", gpuCount: 1 }) { ... }
    # ... all 39 datacenters
  }
}
```
- 41 queries (one per GPU type)
- Each query has 39 datacenter aliases
- Max 3 concurrent via asyncio.Semaphore
- Filter client-side based on stockStatus

### Why This Approach

**Initial optimization idea (using nodeGroupDatacenters):**
- 41 GPUs × 5 active DCs = 205 pricing queries
- Seems efficient (87% reduction)

**Problem discovered:**
- nodeGroupDatacenters returns empty arrays for available GPUs
- RTX 4090 showed `nodeGroupDatacenters: []` but was available in 7 datacenters
- Would hide available inventory

**Final implementation:**
- 43 total queries (2 discovery + 41 pricing)
- Rate limited to 3 concurrent requests
- Complete coverage of all GPU-datacenter combinations
- Client-side filtering ensures we only create instances for available GPUs
- Execution time: ~6.3 seconds
- Result: 78 instances across 25 GPU types (vs 3 types with nodeGroupDatacenters approach)

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

**The actual implementation prioritizes completeness over optimization.**

Due to the `nodeGroupDatacenters` bug (documented in RUNPOD_NODEGROUPDATACENTERS_BUG.md), the implemented approach:

1. Queries ALL datacenters for ALL GPU types (complete coverage)
2. Uses per-GPU-type queries with rate limiting (manageable complexity)
3. Filters client-side based on actual pricing responses (reliable)

**Trade-offs:**
- **Completeness**: Finds all available GPUs (25 types vs 3 with nodeGroupDatacenters)
- **Performance**: ~6.3 seconds for 43 queries (acceptable)
- **Reliability**: No reliance on unreliable metadata
- **Simplicity**: Straightforward logic, easy to understand

**Alternative considered but rejected:**
- Use `nodeGroupDatacenters` for filtering: More efficient but unreliable
- Result: Would hide RTX 4090 and other available inventory

The current implementation chooses correctness over optimization.
