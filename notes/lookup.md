# MongoDB `$lookup` Aggregation Stage - Complete Guide

## High-Level Summary

- `$lookup` performs a left outer join between documents in the current collection and documents in another collection
- It adds a new array field to each input document containing matching documents from the "joined" collection
- Similar to SQL LEFT JOIN, but results are embedded as arrays rather than flattened rows
- Use `$lookup` when you need to combine related data from multiple collections in a single aggregation pipeline

## Core Behavior

### Basic Syntax (Equality Match)

```javascript
{
  $lookup: {
    from: "foreignCollection",        // Collection to join with
    localField: "localFieldName",     // Field in current document
    foreignField: "foreignFieldName", // Field in foreign collection
    as: "outputArrayName"             // Name of new array field
  }
}
```

### Advanced Syntax (Pipeline-based)

```javascript
{
  $lookup: {
    from: "foreignCollection",
    let: { localVar: "$localField" }, // Variables from current document
    pipeline: [                       // Custom aggregation pipeline
      { $match: { $expr: { $eq: ["$foreignField", "$$localVar"] } } },
      { $project: { field1: 1, field2: 1 } }
    ],
    as: "outputArrayName"
  }
}
```

### Key Characteristics

- **Join type**: Always LEFT OUTER JOIN (all input documents are preserved)
- **Output format**: Adds an array field containing matching documents
- **Performance**: Can be expensive without proper indexing
- **Memory**: Pipeline-based lookups can use more memory for complex operations

## Basic Example

### Input Collections

```javascript
// users collection
{ _id: 1, name: "John", departmentId: "dept1" }
{ _id: 2, name: "Jane", departmentId: "dept2" }

// departments collection  
{ _id: "dept1", name: "Engineering", budget: 100000 }
{ _id: "dept2", name: "Marketing", budget: 50000 }
```

### Pipeline

```javascript
db.users.aggregate([
  {
    $lookup: {
      from: "departments",
      localField: "departmentId", 
      foreignField: "_id",
      as: "department"
    }
  }
])
```

### Output

```javascript
[
  {
    _id: 1,
    name: "John", 
    departmentId: "dept1",
    department: [
      { _id: "dept1", name: "Engineering", budget: 100000 }
    ]
  },
  {
    _id: 2,
    name: "Jane",
    departmentId: "dept2", 
    department: [
      { _id: "dept2", name: "Marketing", budget: 50000 }
    ]
  }
]
```

## Advanced Pipeline-Based Lookup

### Complex Join Conditions

```javascript
db.orders.aggregate([
  {
    $lookup: {
      from: "products",
      let: { 
        orderDate: "$orderDate",
        productIds: "$productIds" 
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $in: ["$_id", "$$productIds"] },
                { $lte: ["$releaseDate", "$$orderDate"] }
              ]
            }
          }
        },
        { $project: { name: 1, price: 1 } }
      ],
      as: "availableProducts"
    }
  }
])
```

### Multiple Variable Usage

```javascript
db.users.aggregate([
  {
    $lookup: {
      from: "orders",
      let: { 
        userId: "$_id",
        userType: "$type",
        minDate: { $dateSubtract: { startDate: "$$NOW", unit: "month", amount: 6 } }
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$customerId", "$$userId"] },
                { $gte: ["$orderDate", "$$minDate"] },
                { $or: [
                  { $eq: ["$$userType", "premium"] },
                  { $gte: ["$total", 100] }
                ]}
              ]
            }
          }
        }
      ],
      as: "recentOrders"
    }
  }
])
```

## Real-World Examples from Your Code

### External Key-Based Lookup

```javascript
// From your competition-full.js pipeline
{
  $lookup: {
    from: 'stages',
    let: { cid: '$_externalId', cs: '$_externalIdScope' },
    pipeline: [
      { 
        $match: { 
          $expr: { 
            $and: [
              { $eq: ['$_externalCompetitionId', '$$cid'] }, 
              { $eq: ['$_externalCompetitionIdScope', '$$cs'] }
            ] 
          } 
        } 
      },
      { $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } }
    ],
    as: 'stages'
  }
}
```

### Array-Based Lookups with String Concatenation

```javascript
// From your competition-full.js pipeline
{
  $lookup: {
    from: 'events',
    let: { keys: '$_stageKeys' },
    pipeline: [
      {
        $match: {
          $expr: {
            $in: [
              { $concat: ['$_externalStageId', '|', '$_externalStageIdScope'] }, 
              '$$keys'
            ]
          }
        }
      },
      {
        $project: {
          _id: 1,
          _externalVenueId: 1,
          _externalVenueIdScope: 1,
          participants: 1
        }
      }
    ],
    as: 'events'
  }
}
```

### Multiple Field Matching with Complex Keys

```javascript
// From your competition-full.js pipeline
{
  $lookup: {
    from: 'venues',
    let: { keys: '$venueKeys' },
    pipeline: [
      {
        $match: {
          $expr: { 
            $in: [
              { $concat: ['$_externalId', '|', '$_externalIdScope'] }, 
              { 
                $map: { 
                  input: '$$keys', 
                  as: 'k', 
                  in: { $concat: ['$$k.id', '|', '$$k.scope'] } 
                } 
              }
            ] 
          }
        }
      },
      { $project: { _id: 1 } }
    ],
    as: 'venueDocs'
  }
}
```

## Performance Considerations

### Indexing Strategy

```javascript
// Create indexes on foreign collection fields used in lookups
db.stages.createIndex({ "_externalCompetitionId": 1, "_externalCompetitionIdScope": 1 })
db.events.createIndex({ "_externalStageId": 1, "_externalStageIdScope": 1 })
db.venues.createIndex({ "_externalId": 1, "_externalIdScope": 1 })

// For concatenated string matching
db.events.createIndex({ 
  "$**": "text"  // Text index for string operations (less efficient)
})

// Better: create computed index
db.events.createIndex({ 
  "stageKey": 1  // Pre-computed field: _externalStageId + "|" + _externalStageIdScope
})
```

### Memory and Performance Tips

```javascript
// GOOD: Limit and project early in lookup pipeline
{
  $lookup: {
    from: "largeCollection",
    let: { id: "$_id" },
    pipeline: [
      { $match: { $expr: { $eq: ["$parentId", "$$id"] } } },
      { $limit: 10 },                    // Limit early
      { $project: { _id: 1, name: 1 } }, // Project only needed fields
      { $sort: { createdAt: -1 } }       // Sort after limiting
    ],
    as: "related"
  }
}

// LESS EFFICIENT: Full lookup then limit
{
  $lookup: {
    from: "largeCollection",
    localField: "_id",
    foreignField: "parentId", 
    as: "related"
  }
},
{ $addFields: { 
    related: { $slice: ["$related", 10] }  // Slice after full lookup
  }
}
```

### Optimization Patterns

```javascript
// Use $facet to parallelize multiple lookups
{
  $facet: {
    stages: [{
      $lookup: {
        from: "stages",
        // ... stage lookup
      }
    }],
    events: [{
      $lookup: {
        from: "events", 
        // ... event lookup
      }
    }],
    venues: [{
      $lookup: {
        from: "venues",
        // ... venue lookup  
      }
    }]
  }
}
```

## Common Patterns and Use Cases

### 1. One-to-Many Relationships

```javascript
// Get user with all their orders
{
  $lookup: {
    from: "orders",
    localField: "_id",
    foreignField: "userId",
    as: "orders"
  }
}
```

### 2. Many-to-Many Relationships

```javascript
// Users with roles through junction collection
{
  $lookup: {
    from: "userRoles",
    localField: "_id", 
    foreignField: "userId",
    as: "roleAssignments"
  }
},
{
  $lookup: {
    from: "roles",
    localField: "roleAssignments.roleId",
    foreignField: "_id",
    as: "roles"
  }
}
```

### 3. Conditional Lookups

```javascript
{
  $lookup: {
    from: "premiumFeatures",
    let: { userType: "$type", userId: "$_id" },
    pipeline: [
      {
        $match: {
          $expr: {
            $and: [
              { $eq: ["$userId", "$$userId"] },
              { $eq: ["$$userType", "premium"] }  // Only lookup if premium
            ]
          }
        }
      }
    ],
    as: "premiumFeatures"
  }
}
```

### 4. Self-Referencing Lookups

```javascript
// Employee hierarchy
{
  $lookup: {
    from: "employees",  // Same collection
    localField: "_id",
    foreignField: "managerId",
    as: "directReports"
  }
}
```

### 5. Cross-Database Lookups (MongoDB 3.6+)

```javascript
{
  $lookup: {
    from: {
      db: "otherDatabase",
      coll: "otherCollection"
    },
    localField: "foreignKey",
    foreignField: "_id", 
    as: "crossDbData"
  }
}
```

## Edge Cases and Gotchas

### No Matches

```javascript
// Input document
{ _id: 1, name: "John", departmentId: "nonexistent" }

// Lookup result - empty array, document is preserved
{
  _id: 1,
  name: "John", 
  departmentId: "nonexistent",
  department: []  // Empty array, not null
}
```

### Multiple Matches  

```javascript
// If multiple documents match, all are included in the array
{
  _id: 1,
  name: "John",
  orders: [
    { _id: "order1", total: 100 },
    { _id: "order2", total: 200 },
    { _id: "order3", total: 150 }
  ]
}
```

### Null/Missing Local Field

```javascript
// Input documents with null/missing localField
{ _id: 1, name: "John", departmentId: null }
{ _id: 2, name: "Jane" }  // missing departmentId

// Both result in empty lookup arrays
[
  { _id: 1, name: "John", departmentId: null, department: [] },
  { _id: 2, name: "Jane", department: [] }
]
```

### BSON Document Size Limit

```javascript
// Be careful with large lookup results
{
  $lookup: {
    from: "largeCollection",
    // ... lookup config
    pipeline: [
      { $limit: 1000 }  // Prevent hitting 16MB document limit
    ]
  }
}
```

## Error Handling and Validation

### Pipeline Validation

```javascript
// Invalid: cannot use $lookup in lookup pipeline's $lookup
{
  $lookup: {
    from: "collection1",
    pipeline: [
      {
        $lookup: {  // This will cause an error in older MongoDB versions
          from: "collection2",
          // ...
        }
      }
    ],
    as: "result"
  }
}
```

### Performance Monitoring

```javascript
// Use explain to analyze lookup performance
db.collection.aggregate([
  // ... your pipeline with $lookup
]).explain("executionStats")

// Look for:
// - totalDocsExamined in lookup stages
// - Index usage in winningPlan
// - executionTimeMillis for each stage
```

## When to Use `$lookup`

### Good Use Cases

- **Related data enrichment**: Adding details from reference collections
- **Denormalization**: Creating rich documents for read-heavy workloads  
- **Complex reporting**: Joining multiple collections for analytics
- **Data validation**: Checking references exist in other collections

### When NOT to Use `$lookup`

- **Simple reference resolution**: Consider embedding if data is small and static
- **High-frequency operations**: Pre-compute joins and store results
- **Very large result sets**: May hit memory/document size limits
- **Real-time applications**: Can be slower than separate queries with caching

## Alternatives to Consider

### Embedding (Document Design)

```javascript
// Instead of lookup, embed related data
{
  _id: 1,
  name: "John",
  department: {  // Embedded instead of referenced
    _id: "dept1", 
    name: "Engineering"
  }
}
```

### Application-Level Joins

```javascript
// Multiple queries in application code
const users = await db.users.find({}).toArray()
const departmentIds = users.map(u => u.departmentId)
const departments = await db.departments.find({ _id: { $in: departmentIds } }).toArray()
// Join in application code
```

### Pre-computed Views/Collections

```javascript
// Materialized view approach (like your materialisedAggregations)
{
  $merge: { 
    into: 'userDepartmentView',
    on: ['_id'],
    whenMatched: 'replace'
  }
}
```

## Pre-Use Checklist

Before using `$lookup`, consider:

- ✅ Are the foreign collection fields properly indexed?
- ✅ Will the lookup result arrays fit within BSON document size limits?
- ✅ Can you filter/limit within the lookup pipeline to reduce data?
- ✅ Is this lookup needed frequently enough to justify the performance cost?
- ✅ Would embedding or pre-computation be more appropriate?
- ✅ Are you using pipeline-based lookup for complex conditions?
- ✅ Have you tested with realistic data volumes?

---

*`$lookup` is essential for joining collections in MongoDB, but requires careful consideration of indexing, data size, and performance implications. Your competition-full.js pipeline shows sophisticated usage of pipeline-based lookups with complex key matching.*
