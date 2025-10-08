# MongoDB `$addToSet` Accumulator Operator - Complete Guide

## High-Level Summary

- `$addToSet` is an accumulator operator used within `$group` stages to create arrays of unique values
- It collects values from a specified field across all documents in each group, automatically removing duplicates
- Unlike `$push` which includes all values (including duplicates), `$addToSet` ensures each value appears only once
- The resulting array contains unique values in no guaranteed order
- Use `$addToSet` when you need to collect distinct values from grouped documents

## Core Behavior

### Basic Syntax

```javascript
{
  $group: {
    _id: <group key>,
    <arrayField>: { $addToSet: <expression> }
  }
}
```

### Key Characteristics

- **Uniqueness**: Automatically deduplicates values
- **Data type**: Can handle any BSON data type (strings, numbers, objects, arrays, etc.)
- **Order**: No guaranteed order in the resulting array
- **Null handling**: null values are treated as distinct values and included once if present
- **Missing fields**: Missing fields are ignored (not added as null)
- **Performance**: Generally efficient, but memory usage grows with unique value count

## Basic Examples

### Simple Field Collection

```javascript
// Input documents
[
  { _id: 1, category: "electronics", tag: "mobile" },
  { _id: 2, category: "electronics", tag: "laptop" },
  { _id: 3, category: "electronics", tag: "mobile" },  // duplicate
  { _id: 4, category: "books", tag: "fiction" },
  { _id: 5, category: "books", tag: "non-fiction" }
]

// Pipeline
db.products.aggregate([
  {
    $group: {
      _id: "$category",
      uniqueTags: { $addToSet: "$tag" }
    }
  }
])

// Output
[
  { _id: "electronics", uniqueTags: ["mobile", "laptop"] },     // "mobile" appears once
  { _id: "books", uniqueTags: ["fiction", "non-fiction"] }
]
```

### Collecting Complex Objects

```javascript
// Input documents
[
  { _id: 1, userId: "user1", address: { city: "NYC", state: "NY" } },
  { _id: 2, userId: "user1", address: { city: "NYC", state: "NY" } },  // duplicate object
  { _id: 3, userId: "user1", address: { city: "LA", state: "CA" } },
  { _id: 4, userId: "user2", address: { city: "Chicago", state: "IL" } }
]

// Pipeline
db.orders.aggregate([
  {
    $group: {
      _id: "$userId",
      uniqueAddresses: { $addToSet: "$address" }
    }
  }
])

// Output
[
  { 
    _id: "user1", 
    uniqueAddresses: [
      { city: "NYC", state: "NY" },      // Duplicate object removed
      { city: "LA", state: "CA" }
    ] 
  },
  { 
    _id: "user2", 
    uniqueAddresses: [
      { city: "Chicago", state: "IL" }
    ] 
  }
]
```

## Real-World Examples from Your Pipeline

### Collecting SGO IDs

```javascript
// From your competition-full.js SGO facet
{
  $group: { 
    _id: null,                           // Group all documents together
    ids: { $addToSet: '$sgo._id' }      // Collect unique SGO _id values
  }
}

// What this does:
// - Takes all documents that have SGO lookups
// - Extracts the '_id' field from each '$sgo' object
// - Creates an array of unique SGO ObjectIds
// - If multiple documents reference the same SGO, it appears only once

// Example result:
{ _id: null, ids: [ObjectId("..."), ObjectId("..."), ObjectId("...")] }
```

### Collecting Stage Keys with Complex Objects

```javascript
// From your competition-full.js stages facet
{
  $group: { 
    _id: null,
    ids: { $addToSet: '$stages._id' },              // Unique stage ObjectIds
    stageKeys: { 
      $addToSet: { 
        id: '$stages._externalId',                   // Build unique key objects
        scope: '$stages._externalIdScope' 
      } 
    }
  }
}

// What this does:
// - Collects unique stage _id values in 'ids' array
// - Collects unique {id, scope} objects in 'stageKeys' array
// - If two stages have the same _externalId AND _externalIdScope, only one key object is kept
// - Objects are compared by their field values for uniqueness

// Example result:
{
  _id: null,
  ids: [ObjectId("stage1"), ObjectId("stage2")],
  stageKeys: [
    { id: "group-stage", scope: "fifa" },
    { id: "knockout-stage", scope: "fifa" }
  ]
}
```

### Collecting Venue Keys After Flattening

```javascript
// From your main $group stage
{
  $group: {
    _id: null,
    sgos: { $first: '$sgos' },
    stages: { $first: '$stages' },
    events: { $first: '$events' },
    
    // Collect unique venue key arrays (nested arrays get flattened)
    venueKeys: { $addToSet: '$_venueKeys' },
    
    // Collect unique team key objects
    teamKeys: {
      $addToSet: {
        id: '$_participants._externalTeamId',
        scope: '$_participants._externalTeamIdScope',
      },
    },
    
    // Collect unique sports person key objects  
    spKeys: {
      $addToSet: {
        id: '$_participants._externalSportsPersonId',
        scope: '$_participants._externalSportsPersonIdScope',
      },
    },
  }
}

// What this does after $unwind operations:
// - venueKeys: Collects arrays of venue keys, each array becomes a unique element
// - teamKeys: Creates unique {id, scope} objects for teams from participant data
// - spKeys: Creates unique {id, scope} objects for sports persons from participant data
// - Automatically handles null values in id/scope fields
```

## Comparison with `$push`

### `$push` vs `$addToSet`

```javascript
// Input documents
[
  { category: "books", author: "Smith" },
  { category: "books", author: "Jones" },
  { category: "books", author: "Smith" }    // duplicate
]

// Using $push - includes duplicates
{
  $group: {
    _id: "$category",
    allAuthors: { $push: "$author" }        // Keeps duplicates
  }
}
// Result: { _id: "books", allAuthors: ["Smith", "Jones", "Smith"] }

// Using $addToSet - removes duplicates  
{
  $group: {
    _id: "$category", 
    uniqueAuthors: { $addToSet: "$author" } // Removes duplicates
  }
}
// Result: { _id: "books", uniqueAuthors: ["Smith", "Jones"] }
```

## Advanced Usage Patterns

### Conditional Addition

```javascript
{
  $group: {
    _id: "$category",
    premiumFeatures: { 
      $addToSet: {
        $cond: [
          { $eq: ["$tier", "premium"] },    // Condition
          "$feature",                       // Value if true
          "$$REMOVE"                        // Remove if false (don't add)
        ]
      }
    }
  }
}
```

### Expression-Based Values

```javascript
{
  $group: {
    _id: "$userId",
    yearMonths: { 
      $addToSet: {
        $dateToString: {
          format: "%Y-%m",                  // Create YYYY-MM strings
          date: "$orderDate"
        }
      }
    }
  }
}
```

### Nested Field Extraction

```javascript
{  
  $group: {
    _id: "$customerId",
    productCategories: { $addToSet: "$items.category" },     // From nested object
    shippingStates: { $addToSet: "$shipping.address.state" } // From deeply nested
  }
}
```

### Array Element Collection (after $unwind)

```javascript
// First unwind an array field, then collect unique elements
[
  { $unwind: "$tags" },                   // Split array elements into documents
  {
    $group: {
      _id: "$category",
      allUniqueTags: { $addToSet: "$tags" } // Collect unique values across all documents
    }
  }
]
```

## Handling Edge Cases

### Null and Missing Values

```javascript
// Input documents
[
  { _id: 1, category: "books", rating: 5 },
  { _id: 2, category: "books", rating: null },    // explicit null
  { _id: 3, category: "books" },                  // missing rating field
  { _id: 4, category: "books", rating: 5 }        // duplicate
]

{
  $group: {
    _id: "$category",
    uniqueRatings: { $addToSet: "$rating" }
  }
}

// Result: { _id: "books", uniqueRatings: [5, null] }
// Note: 
// - null is included once as a distinct value
// - missing field is ignored (not added as null)
// - duplicate 5 is removed
```

### Empty Arrays and Complex Types

```javascript
// Input documents
[
  { _id: 1, tags: ["red", "blue"] },
  { _id: 2, tags: [] },                   // empty array
  { _id: 3, tags: ["red", "blue"] },      // duplicate array
  { _id: 4, tags: ["green"] }
]

{
  $group: {
    _id: null,
    uniqueTagArrays: { $addToSet: "$tags" }
  }
}

// Result: 
{ 
  _id: null, 
  uniqueTagArrays: [
    ["red", "blue"],    // Appears once despite duplicate
    [],                 // Empty array is included
    ["green"]
  ] 
}
```

## Performance Considerations

### Memory Usage

```javascript
// $addToSet holds unique values in memory for each group
// Memory usage = (number of groups) × (average unique values per group) × (average value size)

// For large datasets, monitor memory usage:
db.collection.aggregate([
  // ... pipeline with $addToSet
], { 
  allowDiskUse: true,           // Allow spilling to disk
  explain: "executionStats"     // Monitor memory usage
})
```

### Optimization Strategies

```javascript
// GOOD: Filter before grouping to reduce working set
[
  { $match: { active: true, date: { $gte: recentDate } } },  // Reduce documents first
  {
    $group: {
      _id: "$userId", 
      uniqueCategories: { $addToSet: "$category" }
    }
  }
]

// LESS EFFICIENT: Group all then filter
[
  {
    $group: {
      _id: "$userId",
      uniqueCategories: { $addToSet: "$category" }
    }
  },
  { $match: { "uniqueCategories.2": { $exists: true } } }    // Filter after expensive grouping
]
```

### Index Strategy

```javascript
// Create indexes on grouping fields
db.collection.createIndex({ "userId": 1, "category": 1 })

// For your competition pipeline, ensure indexes exist for:
db.stages.createIndex({ "_externalCompetitionId": 1, "_externalCompetitionIdScope": 1 })
db.events.createIndex({ "_externalStageId": 1, "_externalStageIdScope": 1 })
```

## Real-World Use Cases

### 1. User Activity Tracking

```javascript
// Collect unique pages visited by each user
{
  $group: {
    _id: "$userId",
    uniquePagesVisited: { $addToSet: "$pageUrl" },
    uniqueDevices: { $addToSet: "$deviceType" }
  }
}
```

### 2. Product Categorization

```javascript
// Find all unique categories and brands per product type
{
  $group: {
    _id: "$productType",
    availableCategories: { $addToSet: "$category" },
    availableBrands: { $addToSet: "$brand" }
  }
}
```

### 3. Geographic Analysis

```javascript
// Collect unique locations where events occurred
{
  $group: {
    _id: "$eventType",
    uniqueCountries: { $addToSet: "$location.country" },
    uniqueCities: { $addToSet: { 
      city: "$location.city", 
      country: "$location.country" 
    }}
  }
}
```

### 4. Relationship Mapping (Your Use Case)

```javascript
// From your pipeline - collecting related entity keys
{
  $group: {
    _id: null,
    // Collect unique external reference keys for later lookups
    teamKeys: { 
      $addToSet: { 
        id: '$_participants._externalTeamId',
        scope: '$_participants._externalTeamIdScope'
      } 
    },
    venueKeys: { $addToSet: '$_venueKeys' }
  }
}
```

## Common Pitfalls and Solutions

### 1. Order Dependency

```javascript
// WRONG: Expecting specific order
{
  $group: {
    _id: "$category",
    tags: { $addToSet: "$tag" }
  }
}
// Result order is unpredictable

// CORRECT: Sort after if order matters
[
  {
    $group: {
      _id: "$category", 
      tags: { $addToSet: "$tag" }
    }
  },
  {
    $addFields: {
      tags: { $sortArray: { input: "$tags", sortBy: 1 } }  // MongoDB 5.2+
    }
  }
]
```

### 2. Object Comparison Understanding

```javascript
// Objects are compared by their field values and structure
{ $addToSet: { name: "$name", age: "$age" } }

// These are considered DIFFERENT:
{ name: "John", age: 25 }
{ age: 25, name: "John" }  // Different field order = different object

// These are considered SAME:
{ name: "John", age: 25 }
{ name: "John", age: 25 }  // Exact match = duplicate removed
```

### 3. Large Array Handling

```javascript
// Be careful with large unique value sets
{
  $group: {
    _id: "$userId",
    allUniqueClicks: { $addToSet: "$clickData" }  // Could become very large
  }
}

// Consider limiting or sampling:
{
  $group: {
    _id: "$userId", 
    recentUniqueClicks: { 
      $addToSet: {
        $cond: [
          { $gte: ["$clickDate", recentThreshold] },
          "$clickData",
          "$$REMOVE"
        ]
      }
    }
  }
}
```

## When to Use `$addToSet`

### Good Use Cases

- **Collecting unique references**: Like ObjectIds, external keys, categories
- **Deduplicating lists**: When you need distinct values from grouped data
- **Building lookup keys**: Creating arrays of unique identifiers for subsequent lookups
- **Tag/category aggregation**: Collecting unique tags, categories, or labels
- **Relationship mapping**: Finding unique related entities (as in your competition pipeline)

### When NOT to Use `$addToSet`

- **When duplicates matter**: Use `$push` if you need to preserve all values
- **When order matters**: `$addToSet` doesn't guarantee order
- **With very large unique sets**: May cause memory issues
- **Simple existence checks**: Use `$group` with boolean logic instead

## Pre-Use Checklist

Before using `$addToSet`, consider:

- ✅ Do you need unique values, or are duplicates meaningful?
- ✅ Will the unique value count per group be manageable in memory?
- ✅ Are you grouping by indexed fields for performance?
- ✅ Do you need the results in a specific order? (Plan for post-sorting)
- ✅ Are you collecting objects? Ensure field order consistency
- ✅ Have you filtered input data to reduce processing load?
- ✅ Is the value expression correctly handling null/missing fields?

---

*`$addToSet` is essential for deduplication during grouping operations. Your competition-full.js pipeline demonstrates sophisticated usage for collecting unique entity references across complex hierarchical relationships, enabling efficient subsequent lookups while maintaining data integrity.*
