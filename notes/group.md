# MongoDB `$group` Aggregation Stage - Complete Guide

## High-Level Summary

- `$group` groups documents by specified identifier expressions and applies accumulator expressions to each group
- It's similar to SQL's GROUP BY clause but with more powerful aggregation operators
- Groups can be formed by single fields, multiple fields, expressions, or even null (to aggregate all documents)
- Each group produces exactly one output document containing the group key and computed aggregation values
- Use `$group` when you need to calculate statistics, counts, sums, or other aggregate values across sets of documents

## Core Behavior

### Basic Syntax

```javascript
{
  $group: {
    _id: <group key expression>,           // Required: what to group by
    <field1>: { <accumulator1>: <expression1> },  // Computed fields
    <field2>: { <accumulator2>: <expression2> },
    ...
  }
}
```

### Key Characteristics

- **Required `_id` field**: Defines the grouping criteria (can be null, field, or complex expression)
- **Accumulator operators**: Perform calculations across documents in each group
- **Memory usage**: Holds all groups in memory simultaneously (can be memory-intensive)
- **Document transformation**: Input document count ≠ output document count
- **Order**: Output order is not guaranteed unless followed by `$sort`

## Basic Examples

### Group by Single Field

```javascript
// Input documents - our sample product data
[
  { _id: 1, category: "electronics", price: 100 },
  { _id: 2, category: "electronics", price: 200 },
  { _id: 3, category: "books", price: 15 },
  { _id: 4, category: "books", price: 25 }
]

// MongoDB $group aggregation pipeline
db.products.aggregate([
  {
    $group: {
      // _id: "$category" - THE GROUPING KEY (REQUIRED)
      // This is the most important part - it defines HOW documents are grouped
      // "$category" means "use the value of the 'category' field as the group key"
      // Documents with the same category value will be grouped together
      // 
      // What happens:
      // - Document 1 has category "electronics" → goes to "electronics" group
      // - Document 2 has category "electronics" → goes to "electronics" group  
      // - Document 3 has category "books" → goes to "books" group
      // - Document 4 has category "books" → goes to "books" group
      // 
      // Result: 2 groups are created:
      // Group 1: _id="electronics", contains docs 1 & 2
      // Group 2: _id="books", contains docs 3 & 4
      _id: "$category",
      
      // totalPrice: { $sum: "$price" } - ACCUMULATOR EXPRESSION
      // This creates a new field called "totalPrice" in the output
      // $sum is an accumulator operator that adds up values across all documents in each group
      // "$price" tells it to sum the "price" field from each document
      //
      // What happens for each group:
      // Electronics group: $sum adds 100 + 200 = 300
      // Books group: $sum adds 15 + 25 = 40
      //
      // Note: If a document had no "price" field or price was null, it would be ignored
      totalPrice: { $sum: "$price" },
      
      // count: { $sum: 1 } - DOCUMENT COUNTER
      // This creates a field called "count" 
      // $sum: 1 means "add 1 for each document in the group"
      // This is the standard way to count documents in MongoDB aggregation
      // The "1" is a literal value, not a field reference
      //
      // What happens for each group:
      // Electronics group: 1 + 1 = 2 documents
      // Books group: 1 + 1 = 2 documents
      //
      // Alternative ways to write this:
      // count: { $count: {} }  // MongoDB 5.0+, but $sum: 1 is more widely supported
      count: { $sum: 1 },
      
      // avgPrice: { $avg: "$price" } - AVERAGE CALCULATOR
      // This creates a field called "avgPrice"
      // $avg calculates the arithmetic mean of the specified field across all documents in each group
      // "$price" tells it to average the "price" field values
      //
      // What happens for each group:
      // Electronics group: (100 + 200) / 2 = 150
      // Books group: (15 + 25) / 2 = 20
      //
      // Note: $avg automatically ignores null values and missing fields
      // If all values in a group are null/missing, $avg returns null
      avgPrice: { $avg: "$price" }
    }
  }
])

// STEP-BY-STEP PROCESSING:
//
// Step 1: MongoDB scans all input documents
// Step 2: For each document, it evaluates the _id expression ("$category")
// Step 3: Documents are placed into groups based on the _id value:
//
//   Group "_id: electronics":
//   - { _id: 1, category: "electronics", price: 100 }
//   - { _id: 2, category: "electronics", price: 200 }
//
//   Group "_id: books": 
//   - { _id: 3, category: "books", price: 15 }
//   - { _id: 4, category: "books", price: 25 }
//
// Step 4: For each group, MongoDB applies the accumulator expressions:
//
//   For electronics group:
//   - totalPrice: $sum([100, 200]) = 300
//   - count: $sum([1, 1]) = 2  
//   - avgPrice: $avg([100, 200]) = 150
//
//   For books group:
//   - totalPrice: $sum([15, 25]) = 40
//   - count: $sum([1, 1]) = 2
//   - avgPrice: $avg([15, 25]) = 20
//
// Step 5: MongoDB creates one output document per group

// Final Output - exactly 2 documents (one per group)
[
  { 
    _id: "electronics",    // The grouping key value
    totalPrice: 300,       // Sum of prices: 100 + 200
    count: 2,              // Number of documents: 2
    avgPrice: 150          // Average price: (100 + 200) / 2
  },
  { 
    _id: "books",          // The grouping key value  
    totalPrice: 40,        // Sum of prices: 15 + 25
    count: 2,              // Number of documents: 2
    avgPrice: 20           // Average price: (15 + 25) / 2
  }
]

// IMPORTANT NOTES:
//
// 1. INPUT vs OUTPUT document count:
//    - Input: 4 documents
//    - Output: 2 documents (one per unique category)
//    - $group always reduces the document count (unless every document has a unique _id)
//
// 2. Field name mapping:
//    - Input documents have fields: _id, category, price
//    - Output documents have fields: _id, totalPrice, count, avgPrice
//    - The original _id field is lost (replaced by the grouping key)
//    - The category field is now the _id field
//    - New computed fields (totalPrice, count, avgPrice) are added
//
// 3. Order is not guaranteed:
//    - The output could be in any order unless you add { $sort: { _id: 1 } }
//
// 4. Memory usage:
//    - MongoDB holds all groups in memory during processing
//    - With 2 groups this is trivial, but with millions of groups it could be problematic
//
// 5. Null handling:
//    - If any document had category: null or missing category field, 
//      it would create a group with _id: null
//    - Null prices would be ignored in $sum and $avg calculations
```

### Group All Documents (Global Aggregation)

```javascript
db.products.aggregate([
  {
    $group: {
      _id: null,  // Groups all documents together
      totalRevenue: { $sum: "$price" },
      productCount: { $sum: 1 },
      categories: { $addToSet: "$category" }
    }
  }
])

// Output
[
  { 
    _id: null, 
    totalRevenue: 340, 
    productCount: 4, 
    categories: ["electronics", "books"] 
  }
]
```

### Group by Multiple Fields

```javascript
db.sales.aggregate([
  {
    $group: {
      _id: { 
        category: "$category",
        region: "$region" 
      },
      totalSales: { $sum: "$amount" },
      transactionCount: { $sum: 1 }
    }
  }
])

// Output
[
  { _id: { category: "electronics", region: "north" }, totalSales: 500, transactionCount: 3 },
  { _id: { category: "electronics", region: "south" }, totalSales: 300, transactionCount: 2 }
]
```

## Accumulator Operators

### Arithmetic Accumulators

```javascript
{
  $group: {
    _id: "$category",
    total: { $sum: "$price" },           // Sum of values
    count: { $sum: 1 },                  // Count documents
    average: { $avg: "$price" },         // Average value
    minimum: { $min: "$price" },         // Minimum value
    maximum: { $max: "$price" }          // Maximum value
  }
}
```

### Array Accumulators

```javascript
{
  $group: {
    _id: "$category",
    allPrices: { $push: "$price" },      // Array of all values (with duplicates)
    uniquePrices: { $addToSet: "$price" }, // Array of unique values
    firstProduct: { $first: "$name" },    // First document's value (requires sort)
    lastProduct: { $last: "$name" },      // Last document's value (requires sort)
    topPrice: { $top: { output: "$price", sortBy: { price: -1 } } } // MongoDB 5.2+
  }
}
```

### Conditional Accumulators

```javascript
{
  $group: {
    _id: "$category",
    expensiveItems: {
      $sum: { 
        $cond: [
          { $gt: ["$price", 100] },  // Condition
          1,                         // Value if true
          0                          // Value if false
        ]
      }
    },
    premiumRevenue: {
      $sum: {
        $cond: [
          { $gt: ["$price", 100] },
          "$price",
          0
        ]
      }
    }
  }
}
```

## Real-World Examples from Your Code

### Complex Grouping with Set Operations

```javascript
// From your competition-full.js pipeline
{
  $group: {
    _id: null,
    sgos: { $first: '$sgos' },
    stages: { $first: '$stages' },
    events: { $first: '$events' },
    venueKeys: { $addToSet: '$_venueKeys' },        // Collect unique venue keys
    teamKeys: {
      $addToSet: {                                   // Collect unique team external keys
        id: '$_participants._externalTeamId',
        scope: '$_participants._externalTeamIdScope',
      },
    },
    spKeys: {
      $addToSet: {                                   // Collect unique sports person keys
        id: '$_participants._externalSportsPersonId',
        scope: '$_participants._externalSportsPersonIdScope',
      },
    },
  }
}
```

### SGO Membership Grouping

```javascript
// From your competition-full.js SGO facet
{
  $group: { 
    _id: null, 
    ids: { $addToSet: '$sgo._id' }  // Collect unique SGO IDs
  }
}
```

### Stage Keys Collection

```javascript
// From your competition-full.js stages facet
{
  $group: { 
    _id: null, 
    ids: { $addToSet: '$stages._id' },                    // Stage IDs
    stageKeys: { 
      $addToSet: { 
        id: '$stages._externalId', 
        scope: '$stages._externalIdScope' 
      } 
    }
  }
}
```

## Advanced Grouping Patterns

### Date-Based Grouping

```javascript
db.orders.aggregate([
  {
    $group: {
      _id: {
        year: { $year: "$orderDate" },
        month: { $month: "$orderDate" }
      },
      monthlyRevenue: { $sum: "$total" },
      orderCount: { $sum: 1 },
      avgOrderValue: { $avg: "$total" }
    }
  }
])
```

### Expression-Based Grouping

```javascript
db.products.aggregate([
  {
    $group: {
      _id: {
        $switch: {
          branches: [
            { case: { $lt: ["$price", 50] }, then: "budget" },
            { case: { $lt: ["$price", 200] }, then: "mid-range" },
            { case: { $gte: ["$price", 200] }, then: "premium" }
          ],
          default: "unknown"
        }
      },
      count: { $sum: 1 },
      avgPrice: { $avg: "$price" }
    }
  }
])
```

### Nested Field Grouping

```javascript
db.users.aggregate([
  {
    $group: {
      _id: "$address.city",           // Group by nested field
      userCount: { $sum: 1 },
      avgAge: { $avg: "$age" },
      zipcodes: { $addToSet: "$address.zipcode" }
    }
  }
])
```

### Array Element Grouping (after $unwind)

```javascript
db.orders.aggregate([
  { $unwind: "$items" },              // First unwind the array
  {
    $group: {
      _id: "$items.productId",        // Group by array element field
      totalQuantity: { $sum: "$items.quantity" },
      totalRevenue: { $sum: { $multiply: ["$items.price", "$items.quantity"] } },
      orderCount: { $sum: 1 }
    }
  }
])
```

## Performance Considerations

### Memory Usage

```javascript
// $group holds all groups in memory - can be memory intensive
// Use allowDiskUse for large datasets
db.collection.aggregate([
  // ... pipeline stages
  { $group: { /* ... */ } }
], { allowDiskUse: true })
```

### Index Optimization

```javascript
// Create indexes on grouping fields
db.sales.createIndex({ "category": 1, "region": 1 })

// For date-based grouping
db.orders.createIndex({ "orderDate": 1 })

// Pipeline: filter first, then group
[
  { $match: { orderDate: { $gte: new Date("2024-01-01") } } },  // Filter first
  { $group: { _id: "$customerId", total: { $sum: "$amount" } } } // Then group
]
```

### Optimization Patterns

```javascript
// GOOD: Reduce document count before grouping
[
  { $match: { status: "active" } },           // Filter early
  { $project: { category: 1, price: 1 } },   // Project only needed fields
  { $group: { _id: "$category", avg: { $avg: "$price" } } }
]

// LESS EFFICIENT: Group all documents then filter
[
  { $group: { _id: "$category", avg: { $avg: "$price" } } },
  { $match: { avg: { $gt: 100 } } }          // Filter after expensive grouping
]
```

## Common Patterns and Use Cases

### 1. Top N per Group

```javascript
// Get top 3 products by sales in each category
db.products.aggregate([
  { $sort: { category: 1, sales: -1 } },    // Sort first
  {
    $group: {
      _id: "$category",
      topProducts: { 
        $push: { 
          name: "$name", 
          sales: "$sales" 
        } 
      }
    }
  },
  {
    $project: {
      _id: 1,
      topProducts: { $slice: ["$topProducts", 3] }  // Take top 3
    }
  }
])
```

### 2. Running Totals and Percentages

```javascript
db.sales.aggregate([
  {
    $group: {
      _id: "$region",
      totalSales: { $sum: "$amount" }
    }
  },
  {
    $group: {
      _id: null,
      regions: { 
        $push: { 
          region: "$_id", 
          sales: "$totalSales" 
        } 
      },
      grandTotal: { $sum: "$totalSales" }
    }
  },
  { $unwind: "$regions" },
  {
    $project: {
      region: "$regions.region",
      sales: "$regions.sales",
      percentage: { 
        $multiply: [
          { $divide: ["$regions.sales", "$grandTotal"] }, 
          100
        ] 
      }
    }
  }
])
```

### 3. Pivot Table Style Aggregation

```javascript
db.sales.aggregate([
  {
    $group: {
      _id: "$product",
      Q1: { $sum: { $cond: [{ $in: [{ $month: "$date" }, [1,2,3]] }, "$amount", 0] } },
      Q2: { $sum: { $cond: [{ $in: [{ $month: "$date" }, [4,5,6]] }, "$amount", 0] } },
      Q3: { $sum: { $cond: [{ $in: [{ $month: "$date" }, [7,8,9]] }, "$amount", 0] } },
      Q4: { $sum: { $cond: [{ $in: [{ $month: "$date" }, [10,11,12]] }, "$amount", 0] } }
    }
  }
])
```

### 4. Statistical Analysis

```javascript
db.scores.aggregate([
  {
    $group: {
      _id: "$subject",
      count: { $sum: 1 },
      mean: { $avg: "$score" },
      min: { $min: "$score" },
      max: { $max: "$score" },
      stdDev: { $stdDevPop: "$score" },
      scores: { $push: "$score" }  // For median calculation
    }
  },
  {
    $addFields: {
      median: {
        $arrayElemAt: [
          "$scores",
          { $floor: { $divide: [{ $size: "$scores" }, 2] } }
        ]
      }
    }
  }
])
```

### 5. Hierarchical Grouping (Multiple Levels)

```javascript
// Group by department, then by team within department
db.employees.aggregate([
  {
    $group: {
      _id: {
        department: "$department",
        team: "$team"
      },
      teamSize: { $sum: 1 },
      avgSalary: { $avg: "$salary" }
    }
  },
  {
    $group: {
      _id: "$_id.department",
      teams: {
        $push: {
          team: "$_id.team",
          size: "$teamSize",
          avgSalary: "$avgSalary"
        }
      },
      totalEmployees: { $sum: "$teamSize" },
      deptAvgSalary: { $avg: "$avgSalary" }
    }
  }
])
```

## Edge Cases and Gotchas

### Null and Missing Values

```javascript
// Input documents
[
  { _id: 1, category: "electronics", price: 100 },
  { _id: 2, category: null, price: 200 },
  { _id: 3, price: 150 },  // missing category
  { _id: 4, category: "books", price: null }
]

{
  $group: {
    _id: "$category",  // null and missing both group to _id: null
    avgPrice: { $avg: "$price" },  // null values are ignored in $avg
    count: { $sum: 1 }
  }
}

// Output
[
  { _id: "electronics", avgPrice: 100, count: 1 },
  { _id: "books", avgPrice: null, count: 1 },      // No valid prices
  { _id: null, avgPrice: 175, count: 2 }           // Both null and missing category
]
```

### Empty Groups

```javascript
// If no documents match after $match, $group produces no output
db.products.aggregate([
  { $match: { category: "nonexistent" } },
  { $group: { _id: "$category", count: { $sum: 1 } } }
])
// Result: [] (empty array)
```

### Order Dependency with $first/$last

```javascript
// $first and $last depend on document order
// WRONG: Random order
{
  $group: {
    _id: "$category",
    firstProduct: { $first: "$name" }  // Unpredictable result
  }
}

// CORRECT: Sort first
[
  { $sort: { createdAt: 1 } },         // Establish order
  {
    $group: {
      _id: "$category", 
      firstProduct: { $first: "$name" } // Now predictable
    }
  }
]
```

### Large Group Counts

```javascript
// Too many groups can cause memory issues
// Monitor with explain()
db.collection.aggregate([
  { $group: { _id: "$userId", count: { $sum: 1 } } }  // Could create millions of groups
]).explain("executionStats")

// Alternative: Use $bucket or $bucketAuto for large cardinality
{
  $bucket: {
    groupBy: "$userId",
    boundaries: [0, 1000, 5000, 10000, Infinity],
    default: "Other",
    output: { count: { $sum: 1 } }
  }
}
```

## When to Use `$group`

### Good Use Cases

- **Aggregating metrics**: Sums, counts, averages across categories
- **Statistical analysis**: Min/max, standard deviation, percentiles
- **Data summarization**: Rolling up detail records to summary records
- **Pivot operations**: Transforming rows to columns conceptually
- **Collecting related values**: Using $push/$addToSet to gather arrays
- **Top-N analysis**: Combined with $sort for rankings

### When NOT to Use `$group`

- **Simple filtering**: Use $match instead
- **Document transformation**: Use $project/$addFields for individual document changes
- **Large cardinality grouping**: Consider $bucket/$bucketAuto or pre-aggregated collections
- **Real-time analytics**: Consider pre-computed aggregations for frequently accessed metrics

## Memory and Performance Tips

### Memory Management

```javascript
// For large datasets, enable disk usage
{ allowDiskUse: true }

// Reduce memory by projecting only needed fields before grouping
[
  { $project: { category: 1, price: 1, date: 1 } },  // Remove unnecessary fields
  { $group: { _id: "$category", avg: { $avg: "$price" } } }
]
```

### Index Strategy

```javascript
// Create compound indexes for multi-field grouping
db.sales.createIndex({ category: 1, region: 1, date: 1 })

// Use covered queries when possible
[
  { $match: { category: "electronics" } },           // Uses index
  { $group: { _id: "$region", total: { $sum: "$amount" } } }
]
```

### Pipeline Optimization

```javascript
// Optimal order: Match → Sort → Group → Project
[
  { $match: { /* filter conditions */ } },    // Reduce documents early
  { $sort: { /* if needed for $first/$last */ } },
  { $group: { /* grouping logic */ } },
  { $project: { /* shape output */ } }
]
```

## Pre-Use Checklist

Before using `$group`, consider:

- ✅ Have you filtered documents with $match to reduce the working set?
- ✅ Are you grouping by indexed fields when possible?
- ✅ Will the number of groups fit comfortably in memory?
- ✅ Do you need $sort before $group for $first/$last operations?
- ✅ Are you using the most efficient accumulator operators?
- ✅ Have you projected only necessary fields before grouping?
- ✅ Would $bucket/$bucketAuto be more appropriate for high cardinality grouping?
- ✅ Do you need allowDiskUse: true for large datasets?

---

*`$group` is one of the most powerful and commonly used aggregation stages, essential for data analysis and reporting. Your competition-full.js pipeline demonstrates sophisticated usage with $addToSet for collecting unique keys and $first for preserving single values across group operations.*
