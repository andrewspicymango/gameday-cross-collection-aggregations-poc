# MongoDB `$unwind` Aggregation Stage - Complete Guide

## High-Level Summary

- `$unwind` deconstructs an array field from input documents to output one document for each array element
- If a document has an array with 3 elements, `$unwind` produces 3 output documents (one per element)
- The array field is replaced with the individual element value in each output document
- Use `$unwind` when you need to process array elements individually or join array elements with other collections

## Core Behavior

### Basic Syntax

```javascript
{ $unwind: "$arrayFieldName" }
```

### Advanced Syntax with Options

```javascript
{ 
  $unwind: {
    path: "$arrayFieldName",
    includeArrayIndex: "indexFieldName",
    preserveNullAndEmptyArrays: true
  }
}
```

### Key Characteristics

- **Input**: Documents with array fields
- **Output**: Multiple documents (one per array element)
- **Array replacement**: The array field becomes a scalar value in each output document
- **Document multiplication**: 1 input document with N array elements → N output documents
- **Memory efficient**: Processes one document at a time (no accumulation like `$group`)

## Basic Example

### Input Document

```javascript
{
  _id: 1,
  name: "John",
  hobbies: ["reading", "swimming", "coding"]
}
```

### Pipeline

```javascript
db.users.aggregate([
  { $unwind: "$hobbies" }
])
```

### Output

```javascript
[
  { _id: 1, name: "John", hobbies: "reading" },
  { _id: 1, name: "John", hobbies: "swimming" },
  { _id: 1, name: "John", hobbies: "coding" }
]
```

## Advanced Options

### `includeArrayIndex` - Track Original Position

```javascript
db.users.aggregate([
  { 
    $unwind: {
      path: "$hobbies",
      includeArrayIndex: "hobbyIndex"
    }
  }
])
```

**Output:**

```javascript
[
  { _id: 1, name: "John", hobbies: "reading", hobbyIndex: 0 },
  { _id: 1, name: "John", hobbies: "swimming", hobbyIndex: 1 },
  { _id: 1, name: "John", hobbies: "coding", hobbyIndex: 2 }
]
```

### `preserveNullAndEmptyArrays` - Handle Missing/Empty Arrays

```javascript
// Input documents
[
  { _id: 1, name: "John", hobbies: ["reading"] },
  { _id: 2, name: "Jane", hobbies: [] },
  { _id: 3, name: "Bob" }  // no hobbies field
]

// Without preserveNullAndEmptyArrays (default behavior)
db.users.aggregate([
  { $unwind: "$hobbies" }
])
// Output: Only John's document (Jane and Bob are dropped)
[
  { _id: 1, name: "John", hobbies: "reading" }
]

// With preserveNullAndEmptyArrays: true
db.users.aggregate([
  { 
    $unwind: {
      path: "$hobbies",
      preserveNullAndEmptyArrays: true
    }
  }
])
// Output: All documents preserved
[
  { _id: 1, name: "John", hobbies: "reading" },
  { _id: 2, name: "Jane", hobbies: null },
  { _id: 3, name: "Bob", hobbies: null }
]
```

## Common Usage Patterns

### 1. Array Element Processing

```javascript
// Process each order item individually
db.orders.aggregate([
  { $unwind: "$items" },
  { $group: {
      _id: "$items.productId",
      totalQuantity: { $sum: "$items.quantity" },
      totalRevenue: { $sum: { $multiply: ["$items.price", "$items.quantity"] }}
    }
  }
])
```

### 2. Array-to-Document Joins

```javascript
// Join each tag with tag details
db.posts.aggregate([
  { $unwind: "$tags" },
  { $lookup: {
      from: "tagDetails",
      localField: "tags",
      foreignField: "tagName",
      as: "tagInfo"
    }
  }
])
```

### 3. Nested Array Unwinding

```javascript
// Unwind multiple levels of arrays
db.users.aggregate([
  { $unwind: "$orders" },           // First level: orders array
  { $unwind: "$orders.items" }      // Second level: items array within each order
])
```

### 4. Filtering After Unwind

```javascript
// Find all users who have "coding" as a hobby
db.users.aggregate([
  { $unwind: "$hobbies" },
  { $match: { hobbies: "coding" } },
  { $group: { _id: "$_id", name: { $first: "$name" } } }
])
```

## Performance Considerations

### Memory Usage

- **Low memory footprint**: Processes documents one at a time
- **No accumulation**: Unlike `$group`, doesn't need to hold all data in memory
- **Streaming friendly**: Can process large datasets efficiently

### Document Count Impact

- **Multiplication effect**: Output document count = sum of all array lengths
- **Index considerations**: Indexes on unwound fields may need rebuilding
- **Pipeline placement**: Put filtering stages (`$match`) after `$unwind` when possible

### Optimization Tips

```javascript
// GOOD: Filter before unwind to reduce processing
db.orders.aggregate([
  { $match: { status: "active" } },     // Reduce input documents first
  { $unwind: "$items" },
  { $match: { "items.category": "electronics" } }  // Then filter array elements
])

// LESS EFFICIENT: Unwind all documents then filter
db.orders.aggregate([
  { $unwind: "$items" },
  { $match: { 
      status: "active",
      "items.category": "electronics" 
    }
  }
])
```

## Edge Cases and Gotchas

### Empty Arrays

```javascript
// Input
{ _id: 1, tags: [] }

// Default behavior: document is dropped
{ $unwind: "$tags" }  // No output

// Preserve empty arrays
{ $unwind: { path: "$tags", preserveNullAndEmptyArrays: true } }
// Output: { _id: 1, tags: null }
```

### Non-Array Fields

```javascript
// Input
{ _id: 1, category: "electronics" }  // category is not an array

// Behavior: treats as single-element array
{ $unwind: "$category" }  
// Output: { _id: 1, category: "electronics" }  // Same document
```

### Missing Fields

```javascript
// Input
{ _id: 1, name: "John" }  // no tags field

// Default: document is dropped
{ $unwind: "$tags" }  // No output

// With preserveNullAndEmptyArrays
{ $unwind: { path: "$tags", preserveNullAndEmptyArrays: true } }
// Output: { _id: 1, name: "John", tags: null }
```

### Null Values in Arrays

```javascript
// Input
{ _id: 1, tags: ["red", null, "blue"] }

// Output: null is preserved as an element
[
  { _id: 1, tags: "red" },
  { _id: 1, tags: null },
  { _id: 1, tags: "blue" }
]
```

## When to Use `$unwind`

### Good Use Cases

- **Array element analysis**: Calculating statistics per array element
- **Array-based joins**: Joining each array element with another collection
- **Filtering array elements**: Finding documents with specific array values
- **Flattening nested structures**: Converting arrays to individual records
- **Grouping by array elements**: Aggregating data grouped by array values

### When NOT to Use `$unwind`

- **Simple array operations**: Use `$addFields` with array operators instead
- **Array length checks**: Use `$size` operator
- **Array filtering without processing elements**: Use `$filter` operator
- **Large arrays with memory constraints**: Consider alternative approaches or pagination

## Real-World Example: E-commerce Order Analysis

```javascript
// Find top-selling products across all orders
db.orders.aggregate([
  // Stage 1: Filter recent orders
  { $match: { 
      orderDate: { $gte: new Date("2024-01-01") },
      status: "completed"
    }
  },
  
  // Stage 2: Unwind order items
  { $unwind: "$items" },
  
  // Stage 3: Group by product
  { $group: {
      _id: "$items.productId",
      productName: { $first: "$items.name" },
      totalQuantitySold: { $sum: "$items.quantity" },
      totalRevenue: { $sum: { $multiply: ["$items.price", "$items.quantity"] } },
      orderCount: { $sum: 1 }
    }
  },
  
  // Stage 4: Sort by revenue
  { $sort: { totalRevenue: -1 } },
  
  // Stage 5: Limit to top 10
  { $limit: 10 }
])
```

## Pre-Use Checklist

Before using `$unwind`, consider:

- ✅ Do you need to process array elements individually?
- ✅ Will the document multiplication (array length × document count) be manageable?
- ✅ Should empty/null arrays be preserved or dropped?
- ✅ Can you filter documents before unwinding to reduce processing?
- ✅ Do you need to track original array positions?
- ✅ Are there alternative array operators that could work instead?

---

*`$unwind` is one of the most powerful and commonly used aggregation stages, particularly useful for transforming document-oriented data into relational-style processing patterns.*
