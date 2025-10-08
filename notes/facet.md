# MongoDB `$facet` Aggregation Stage - Complete Guide

## High-Level Summary

- `$facet` runs multiple sub-pipelines in parallel against the same input documents that reach the stage
- It returns a single document where each facet key maps to an array containing that facet's result array
- Use `$facet` when you need multiple different aggregations (counts, top-N, stats, samples) from the same filtered set in one pass
- All facets operate on identical input data, making it efficient for multi-dimensional analysis

## Core Behavior

### Basic Syntax

```javascript
{
  $facet: {
    <facet1Name>: [ <stage1>, <stage2>, ... ],
    <facet2Name>: [ <stageA>, <stageB>, ... ],
    <facet3Name>: [ <stageX>, <stageY>, ... ]
  }
}
```

### Key Characteristics

- **Parallel processing**: All facet pipelines execute simultaneously on the same input documents
- **Single output document**: Always produces exactly one document containing all facet results
- **Array results**: Each facet name maps to an array of documents (even if empty)
- **Memory constraints**: Final document must fit within BSON 16MB limit
- **Input sharing**: All facets see identical input documents
- **Independence**: Facets cannot reference each other's results

## Basic Examples

### Simple Multi-Dimensional Analysis

```javascript
// Input documents
[
  { _id: 1, category: "electronics", price: 100, status: "active" },
  { _id: 2, category: "electronics", price: 200, status: "active" },
  { _id: 3, category: "books", price: 15, status: "active" },
  { _id: 4, category: "books", price: 25, status: "inactive" }
]

// Pipeline
db.products.aggregate([
  { $match: { status: "active" } },           // Pre-filter input
  {
    $facet: {
      // Facet 1: Get counts by category
      categoryStats: [
        {
          $group: {
            _id: "$category",
            count: { $sum: 1 },
            avgPrice: { $avg: "$price" }
          }
        }
      ],
      
      // Facet 2: Get overall statistics
      overallStats: [
        {
          $group: {
            _id: null,
            totalProducts: { $sum: 1 },
            avgPrice: { $avg: "$price" },
            minPrice: { $min: "$price" },
            maxPrice: { $max: "$price" }
          }
        }
      ],
      
      // Facet 3: Get top 2 most expensive products
      topProducts: [
        { $sort: { price: -1 } },
        { $limit: 2 },
        { $project: { _id: 1, category: 1, price: 1 } }
      ]
    }
  }
])

// Output - Single document with three facet arrays
{
  categoryStats: [
    { _id: "electronics", count: 2, avgPrice: 150 },
    { _id: "books", count: 1, avgPrice: 15 }
  ],
  overallStats: [
    { _id: null, totalProducts: 3, avgPrice: 105, minPrice: 15, maxPrice: 200 }
  ],
  topProducts: [
    { _id: 2, category: "electronics", price: 200 },
    { _id: 1, category: "electronics", price: 100 }
  ]
}
```

### Pagination with Metadata

```javascript
// Common pattern: Get paginated results + total count in one query
db.products.aggregate([
  { $match: { status: "active" } },
  {
    $facet: {
      // Facet 1: Get total count for pagination
      metadata: [
        { $count: "total" },
        { 
          $addFields: { 
            page: 1, 
            pageSize: 10,
            hasMore: { $gt: ["$total", 10] }
          } 
        }
      ],
      
      // Facet 2: Get actual page of data
      data: [
        { $sort: { createdAt: -1 } },
        { $skip: 0 },
        { $limit: 10 },
        { $project: { name: 1, price: 1, category: 1 } }
      ],
      
      // Facet 3: Get quick stats for UI
      quickStats: [
        {
          $group: {
            _id: null,
            categories: { $addToSet: "$category" },
            priceRange: {
              min: { $min: "$price" },
              max: { $max: "$price" }
            }
          }
        }
      ]
    }
  }
])

// Output structure for UI consumption
{
  metadata: [{ total: 156, page: 1, pageSize: 10, hasMore: true }],
  data: [
    { _id: 1, name: "Product A", price: 100, category: "electronics" },
    // ... 9 more products
  ],
  quickStats: [{ 
    _id: null, 
    categories: ["electronics", "books", "clothing"],
    priceRange: { min: 5, max: 999 }
  }]
}
```

## Real-World Examples from Your Pipeline

### Competition Data Faceting

```javascript
// From your competition-full.js pipeline
{
  $facet: {
    // Facet 1: SGO (Sports Governing Organization) data collection
    sgos: [
      { $project: { sgoMemberships: 1 } },
      { $unwind: { path: '$sgoMemberships', preserveNullAndEmptyArrays: true } },
      {
        $lookup: {
          from: 'sgos',
          let: {
            sid: '$sgoMemberships._externalSgoId',
            sscope: '$sgoMemberships._externalSgoIdScope',
          },
          pipeline: [
            { $match: { $expr: { $and: [
              { $eq: ['$_externalId', '$$sid'] }, 
              { $eq: ['$_externalIdScope', '$$sscope'] }
            ] } } },
            { $project: { _id: 1 } }
          ],
          as: 'sgo',
        },
      },
      { $unwind: { path: '$sgo', preserveNullAndEmptyArrays: true } },
      { $group: { _id: null, ids: { $addToSet: '$sgo._id' } } },
      { $project: { _id: 0, ids: 1 } },
    ],
    
    // Facet 2: Stages data collection
    stages: [
      {
        $lookup: {
          from: 'stages',
          let: { cid: '$_externalId', cs: '$_externalIdScope' },
          pipeline: [
            { $match: { $expr: { $and: [
              { $eq: ['$_externalCompetitionId', '$$cid'] }, 
              { $eq: ['$_externalCompetitionIdScope', '$$cs'] }
            ] } } },
            { $project: { _id: 1, _externalId: 1, _externalIdScope: 1 } },
          ],
          as: 'stages',
        },
      },
      { $unwind: { path: '$stages', preserveNullAndEmptyArrays: true } },
      { $group: { 
        _id: null, 
        ids: { $addToSet: '$stages._id' }, 
        stageKeys: { $addToSet: { 
          id: '$stages._externalId', 
          scope: '$stages._externalIdScope' 
        } } 
      } },
      { $project: { _id: 0, ids: 1, stageKeys: 1 } },
    ],
    
    // Facet 3: Events data collection (more complex)
    events: [
      // First get stages to build stage keys
      {
        $lookup: {
          from: 'stages',
          let: { cid: '$_externalId', cs: '$_externalIdScope' },
          pipeline: [
            { $match: { $expr: { $and: [
              { $eq: ['$_externalCompetitionId', '$$cid'] }, 
              { $eq: ['$_externalCompetitionIdScope', '$$cs'] }
            ] } } },
            { $project: { _externalId: 1, _externalIdScope: 1 } },
          ],
          as: 'stages',
        },
      },
      // Transform stage data into searchable keys
      {
        $addFields: {
          _stageKeys: {
            $map: {
              input: '$stages',
              as: 's',
              in: { $concat: ['$$s._externalId', '|', '$$s._externalIdScope'] },
            },
          },
        },
      },
      // Lookup events using the stage keys
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
                  ],
                },
              },
            },
            {
              $project: {
                _id: 1,
                _externalVenueId: 1,
                _externalVenueIdScope: 1,
                participants: 1,
              },
            },
          ],
          as: 'events',
        },
      },
      { $project: { events: '$events' } },
    ],
  },
}

// What this produces:
{
  sgos: [{ ids: [ObjectId("..."), ObjectId("..."), ...] }],
  stages: [{ 
    ids: [ObjectId("..."), ObjectId("..."), ...],
    stageKeys: [
      { id: "group-stage", scope: "fifa" },
      { id: "knockout", scope: "fifa" }
    ]
  }],
  events: [{ 
    events: [
      { _id: ObjectId("..."), _externalVenueId: "venue1", participants: [...] },
      // ... more events
    ] 
  }]
}
```

## Advanced Faceting Patterns

### Statistical Analysis Dashboard

```javascript
db.sales.aggregate([
  { $match: { date: { $gte: new Date("2024-01-01") } } },
  {
    $facet: {
      // Revenue analysis
      revenueStats: [
        {
          $group: {
            _id: { $dateToString: { format: "%Y-%m", date: "$date" } },
            monthlyRevenue: { $sum: "$amount" },
            transactionCount: { $sum: 1 }
          }
        },
        { $sort: { _id: 1 } }
      ],
      
      // Top products
      topProducts: [
        {
          $group: {
            _id: "$productId",
            totalSales: { $sum: "$amount" },
            unitsSold: { $sum: "$quantity" }
          }
        },
        { $sort: { totalSales: -1 } },
        { $limit: 10 }
      ],
      
      // Geographic distribution
      regionBreakdown: [
        {
          $group: {
            _id: "$region",
            sales: { $sum: "$amount" },
            avgOrderSize: { $avg: "$amount" }
          }
        },
        { $sort: { sales: -1 } }
      ],
      
      // Customer segments
      customerSegments: [
        {
          $group: {
            _id: "$customerId",
            totalSpent: { $sum: "$amount" },
            orderCount: { $sum: 1 }
          }
        },
        {
          $bucket: {
            groupBy: "$totalSpent",
            boundaries: [0, 100, 500, 1000, 5000, Infinity],
            default: "Other",
            output: {
              customers: { $sum: 1 },
              avgSpend: { $avg: "$totalSpent" }
            }
          }
        }
      ]
    }
  }
])
```

### A/B Testing Analysis

```javascript
db.experiments.aggregate([
  { $match: { experimentId: "checkout_flow_v2" } },
  {
    $facet: {
      // Control group metrics
      controlGroup: [
        { $match: { variant: "control" } },
        {
          $group: {
            _id: null,
            users: { $sum: 1 },
            conversions: { $sum: { $cond: ["$converted", 1, 0] } },
            avgRevenue: { $avg: "$revenue" }
          }
        },
        {
          $addFields: {
            conversionRate: { $divide: ["$conversions", "$users"] }
          }
        }
      ],
      
      // Treatment group metrics  
      treatmentGroup: [
        { $match: { variant: "treatment" } },
        {
          $group: {
            _id: null,
            users: { $sum: 1 },
            conversions: { $sum: { $cond: ["$converted", 1, 0] } },
            avgRevenue: { $avg: "$revenue" }
          }
        },
        {
          $addFields: {
            conversionRate: { $divide: ["$conversions", "$users"] }
          }
        }
      ],
      
      // Daily breakdown for both groups
      dailyTrend: [
        {
          $group: {
            _id: {
              date: { $dateToString: { format: "%Y-%m-%d", date: "$timestamp" } },
              variant: "$variant"
            },
            users: { $sum: 1 },
            conversions: { $sum: { $cond: ["$converted", 1, 0] } }
          }
        },
        { $sort: { "_id.date": 1 } }
      ]
    }
  }
])
```

### Content Performance Analysis

```javascript
db.articles.aggregate([
  { $match: { publishDate: { $gte: new Date("2024-01-01") } } },
  {
    $facet: {
      // Engagement metrics
      engagement: [
        {
          $group: {
            _id: "$category",
            avgViews: { $avg: "$views" },
            avgShares: { $avg: "$shares" },
            avgComments: { $avg: "$comments" },
            articles: { $sum: 1 }
          }
        },
        { $sort: { avgViews: -1 } }
      ],
      
      // Viral content (top performers)
      viralContent: [
        {
          $addFields: {
            engagementScore: {
              $add: [
                { $multiply: ["$views", 1] },
                { $multiply: ["$shares", 10] },
                { $multiply: ["$comments", 5] }
              ]
            }
          }
        },
        { $sort: { engagementScore: -1 } },
        { $limit: 10 },
        { $project: { title: 1, category: 1, engagementScore: 1, views: 1, shares: 1 } }
      ],
      
      // Author performance
      authorStats: [
        {
          $group: {
            _id: "$author",
            articlesPublished: { $sum: 1 },
            totalViews: { $sum: "$views" },
            avgViewsPerArticle: { $avg: "$views" }
          }
        },
        { $sort: { totalViews: -1 } },
        { $limit: 20 }
      ],
      
      // Publishing trend
      publishingTrend: [
        {
          $group: {
            _id: { $dayOfWeek: "$publishDate" },
            articlesCount: { $sum: 1 },
            avgViews: { $avg: "$views" }
          }
        },
        { $sort: { _id: 1 } }
      ]
    }
  }
])
```

## Performance Considerations and Optimization

### Memory Management

```javascript
// Each facet runs independently but shares input documents
// Memory usage = input documents + (sum of all facet intermediate results)

db.collection.aggregate([
  // Filter early to reduce facet input size
  { $match: { status: "active", date: { $gte: recentDate } } },
  { $project: { field1: 1, field2: 1, field3: 1 } }, // Remove unnecessary fields
  
  {
    $facet: {
      // Add limits to control facet output size
      topItems: [
        { $sort: { score: -1 } },
        { $limit: 100 }  // Prevent massive arrays
      ],
      
      stats: [
        { $group: { _id: null, count: { $sum: 1 } } }  // Minimal output
      ]
    }
  }
], { allowDiskUse: true })  // Enable disk usage for large datasets
```

### BSON Size Management

```javascript
// Monitor total output size - must be under 16MB
{
  $facet: {
    // Each facet should limit its output
    heavyData: [
      { $limit: 1000 },  // Prevent large arrays
      { $project: { essentialField1: 1, essentialField2: 1 } }  // Reduce document size
    ],
    
    lightStats: [
      { $group: { _id: null, count: { $sum: 1 }, avg: { $avg: "$value" } } }
    ]
  }
}

// If hitting size limits, consider breaking into multiple aggregations
```

### Index Utilization

```javascript
// Indexes are used by stages within facets, not by $facet itself
// Ensure pre-facet filtering uses indexes effectively

db.collection.createIndex({ status: 1, category: 1, date: -1 })

[
  // This stage can use the index
  { $match: { status: "active", category: "electronics" } },
  
  {
    $facet: {
      // These internal stages may also use indexes
      recentItems: [
        { $match: { date: { $gte: yesterday } } },  // Can use date part of index
        { $sort: { date: -1 } }                     // Can use index for sorting
      ]
    }
  }
]
```

## Post-Facet Processing Patterns

### Flattening Facet Results

```javascript
// Transform facet output into more usable structure
[
  {
    $facet: {
      totals: [{ $count: "count" }],
      items: [{ $limit: 10 }]
    }
  },
  
  // Extract values from facet arrays
  {
    $project: {
      totalCount: { $arrayElemAt: ["$totals.count", 0] },  // Get first element
      itemList: "$items",                                   // Keep array as-is
      hasItems: { $gt: [{ $size: "$items" }, 0] }         // Boolean check
    }
  }
]
```

### Conditional Facet Processing

```javascript
[
  {
    $facet: {
      metadata: [{ $count: "total" }],
      data: [{ $limit: 20 }]
    }
  },
  
  // Add conditional fields based on facet results
  {
    $addFields: {
      totalCount: { $ifNull: [{ $arrayElemAt: ["$metadata.total", 0] }, 0] },
      isEmpty: { $eq: [{ $size: "$data" }, 0] },
      pagination: {
        hasMore: { 
          $gt: [
            { $ifNull: [{ $arrayElemAt: ["$metadata.total", 0] }, 0] }, 
            20
          ] 
        }
      }
    }
  }
]
```

### Combining Facet Results

```javascript
// From your pipeline - combining facet outputs
{
  $project: {
    sgos: { $ifNull: [{ $first: '$sgos.ids' }, []] },           // Extract SGO IDs array
    stages: { $ifNull: [{ $first: '$stages.ids' }, []] },       // Extract stage IDs array
    events: {
      $ifNull: [
        {
          $setUnion: [                                           // Create union of event IDs
            {
              $map: {
                input: { $ifNull: [{ $first: '$events.events' }, []] },
                as: 'e',
                in: '$$e._id',
              },
            },
            [],
          ],
        },
        [],
      ],
    }
  }
}
```

## Common Pitfalls and Solutions

### 1. Facet Independence Misunderstanding

```javascript
// WRONG: Trying to reference one facet from another
{
  $facet: {
    totals: [{ $count: "total" }],
    percentage: [
      {
        $project: {
          // This won't work - can't access $totals from here
          percent: { $divide: ["$value", "$totals.total"] }
        }
      }
    ]
  }
}

// CORRECT: Do post-processing after $facet
[
  {
    $facet: {
      totals: [{ $count: "total" }],
      items: [{ $project: { value: 1 } }]
    }
  },
  {
    $addFields: {
      totalValue: { $arrayElemAt: ["$totals.total", 0] }
    }
  },
  {
    $addFields: {
      itemsWithPercent: {
        $map: {
          input: "$items",
          as: "item",
          in: {
            value: "$$item.value",
            percent: { $divide: ["$$item.value", "$totalValue"] }
          }
        }
      }
    }
  }
]
```

### 2. BSON Size Limit Exceeded

```javascript
// PROBLEM: Facets producing too much data
{
  $facet: {
    allItems: [/* no limits - could be millions of docs */],
    allCategories: [/* potentially huge arrays */]
  }
}

// SOLUTION: Add limits and projections
{
  $facet: {
    topItems: [
      { $sort: { priority: -1 } },
      { $limit: 1000 },                    // Limit array size
      { $project: { _id: 1, name: 1 } }    // Reduce document size
    ],
    categoryStats: [
      { $group: { _id: "$category", count: { $sum: 1 } } }  // Aggregate instead of raw data
    ]
  }
}
```

### 3. Empty Facet Handling

```javascript
// Input: No documents match pre-facet filters
// Result: All facets will be empty arrays

{
  $facet: {
    data: [{ $limit: 10 }],
    stats: [{ $count: "total" }]
  }
}
// Output: { data: [], stats: [] }

// Handle empty results gracefully
{
  $project: {
    hasData: { $gt: [{ $size: "$data" }, 0] },
    totalCount: { $ifNull: [{ $arrayElemAt: ["$stats.total", 0] }, 0] },
    items: { $cond: [{ $gt: [{ $size: "$data" }, 0] }, "$data", []] }
  }
}
```

## When to Use `$facet`

### Good Use Cases

- **Multi-dimensional analysis**: Need different views of the same dataset
- **Dashboard queries**: Multiple statistics/charts from one dataset  
- **Pagination with metadata**: Count + page data in single query
- **A/B testing analysis**: Compare metrics across groups simultaneously
- **Performance optimization**: Replace multiple separate queries with one
- **Complex reporting**: Multiple aggregation types on filtered data

### When NOT to Use `$facet`

- **Simple single aggregation**: Use regular pipeline stages
- **Sequential processing**: When later stages need results from earlier ones
- **Very large result sets**: Risk hitting BSON size limits
- **Memory constraints**: Large input + multiple heavy facets can exhaust memory
- **Different input filtering**: When facets need different source data

## Best Practices Checklist

Before using `$facet`:

- ✅ Filter input data early to reduce facet processing load
- ✅ Add limits to facets that might produce large arrays  
- ✅ Project only necessary fields before and within facets
- ✅ Consider BSON 16MB limit for total output
- ✅ Use `allowDiskUse: true` for large datasets
- ✅ Plan post-facet processing for combining results
- ✅ Ensure facets are truly independent (no cross-references needed)
- ✅ Monitor explain() output for performance bottlenecks

---

*`$facet` is powerful for multi-dimensional analysis but requires careful planning around memory usage and output size. Your competition-full.js pipeline demonstrates sophisticated usage for collecting related entity data in parallel, enabling efficient downstream processing while maintaining clear separation of concerns.*
