# Gameday – Cross Collection Aggregations (Proof of Concept)

## Table of Contents

- [1. Overview](#1-overview)
- [2. Background – The Gameday Platform](#2-background--the-gameday-platform)
- [3. Purpose of the Project](#3-purpose-of-the-project)
- [4. Project Structure](#4-project-structure)
- [5. Application Flow](#5-application-flow)
- [6. Routes](#6-routes)
- [7. Aggregation Behaviour](#7-aggregation-behaviour)
- [8. Integration](#8-integration)
- [9. MongoDB Schema and Indexing](#9-mongodb-schema-and-indexing)
- [10. Configuration](#10-configuration)
- [11. Logging and Error Handling](#11-logging-and-error-handling)
- [12. Future Integration and Enhancements](#12-future-integration-and-enhancements)
- [13. Running the Service](#13-running-the-service)
- [14. License](#14-license)

---

## 1. Overview

This repository provides a **proof of concept** implementation of *Cross Collection Aggregations* for the **Gameday™** sports metadata platform.

The purpose of this service is to demonstrate how Gameday can aggregate and materialise related resources stored across multiple MongoDB collections, improving API response performance and reducing repeated multi-collection joins at runtime.

The proof of concept runs as a standalone **Express service**, connecting to a MongoDB instance and exposing endpoints that:

- Generate and store **pre-materialised aggregation documents** during ingest.
- Retrieve and assemble **client-facing aggregated responses** based on those pre-materialised records.

---

## 2. Background – The Gameday Platform

**Gameday** is a unified *sports data warehouse and metadata platform* that ingests, transforms, and serves structured data from multiple sports data providers.  
Each data entity is normalised into a consistent model and stored as a separate MongoDB collection, such as:

- `competition`
- `stage`
- `event`
- `team`
- `ranking`
- `staff`
- `keyMoment`
- `club`
- `nation`
- `sportsPerson`
- `venue`
- `sgo` (Sports Governing Organisation)

Each document is uniquely identified by its `_id` (Gameday ID) and external composite key (`_externalId`, `_externalIdScope`, `resourceType`).

The relationships between these entities are represented by a directed graph known as **EDGES**, which defines how one resource type references another (e.g. `competition → stage → event → team`).

---

## 3. Purpose of the Project

MongoDB can traverse relationships dynamically using `$lookup` stages, but performing deep or repeated joins at runtime can be slow.  
This project demonstrates how to **pre-materialise** these joins (to a depth of one hop) during ingest and how to **re-use** those results when serving API responses.

Specifically, this proof of concept shows how to:

1. Build and store aggregation results into a `materialisedAggregations` collection.
2. Retrieve pre-joined data through API endpoints.
3. Apply Gameday’s edge-traversal rules to determine what relationships can be followed.

The solution will later be integrated into:

- The **Ingest Service**, which pre-materialises edges whenever a resource changes.
- The **API Service**, which re-joins pre-materialised results to construct complete client responses.

---

## 4. Project Structure

```txt
├── index.js                      # Application entry point
├── config.js                     # Configuration (service name, Mongo, ports, etc.)
├── log.js                         # Logging helpers (info, warn, error, debug)
├── .env                           # Environment variables (optional)
├── /routes/
│   ├── healthcheckRouter.js       # Health check endpoint
│   ├── logRouter.js               # Development log endpoints
│   └── gamedayDataRouter.js       # Aggregation endpoints (core PoC logic)
├── /controllers/
│   └── getSingleSportsDataController.js  # Example controller for aggregation API
├── /utils/
│   ├── mongoUtils.js              # Mongo connection and index utilities
│   ├── generalUtils.js            # Common helpers (e.g. normalise port)
│   └── httpResponseUtils.js       # Unified HTTP response helpers
└── /src/
├── /pipelines/                # MongoDB aggregation pipeline builders
├── /client/                   # Client-facing aggregation logic
├── /edges/                    # Definitions of resource relationships (EDGES)
└── /utils/                    # Shared utilities used in aggregation logic
```

---

## 5. Application Flow

### Startup Sequence (`index.js`)

1. Loads environment variables (`dotenv`).
2. Configures logging and port (`normalizePort`).
3. Connects to MongoDB (`connectToMongo`).
4. Ensures the `materialisedAggregations` collection exists.
5. Ensures required indexes exist:
   - `{ resourceType: 1, externalKey: 1 }`
   - `{ resourceType: 1, gamedayId: 1 }`
6. Verifies MongoDB session support.
7. Creates an Express application with:
   - JSON and URL-encoded body parsers.
   - CORS configuration.
   - Route registration.
8. Starts listening for incoming requests on the configured port.
9. Responds gracefully to `SIGINT` and `SIGTERM` signals (closes Mongo connection).

---

## 6. Routes

| Route | Purpose |
|-------|----------|
| `/healthcheck` | Service health endpoint for monitoring and orchestration systems. |
| `/log` | Development and diagnostics logging. |
| `/1-0/...` | Gameday data aggregation routes for testing cross-collection materialisation and retrieval. |

The aggregation routes allow testing of:

- Pre-materialisation pipelines (`src/pipelines`).
- Client-facing aggregation queries (`src/client`).

Examples (as implemented in `gamedayDataRouter.js`):

- GET `/aggregate/:schemaType/:scope/:id`
- POST `/aggregate/km/:eventIdScope/:eventId/:type/:subType/:dateTime`
- POST `/aggregate/rankings/:lType/:lIdScope/:lId/:pType/:pIdScope/:pId/:dateTimeLabel/:ranking`

---

## 7. Aggregation Behaviour

### Aggregation Rules

To control traversal and prevent excessive fan-out, the following rules apply when using an automatically generated route across the graph:

- **Rule 1 – Competition-Scoped Roots**  
  May traverse only from competition-scoped → non-competition-scoped resources.  
  Reverse traversal (non-competition → competition) is not allowed.

- **Rule 2 – Non-Competition-Scoped Roots**  
  May traverse both non-competition → competition and competition → non-competition,  
  but may not traverse between two competition-scoped resources.

- **Rule 3 – Key Moments**  
  Key Moments can only be aggregated when the root resource is an `event`.

These traversal rules are enforced when building aggregation pipelines from the EDGES graph.

---

## 8. Integration

### Ingest Integration

When a resource is added or updated:

1. The ingest service identifies changed resources.
2. The appropriate pipeline from `src/pipelines` is executed.
3. A pre-materialised aggregation document is written to `materialisedAggregations`.

### API Integration

When a client requests a resource via the API:

1. The API retrieves the root resource.
2. Joins pre-materialised data from `materialisedAggregations`.
3. Optionally resolves and embeds referenced documents up to a defined limit.
4. Returns a unified, aggregated response.

### Integration into Gameday

This proof of concept is intended to be integrated into two existing services in Gameday:

 1. Ingest Service. Pre-materialises one-hop edges whenever a resource is created or updated, writing documents to the materialisedAggregations collection.
 2. API Service. Uses the pre-materialised aggregation documents to assemble efficient cross-collection responses for clients, resolving referenced documents up to a configured limit.

The goals are reduced query latency, predictable server load, and a consistent traversal of the EDGES graph.

#### Work Packages for Integration

The following work items group the main tasks needed to productionise and integrate this PoC.

1. Ingest-side materialisation (depth 1)
1. 1. Triggering: After create or update of any supported resource type, to Mongo, trigger a depth 1 aggregation build for that resource. This writes the aggregation doc to `materialisedAggregations` with the agreed shape
1. 1. Indexing: Ensure the two unique indexes created at startup exist in all environments:
1. 1. 1. `{ resourceType: 1, externalKey: 1 }`
1. 1. 1. `{ resourceType: 1, gamedayId: 1 }`
1. API-side aggregation assembly
1. 1. Integrate PoC client side code into a GET request for a single resource.
1. 1. 1. This builds the Mongo pipeline to read the relevant materialisedAggregations subsets, union and deduplicate references by targetType, then resolve documents from their home collections.
1. 1. 1. It also applies inclusion and exclusion projections in the defined order, including participant and tag filters.
1. 1. 1. Return 401, 403, 422, and 503 as per SOW when limits or policy are breached.
1. Security and policy
1. 1. There is a desire to make sure aggregations are available to authorised users only. Although the PoC does not include this functionality, the API server is required to implement authorisation checking for use of the aggregation pipeline functionality by using claims in the AIDC token.

#### Cache strategy (future)

- Redis keys: Define stable cache keys that reflect the root, requested views, projections, and limit.
- Invalidation: On ingest update, determine the impacted aggregation keys and evict them. Start with coarse invalidation by root, then refine.

#### Security and policy

There is a desire to make sure aggregations are available to authorised users only. Although the PoC does not include this functionality, the API server is requiered

---

## 9. MongoDB Schema and Indexing

**Collection:** `materialisedAggregations`

| Field | Description |
|--------|-------------|
| `resourceType` | Type of the root resource (e.g. `competition`, `event`). |
| `externalKey`  | Identify the external key of the root document. |
| `gamedayId`  | The Mongo _id for for the root resource |
| `...<linkedResourceType>s` | An array of Mongo `_id` values for linked resources of that type (e.g. `stages` from a `competition` root resource) |
| `...<linkedResourceType>Keys` | An object with the keys being the `externalKey` of the materialised aggregation for that linked resource and whose value is the Mongo `_id` values for linked resources of that type  (e.g. `stageKeys` from a `competition` root resource) |

**Indexes created at startup**:

- `{ resourceType: 1, externalKey: 1 }` (unique)
- `{ resourceType: 1, gamedayId: 1 }` (unique)

---

## 10. Configuration

Configuration is managed through `config.js` and `.env`.

---

## 11. Logging and Error Handling

Logging functions are defined in `log.js` and support the following levels:

- `info`, `warn`, `error`, `debug`.

HTTP responses are normalised via utilities in `httpResponseUtils.js`:

- `send200`, `send400`, `send404`, `send500`, `sendError`.

---

## 12. Future Integration and Enhancements

- Implement Redis cache invalidation when resources update.
- Extend pre-materialisation to multiple hops (depth > 1).
- Add integration tests for edge traversal and performance benchmarking.

---

## 13. Running the Service

### Prerequisites

- Node.js ≥ 20  
- MongoDB instance accessible (local or remote)

### Install Dependencies

```bash
npm install
npm start # or with verbose logging, -v
```

## 14. License

Copyright © 2025

Spicy Mango / Gameday™

All rights reserved.

For internal development and evaluation purposes only.
