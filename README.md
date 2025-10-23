# Gameday – Cross Collection Aggregations (Proof of Concept)

## Table of Contents

- [1. Overview](#1-overview)
- [2. Background – The Gameday Platform](#2-background--the-gameday-platform)
- [3. Purpose of the Project](#3-purpose-of-the-project)
- [4. Project Structure](#4-project-structure)
- [5. Application Flow](#5-application-flow)
- [6. Routes](#6-routes)
- [7. Aggregation Behaviour](#7-aggregation-behaviour)
- [8. Materialisation Process](#8-materialisation-process)
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

## 8. Materialisation Process

### Ingest Integration

When a resource is added or updated:

1. The ingest service identifies changed resources.
2. The appropriate pipeline from `src/pipelines` is executed.
3. A pre-materialised aggregation document is written to `materialisedAggregations`.

These aggregation documents contain:

- The root resource type and key.
- Lists of related document `_id` values grouped by type.

### API Integration

When a client requests a resource via the API:

1. The API retrieves the root resource.
2. Joins pre-materialised data from `materialisedAggregations`.
3. Optionally resolves and embeds referenced documents up to a defined limit.
4. Returns a unified, aggregated response.

---

## 9. MongoDB Schema and Indexing

**Collection:** `materialisedAggregations`

| Field | Description |
|--------|-------------|
| `resourceType` | Type of the root resource (e.g. `competition`, `event`). |
| `_externalIdScope`, `_externalId` | Identify the external key of the root document. |
| `targetType` | Aggregated resource type (e.g. `stages`, `events`, `teams`). |
| `references` | Array of referenced document IDs. |
| `updatedAt` | Timestamp of last aggregation. |

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
- Introduce pagination and rate limiting for client aggregations.
- Replace Express development endpoints with production-ready API integration.
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
