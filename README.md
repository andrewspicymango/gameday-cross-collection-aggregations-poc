# gameday-cross-collection-aggregations-poc

## Overview

This project is a proof of concept for aggregating cross-collection data using a Node.js application with an Express HTTP server and a MongoDB Atlas database.

## Aggregation Pipeline Behaviour per Resource Type

### Competition

- When a competition is created, would need to create
  - a competition stages aggregration view
  - a competition events aggregration view
  - a competition venues aggregration view
  - a competition sgos aggregation view
- When a competition is updated, would need to update
  - the competition sgos aggregation view
