# dagIndex

A Go library for maintaining multiple directed acyclic graphs (DAGs) tied to entities, representing temporal sequences of events.

Each DAG holds chronologically ordered events as nodes. DAGs are indexed by participating entities (tickers, industries, macros, etc.) so you can search for relevant timelines by overlap.

## Core Types

- **Node[T]** — a single event with an external ID, start/end times, and a typed value
- **Dag[T]** — a DAG of nodes connected by temporal edges
- **DagIndex[T]** — a registry that maps entities to DAGs and ranks them by overlap

## Usage

```go
import "dagIndex"

// Build a DAG
head := dagIndex.NewNode[MyEvent]()
d := dagIndex.NewDag(head)
d.AddRootNode(&dagIndex.Node[MyEvent]{
    ExternalId: "evt-1",
    EndTime:    "2026-01-01",
    Value:      myEvent,
})

// Query frontier before a threshold
frontier := d.GetLatestBefore("2026-01-15")

// Index and search DAGs by entity
idx := dagIndex.NewDagIndex[MyEvent]()
idx.AddDag("tech", d, []string{"MSFT", "NVDA"})
results := idx.SearchDags([]string{"NVDA"}, 0.3)
```

## Status

Early stage. Core graph structures and entity-based lookup are in place. Node insertion, cycle prevention, parent tracking, and shared-node support are planned.
