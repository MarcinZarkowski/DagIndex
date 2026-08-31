package dagindex

import "testing"

func TestDagIndexSearch(t *testing.T) {
	r := NewDagIndex[testEvent]()
	a := dagFromRoot(node("A", "2026-01-01"), "MSFT", "NVDA")
	b := dagFromRoot(node("B", "2026-01-02"), "NVDA", "AMD")
	r.AddDag(a)
	r.AddDag(b)
	got := r.SearchDags([]string{"NVDA", "MSFT"}, 0.3)
	if len(got) != 2 || got[0] != a {
		t.Fatalf("expected overlap-ranked DAGs")
	}
	if entities := a.Entities(); len(entities) != 2 || entities[0] != "TICKER:MSFT" || entities[1] != "TICKER:NVDA" {
		t.Fatalf("expected normalized DAG entities, got %v", entities)
	}
}

func TestDagIndexNormalizesNamespacedEntities(t *testing.T) {
	r := NewDagIndex[testEvent]()
	dag := dagFromRoot(node("A", "2026-01-01"), "AAPL", "industry:Technology", "macro:Interest   Rates")
	r.AddDag(dag)

	if got := NormalizeEntity(" ticker:aapl "); got != "TICKER:AAPL" {
		t.Fatalf("unexpected ticker normalization: %q", got)
	}
	if got := NormalizeEntity("industry:Technology"); got != "INDUSTRY:TECHNOLOGY" {
		t.Fatalf("unexpected industry normalization: %q", got)
	}
	if got := NormalizeEntity("macro:Interest   Rates"); got != "MACRO:INTEREST RATES" {
		t.Fatalf("unexpected macro normalization: %q", got)
	}
	if got := r.SearchDags([]string{"industry:technology"}, 0.3); len(got) != 1 || got[0] != dag {
		t.Fatalf("namespaced search did not find DAG")
	}
	if got := r.SearchDags([]string{"AAPL"}, 0.3); len(got) != 1 || got[0] != dag {
		t.Fatalf("bare ticker search did not remain compatible")
	}
}

func TestAddNodeToDagsSharesNodeAndTracksMembership(t *testing.T) {
	index := NewDagIndex[testEvent]()
	first := dagFromRoot(node("A", "2026-01-01"), "AAPL")
	second := dagFromRoot(node("B", "2026-01-02"), "MSFT")
	shared := &Node[testEvent]{
		ExternalId: "event-1",
		StartTime:  "2026-01-03",
		EndTime:    "2026-01-03",
	}

	index.AddNodeToDags([]*Dag[testEvent]{first, second, first}, shared)

	if first.head.Nexts[0].Nexts[0] != shared || second.head.Nexts[0].Nexts[0] != shared {
		t.Fatalf("expected both DAGs to contain the same node pointer")
	}
	memberDags, ok := index.GetNodeDags("event-1")
	if !ok || len(memberDags) != 2 {
		t.Fatalf("expected membership in both DAGs")
	}
	if got := index.SearchDags([]string{"MSFT"}, 0.1); len(got) != 1 || got[0] != second {
		t.Fatalf("expected shared node to leave DAG anchors unchanged")
	}
}

func TestAddNodeToDagsAccumulatesSharedNodeSuccessors(t *testing.T) {
	firstBefore := &Node[testEvent]{ExternalId: "A", StartTime: "2026-01-01", EndTime: "2026-01-01"}
	firstAfter := &Node[testEvent]{ExternalId: "C", StartTime: "2026-01-03", EndTime: "2026-01-03"}
	firstBefore.Nexts = []*Node[testEvent]{firstAfter}
	secondBefore := &Node[testEvent]{ExternalId: "B", StartTime: "2026-01-01", EndTime: "2026-01-01"}
	secondAfter := &Node[testEvent]{ExternalId: "D", StartTime: "2026-01-04", EndTime: "2026-01-04"}
	secondBefore.Nexts = []*Node[testEvent]{secondAfter}
	shared := &Node[testEvent]{ExternalId: "X", StartTime: "2026-01-02", EndTime: "2026-01-02"}

	index := NewDagIndex[testEvent]()
	index.AddNodeToDags(
		[]*Dag[testEvent]{dagFromRoot(firstBefore), dagFromRoot(secondBefore)},
		shared,
	)

	if len(shared.Nexts) != 2 || shared.Nexts[0] != firstAfter || shared.Nexts[1] != secondAfter {
		t.Fatalf("expected shared node to retain successors from both DAGs")
	}
}

func TestDagIndexUsesEntityTypeWeights(t *testing.T) {
	dag := dagFromRoot(node("A", "2026-01-01"), "MSFT")
	index := NewDagIndex[testEvent](map[string]float64{
		"ticker":   10,
		"industry": 1,
	})
	index.AddDag(dag)

	if got := index.SearchDags([]string{"MSFT", "industry:Technology"}, 0.9); len(got) != 1 || got[0] != dag {
		t.Fatalf("expected ticker weight to dominate similarity")
	}
	if got := index.SearchDags([]string{"MSFT", "industry:Technology"}, 0.95); len(got) != 0 {
		t.Fatalf("expected weighted similarity below threshold")
	}
}

func TestDagIndexAllowsUnweightedEntityTypes(t *testing.T) {
	dag := dagFromRoot(node("A", "2026-01-01"), "industry:Technology")
	index := NewDagIndex[testEvent](map[string]float64{"ticker": 10})
	index.AddDag(dag)

	if got := index.SearchDags([]string{"industry:Technology"}, 1); len(got) != 1 || got[0] != dag {
		t.Fatalf("expected unweighted entity type to use the default weight")
	}
}
