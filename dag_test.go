package dagindex

import "testing"

type testEvent struct{}

func node(id, end string) *Node[testEvent] { return &Node[testEvent]{ExternalId: id, EndTime: end} }

func dagFromRoot(root *Node[testEvent], entityIDs ...string) *Dag[testEvent] {
	head := NewNode[testEvent]()
	head.Nexts = []*Node[testEvent]{root}
	return NewDag(head, entityIDs...)
}

func TestGetLatestBeforeReturnsDeduplicatedFrontier(t *testing.T) {
	a := node("A", "2026-01-01")
	b := node("B", "2026-01-03")
	c := node("C", "2026-01-03")
	d := node("D", "2026-01-05")
	a.Nexts = []*Node[testEvent]{b, c}
	b.Nexts = []*Node[testEvent]{d}
	c.Nexts = []*Node[testEvent]{d}
	got := dagFromRoot(a).getLatestBefore("2026-01-04")
	_, hasB := got[b]
	_, hasC := got[c]
	if len(got) != 2 || !hasB || !hasC {
		t.Fatalf("expected B and C as frontier")
	}
}

func TestGetLatestBeforeIgnoresUnknownTimes(t *testing.T) {
	if got := dagFromRoot(node("A", "")).getLatestBefore("2026-01-04"); len(got) != 0 {
		t.Fatalf("expected unknown-time node to be ignored")
	}
}

func TestAddNodeInsertsBetweenNodes(t *testing.T) {
	a := &Node[testEvent]{ExternalId: "A", StartTime: "2026-01-01", EndTime: "2026-01-01"}
	b := &Node[testEvent]{ExternalId: "B", StartTime: "2026-01-03", EndTime: "2026-01-03"}
	x := &Node[testEvent]{ExternalId: "X", StartTime: "2026-01-02", EndTime: "2026-01-02"}
	a.Nexts = []*Node[testEvent]{b}

	dag := dagFromRoot(a)
	if !dag.addNode(x) {
		t.Fatal("expected insertion to succeed")
	}
	if len(a.Nexts) != 1 || a.Nexts[0] != x || len(x.Nexts) != 1 || x.Nexts[0] != b {
		t.Fatal("expected A -> X -> B")
	}
}

func TestAddNodeAddsEarlierRoot(t *testing.T) {
	a := &Node[testEvent]{ExternalId: "A", StartTime: "2026-01-02", EndTime: "2026-01-02"}
	x := &Node[testEvent]{ExternalId: "X", StartTime: "2026-01-01", EndTime: "2026-01-01"}
	dag := dagFromRoot(a)

	if !dag.addNode(x) {
		t.Fatal("expected insertion to succeed")
	}
	if len(dag.head.Nexts) != 1 || dag.head.Nexts[0] != x || len(x.Nexts) != 1 || x.Nexts[0] != a {
		t.Fatal("expected X to become the root before A")
	}
}

func TestAddNodeRejectsDuplicateAndInvalidTime(t *testing.T) {
	a := &Node[testEvent]{ExternalId: "A", StartTime: "2026-01-01", EndTime: "2026-01-01"}
	x := &Node[testEvent]{ExternalId: "X", StartTime: "2026-01-02", EndTime: "2026-01-02"}
	dag := dagFromRoot(a)
	if !dag.addNode(x) || dag.addNode(x) {
		t.Fatal("expected the second insertion to be rejected")
	}

	invalid := &Node[testEvent]{ExternalId: "invalid", StartTime: "2026-01-04", EndTime: "2026-01-03"}
	if dag.addNode(invalid) {
		t.Fatal("expected invalid temporal interval to be rejected")
	}
}

func TestAddNodePreservesUnreplacedEdges(t *testing.T) {
	a := &Node[testEvent]{ExternalId: "A", StartTime: "2026-01-01", EndTime: "2026-01-01"}
	b := &Node[testEvent]{ExternalId: "B", StartTime: "2026-01-03", EndTime: "2026-01-03"}
	unknown := &Node[testEvent]{ExternalId: "unknown"}
	x := &Node[testEvent]{ExternalId: "X", StartTime: "2026-01-02", EndTime: "2026-01-02"}
	a.Nexts = []*Node[testEvent]{b, unknown}

	dag := dagFromRoot(a)
	if !dag.addNode(x) {
		t.Fatal("expected insertion to succeed")
	}
	if len(a.Nexts) != 2 || a.Nexts[0] != unknown || a.Nexts[1] != x {
		t.Fatal("expected unrelated edge to remain and X to replace only B")
	}
	if len(x.Nexts) != 1 || x.Nexts[0] != b {
		t.Fatal("expected X to connect to replaced successor B")
	}
}

func TestAddNodeRejectsCycle(t *testing.T) {
	a := &Node[testEvent]{ExternalId: "A", StartTime: "2026-01-01", EndTime: "2026-01-01"}
	x := &Node[testEvent]{ExternalId: "X", StartTime: "2026-01-02", EndTime: "2026-01-02"}
	x.Nexts = []*Node[testEvent]{a}
	dag := dagFromRoot(a)

	if dag.addNode(x) {
		t.Fatal("expected cycle-producing insertion to be rejected")
	}
	if len(a.Nexts) != 0 {
		t.Fatal("expected rejected insertion to leave the DAG unchanged")
	}
}
