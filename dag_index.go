package dagindex

import (
	"errors"
	"sort"
	"strings"
)

// DagIndex indexes timeline DAGs by their participating entities. It does
// not perform KNN or reranking; callers should use those systems for duplicate
// detection before adding a new event to a DAG.
type DagIndex[T any] struct {
	dags       map[*Dag[T]]struct{}
	entityDAGs map[string]map[*Dag[T]]struct{}
	nodeDags   map[string]map[*Dag[T]]struct{}
}

func NewDagIndex[T any]() *DagIndex[T] {
	return &DagIndex[T]{
		dags:       map[*Dag[T]]struct{}{},
		entityDAGs: map[string]map[*Dag[T]]struct{}{},
		nodeDags:   map[string]map[*Dag[T]]struct{}{},
	}
}

func (r *DagIndex[T]) AddDag(dag *Dag[T]) {
	if r == nil || dag == nil {
		return
	}
	if r.dags == nil {
		r.dags = map[*Dag[T]]struct{}{}
		r.entityDAGs = map[string]map[*Dag[T]]struct{}{}
		r.nodeDags = map[string]map[*Dag[T]]struct{}{}
	}
	r.dags[dag] = struct{}{}
	for entity := range dag.entities {
		if r.entityDAGs[entity] == nil {
			r.entityDAGs[entity] = map[*Dag[T]]struct{}{}
		}
		r.entityDAGs[entity][dag] = struct{}{}
	}
}

// AddNodeToDags adds the same node to each DAG and records its membership.
func (r *DagIndex[T]) AddNodeToDags(dags []*Dag[T], node *Node[T]) {
	if r == nil || node == nil || node.ExternalId == "" {
		return
	}
	if r.nodeDags == nil {
		r.nodeDags = map[string]map[*Dag[T]]struct{}{}
	}
	if r.nodeDags[node.ExternalId] == nil {
		r.nodeDags[node.ExternalId] = map[*Dag[T]]struct{}{}
	}

	seen := map[*Dag[T]]struct{}{}
	for _, dag := range dags {
		if dag == nil {
			continue
		}
		if _, ok := seen[dag]; ok {
			continue
		}
		seen[dag] = struct{}{}

		if err := dag.InsertNode(node); err == nil || errors.Is(err, ErrDuplicateNode) {
			r.nodeDags[node.ExternalId][dag] = struct{}{}
		}
	}
}

func (r *DagIndex[T]) GetNodeDags(id string) ([]*Dag[T], bool) {
	set, ok := r.nodeDags[id]
	if !ok {
		return nil, false
	}
	result := make([]*Dag[T], 0, len(set))
	for dag := range set {
		result = append(result, dag)
	}
	return result, true
}

// Search returns DAGs with at least minOverlap weighted Jaccard alignment,
// ordered from strongest to weakest. Candidate DAGs are obtained through the
// inverted entity index rather than scanning the entire registry.
func (r *DagIndex[T]) SearchDags(entityIDs []string, minOverlap float64) []*Dag[T] {
	if r == nil {
		return nil
	}
	if minOverlap < 0 {
		minOverlap = 0
	}
	query := normalizeEntities(entityIDs)
	candidates := map[*Dag[T]]struct{}{}
	for entity := range query {
		for dag := range r.entityDAGs[entity] {
			candidates[dag] = struct{}{}
		}
	}
	type scored struct {
		dag   *Dag[T]
		score float64
	}
	scores := make([]scored, 0, len(candidates))
	for dag := range candidates {
		score := entityOverlap(query, dag.entities)
		if score >= minOverlap {
			scores = append(scores, scored{dag, score})
		}
	}
	sort.Slice(scores, func(i, j int) bool {
		return scores[i].score > scores[j].score
	})
	result := make([]*Dag[T], 0, len(scores))
	for _, item := range scores {
		result = append(result, item.dag)
	}
	return result
}

func normalizeEntities(values []string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, value := range values {
		if value = NormalizeEntity(value); value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

// NormalizeEntity returns the canonical representation used by DagIndex's
// indexes. Entities may be explicitly namespaced, for example
// "ticker:AAPL", "industry:Technology", or "macro:Interest Rates". Bare
// values remain supported and are treated as ticker symbols for compatibility
// with the original API. Namespace and value matching are case-insensitive;
// whitespace is collapsed and the result is stored in uppercase.
func NormalizeEntity(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	namespace, entityValue := "ticker", value
	if parts := strings.SplitN(value, ":", 2); len(parts) == 2 {
		namespace = strings.TrimSpace(parts[0])
		entityValue = strings.TrimSpace(parts[1])
		if namespace == "" {
			namespace = "ticker"
		}
	}
	entityValue = strings.Join(strings.Fields(entityValue), " ")
	if entityValue == "" {
		return ""
	}
	return strings.ToUpper(namespace) + ":" + strings.ToUpper(entityValue)
}

func entityOverlap(a, b map[string]struct{}) float64 {
	if len(a) == 0 || len(b) == 0 {
		return 0
	}
	intersection := 0
	for id := range a {
		if _, ok := b[id]; ok {
			intersection++
		}
	}
	union := len(a) + len(b) - intersection
	if union == 0 {
		return 0
	}
	return float64(intersection) / float64(union)
}
