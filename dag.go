package dagindex

import (
	"errors"
	"sort"
)

var (
	ErrInvalidNode   = errors.New("invalid node")
	ErrDuplicateNode = errors.New("node already exists")
	ErrCycle         = errors.New("insertion would create a cycle")
)

type Node[T any] struct {
	ID         string
	ExternalId string
	Added      string
	StartTime  string
	EndTime    string
	Value      T
	Nexts      []*Node[T]
}

func NewNode[T any]() *Node[T] { return &Node[T]{} }

type Dag[T any] struct {
	head     *Node[T]
	entities map[string]struct{}
}

func NewDag[T any](node *Node[T], entityIDs ...string) *Dag[T] {
	return &Dag[T]{head: node, entities: normalizeEntities(entityIDs)}
}

func (d *Dag[T]) HeadNode() *Node[T] {
	if d == nil {
		return nil
	}
	return d.head
}

func (d *Dag[T]) Entities() []string {
	if d == nil {
		return nil
	}
	entities := make([]string, 0, len(d.entities))
	for entity := range d.entities {
		entities = append(entities, entity)
	}
	sort.Strings(entities)
	return entities
}

func (d *Dag[T]) AddRootNode(node *Node[T]) {
	if d == nil || d.head == nil || node == nil {
		return
	}
	d.head.Nexts = appendUniqueNodes(d.head.Nexts, node)
}

// addNode inserts newNode between its latest predecessors and their valid
// successors. It returns false when the node is invalid, already present, or
// would create a cycle.
func (dag *Dag[T]) addNode(newNode *Node[T]) bool {
	if dag == nil || dag.head == nil || !hasValidTimes(newNode) || newNode.ExternalId == "" {
		return false
	}
	if dag.hasNode(newNode) || dag.hasExternalID(newNode.ExternalId) {
		return false
	}

	before := dag.getLatestBefore(newNode.StartTime)
	if len(before) == 0 {
		return dag.addRootNode(newNode)
	}

	for beforeNode := range before {
		if dag.reachable(newNode, beforeNode) {
			return false
		}
	}

	successors := make([]*Node[T], 0)
	for beforeNode := range before {
		for _, next := range beforeNode.Nexts {
			if isSuccessor(newNode, next) {
				if dag.reachable(next, newNode) {
					return false
				}
				successors = appendUniqueNodes(successors, next)
			}
		}
	}

	for beforeNode := range before {
		nexts := make([]*Node[T], 0, len(beforeNode.Nexts)+1)
		for _, next := range beforeNode.Nexts {
			if !isSuccessor(newNode, next) {
				nexts = appendUniqueNodes(nexts, next)
			}
		}
		beforeNode.Nexts = appendUniqueNodes(nexts, newNode)
	}
	newNode.Nexts = successors
	return true
}

// InsertNode adds a node to the DAG according to its temporal position.
func (dag *Dag[T]) InsertNode(newNode *Node[T]) error {
	if dag == nil || dag.head == nil || !hasValidTimes(newNode) || newNode.ExternalId == "" {
		return ErrInvalidNode
	}
	if dag.hasNode(newNode) || dag.hasExternalID(newNode.ExternalId) {
		return ErrDuplicateNode
	}
	if !dag.addNode(newNode) {
		return ErrCycle
	}
	return nil
}

func (dag *Dag[T]) addRootNode(newNode *Node[T]) bool {
	successors := make([]*Node[T], 0)
	roots := make([]*Node[T], 0, len(dag.head.Nexts)+1)
	for _, root := range dag.head.Nexts {
		if isSuccessor(newNode, root) {
			if dag.reachable(root, newNode) {
				return false
			}
			successors = appendUniqueNodes(successors, root)
			continue
		}
		roots = appendUniqueNodes(roots, root)
	}
	dag.head.Nexts = appendUniqueNodes(roots, newNode)
	newNode.Nexts = successors
	return true
}

func hasValidTimes[T any](node *Node[T]) bool {
	return node != nil && node.StartTime != "" && node.EndTime != "" && node.StartTime <= node.EndTime
}

func isSuccessor[T any](newNode, candidate *Node[T]) bool {
	return candidate != nil && candidate.StartTime != "" && newNode.EndTime <= candidate.StartTime
}

func (dag *Dag[T]) hasExternalID(externalID string) bool {
	if externalID == "" {
		return false
	}
	return dag.hasNodeMatching(func(node *Node[T]) bool { return node.ExternalId == externalID })
}

func (dag *Dag[T]) hasNode(target *Node[T]) bool {
	return dag.hasNodeMatching(func(node *Node[T]) bool { return node == target })
}

func (dag *Dag[T]) hasNodeMatching(match func(*Node[T]) bool) bool {
	if dag == nil || dag.head == nil {
		return false
	}
	seen := map[*Node[T]]struct{}{}
	stack := append([]*Node[T]{}, dag.head.Nexts...)
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current == nil {
			continue
		}
		if _, ok := seen[current]; ok {
			continue
		}
		seen[current] = struct{}{}
		if match(current) {
			return true
		}
		stack = append(stack, current.Nexts...)
	}
	return false
}

func (dag *Dag[T]) reachable(from, target *Node[T]) bool {
	if from == nil || target == nil {
		return false
	}
	seen := map[*Node[T]]struct{}{}
	stack := []*Node[T]{from}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current == target {
			return true
		}
		if current == nil {
			continue
		}
		if _, ok := seen[current]; ok {
			continue
		}
		seen[current] = struct{}{}
		stack = append(stack, current.Nexts...)
	}
	return false
}

// getLatestBefore returns the frontier nodes that end at or before thresholdStart.
func (dag *Dag[T]) getLatestBefore(thresholdStart string) map[*Node[T]]struct{} {
	if dag == nil || dag.head == nil || thresholdStart == "" {
		return nil
	}

	memo := map[*Node[T]]map[*Node[T]]struct{}{}
	visiting := map[*Node[T]]struct{}{}
	var walk func(*Node[T]) map[*Node[T]]struct{}
	walk = func(curr *Node[T]) map[*Node[T]]struct{} {
		if cached, ok := memo[curr]; ok {
			return cached
		}
		result := make(map[*Node[T]]struct{})
		if curr == nil || curr.EndTime == "" || curr.EndTime > thresholdStart {
			return result
		}
		if _, ok := visiting[curr]; ok {
			return result
		}
		visiting[curr] = struct{}{}
		defer delete(visiting, curr)
		if len(curr.Nexts) == 0 {
			result[curr] = struct{}{}
			memo[curr] = result
			return result
		}

		for _, neighbor := range curr.Nexts {
			if neighbor != nil && neighbor.EndTime != "" && neighbor.EndTime <= thresholdStart {
				for node := range walk(neighbor) {
					result[node] = struct{}{}
				}
			} else {
				result[curr] = struct{}{}
			}
		}
		memo[curr] = result
		return result
	}

	result := make(map[*Node[T]]struct{})
	for _, root := range dag.head.Nexts {
		for node := range walk(root) {
			result[node] = struct{}{}
		}
	}
	return result
}

// GetLatestBefore returns the frontier nodes ending at or before thresholdStart.
func (dag *Dag[T]) GetLatestBefore(thresholdStart string) []*Node[T] {
	frontier := dag.getLatestBefore(thresholdStart)
	result := make([]*Node[T], 0, len(frontier))
	for node := range frontier {
		result = append(result, node)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].EndTime == result[j].EndTime {
			return result[i].ExternalId < result[j].ExternalId
		}
		return result[i].EndTime < result[j].EndTime
	})
	return result
}

func appendUniqueNodes[T any](dst []*Node[T], nodes ...*Node[T]) []*Node[T] {
	for _, candidate := range nodes {
		if candidate == nil {
			continue
		}
		found := false
		for _, existing := range dst {
			if existing == candidate {
				found = true
				break
			}
		}
		if !found {
			dst = append(dst, candidate)
		}
	}
	return dst
}
