package auth

import (
	"path/filepath"
	"strings"
	"sync"
)

// ResourceType identifies the kind of resource being accessed.
type ResourceType int

const (
	ResourceTopic         ResourceType = iota
	ResourceConsumerGroup
	ResourceCluster
	ResourceSchema
	ResourceWASM
)

// Operation identifies the action being performed.
type Operation int

const (
	OpRead     Operation = iota
	OpWrite
	OpCreate
	OpDelete
	OpAlter
	OpDescribe
	OpAll
)

// Permission determines allow or deny.
type Permission int

const (
	PermissionAllow Permission = iota
	PermissionDeny
)

// ACLEntry defines a single access control rule.
type ACLEntry struct {
	Principal    string       // user/group ID, "*" matches all
	ResourceType ResourceType
	ResourceName string       // specific name or "*" for wildcard
	Operation    Operation    // OpAll matches all operations
	Permission   Permission
}

// ACLEngine evaluates access control entries.
type ACLEngine struct {
	mu            sync.RWMutex
	entries       []ACLEntry
	defaultPolicy Permission
}

// NewACLEngine creates a new ACL engine with the given default policy.
func NewACLEngine(defaultPolicy Permission) *ACLEngine {
	return &ACLEngine{
		entries:       make([]ACLEntry, 0),
		defaultPolicy: defaultPolicy,
	}
}

// AddEntry adds an ACL entry.
func (e *ACLEngine) AddEntry(entry ACLEntry) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entries = append(e.entries, entry)
}

// SetEntries replaces all ACL entries.
func (e *ACLEngine) SetEntries(entries []ACLEntry) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.entries = entries
}

// Check determines whether an identity is allowed to perform an operation.
func (e *ACLEngine) Check(identity *Identity, rt ResourceType, name string, op Operation) bool {
	if identity == nil {
		return e.defaultPolicy == PermissionAllow
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	for _, entry := range e.entries {
		if e.matchEntry(entry, identity, rt, name, op) {
			return entry.Permission == PermissionAllow
		}
	}

	return e.defaultPolicy == PermissionAllow
}

func (e *ACLEngine) matchEntry(entry ACLEntry, identity *Identity, rt ResourceType, name string, op Operation) bool {
	// Check principal
	if entry.Principal != "*" && entry.Principal != identity.UserID {
		// Check roles
		found := false
		for _, role := range identity.Roles {
			if entry.Principal == role {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check resource type
	if entry.ResourceType != rt {
		return false
	}

	// Check resource name (supports glob-style wildcard)
	if entry.ResourceName != "*" && !matchGlob(entry.ResourceName, name) {
		return false
	}

	// Check operation
	if entry.Operation != OpAll && entry.Operation != op {
		return false
	}

	return true
}

// matchGlob does simple glob matching: "*" matches any suffix/prefix.
func matchGlob(pattern, name string) bool {
	if pattern == "*" {
		return true
	}
	if matched, _ := filepath.Match(pattern, name); matched {
		return true
	}
	// Also support prefix wildcard like "public.*"
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, ".*")
		return strings.HasPrefix(name, prefix+".")
	}
	return pattern == name
}

// ParsePermission converts a string to a Permission.
func ParsePermission(s string) Permission {
	switch strings.ToLower(s) {
	case "allow":
		return PermissionAllow
	case "deny":
		return PermissionDeny
	default:
		return PermissionAllow
	}
}

// ParseResourceType converts a string to a ResourceType.
func ParseResourceType(s string) ResourceType {
	switch strings.ToLower(s) {
	case "topic":
		return ResourceTopic
	case "consumer_group", "consumergroup", "group":
		return ResourceConsumerGroup
	case "cluster":
		return ResourceCluster
	case "schema":
		return ResourceSchema
	case "wasm":
		return ResourceWASM
	default:
		return ResourceTopic
	}
}

// ParseOperation converts a string to an Operation.
func ParseOperation(s string) Operation {
	switch strings.ToLower(s) {
	case "read":
		return OpRead
	case "write":
		return OpWrite
	case "create":
		return OpCreate
	case "delete":
		return OpDelete
	case "alter":
		return OpAlter
	case "describe":
		return OpDescribe
	case "all":
		return OpAll
	default:
		return OpRead
	}
}
