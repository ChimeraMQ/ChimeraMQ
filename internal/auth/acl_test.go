package auth

import (
	"testing"
)

func TestACLAllowAll(t *testing.T) {
	acl := NewACLEngine(PermissionAllow)
	id := &Identity{UserID: "user1"}

	if !acl.Check(id, ResourceTopic, "test", OpRead) {
		t.Error("default allow should permit all")
	}
}

func TestACLDenyAll(t *testing.T) {
	acl := NewACLEngine(PermissionDeny)
	id := &Identity{UserID: "user1"}

	if acl.Check(id, ResourceTopic, "test", OpRead) {
		t.Error("default deny should reject all")
	}
}

func TestACLWildcardPrincipal(t *testing.T) {
	acl := NewACLEngine(PermissionDeny)
	acl.AddEntry(ACLEntry{
		Principal:    "*",
		ResourceType: ResourceTopic,
		ResourceName: "public",
		Operation:    OpRead,
		Permission:   PermissionAllow,
	})

	id := &Identity{UserID: "anyone"}
	if !acl.Check(id, ResourceTopic, "public", OpRead) {
		t.Error("wildcard principal should match")
	}
	if acl.Check(id, ResourceTopic, "public", OpWrite) {
		t.Error("write should not match read-only rule")
	}
}

func TestACLSpecificUser(t *testing.T) {
	acl := NewACLEngine(PermissionDeny)
	acl.AddEntry(ACLEntry{
		Principal:    "admin",
		ResourceType: ResourceCluster,
		ResourceName: "*",
		Operation:    OpAll,
		Permission:   PermissionAllow,
	})

	admin := &Identity{UserID: "admin"}
	user := &Identity{UserID: "user"}

	if !acl.Check(admin, ResourceCluster, "anything", OpCreate) {
		t.Error("admin should be allowed")
	}
	if acl.Check(user, ResourceCluster, "anything", OpCreate) {
		t.Error("user should be denied")
	}
}

func TestACLDenyOverrides(t *testing.T) {
	acl := NewACLEngine(PermissionAllow)
	// Allow all reads
	acl.AddEntry(ACLEntry{
		Principal:    "*",
		ResourceType: ResourceTopic,
		ResourceName: "*",
		Operation:    OpRead,
		Permission:   PermissionAllow,
	})
	// Deny specific user
	acl.AddEntry(ACLEntry{
		Principal:    "baduser",
		ResourceType: ResourceTopic,
		ResourceName: "secret",
		Operation:    OpRead,
		Permission:   PermissionDeny,
	})

	goodUser := &Identity{UserID: "gooduser"}
	badUser := &Identity{UserID: "baduser"}

	// gooduser reads "other" topic: first rule matches (allow)
	if !acl.Check(goodUser, ResourceTopic, "other", OpRead) {
		t.Error("gooduser should read other topics")
	}

	// baduser reads "secret": with deny-wins semantics, the specific deny
	// entry matches even though the wildcard allow comes first.
	if acl.Check(badUser, ResourceTopic, "secret", OpRead) {
		t.Error("baduser should be denied for secret topic (deny-wins)")
	}
}

func TestACLDenyFirst(t *testing.T) {
	acl := NewACLEngine(PermissionAllow)
	// Deny first
	acl.AddEntry(ACLEntry{
		Principal:    "baduser",
		ResourceType: ResourceTopic,
		ResourceName: "secret",
		Operation:    OpRead,
		Permission:   PermissionDeny,
	})
	// Then allow all
	acl.AddEntry(ACLEntry{
		Principal:    "*",
		ResourceType: ResourceTopic,
		ResourceName: "*",
		Operation:    OpRead,
		Permission:   PermissionAllow,
	})

	badUser := &Identity{UserID: "baduser"}
	goodUser := &Identity{UserID: "gooduser"}

	if acl.Check(badUser, ResourceTopic, "secret", OpRead) {
		t.Error("baduser should be denied for secret topic")
	}
	if !acl.Check(badUser, ResourceTopic, "public", OpRead) {
		t.Error("baduser should be allowed for non-secret topics")
	}
	if !acl.Check(goodUser, ResourceTopic, "secret", OpRead) {
		t.Error("gooduser should be allowed for secret topic")
	}
}

func TestACLOpAll(t *testing.T) {
	acl := NewACLEngine(PermissionDeny)
	acl.AddEntry(ACLEntry{
		Principal:    "admin",
		ResourceType: ResourceTopic,
		ResourceName: "*",
		Operation:    OpAll,
		Permission:   PermissionAllow,
	})

	admin := &Identity{UserID: "admin"}
	ops := []Operation{OpRead, OpWrite, OpCreate, OpDelete, OpAlter, OpDescribe}
	for _, op := range ops {
		if !acl.Check(admin, ResourceTopic, "any-topic", op) {
			t.Errorf("admin should be allowed for operation %d", op)
		}
	}
}

func TestACLResourceTypeIsolation(t *testing.T) {
	acl := NewACLEngine(PermissionDeny)
	acl.AddEntry(ACLEntry{
		Principal:    "user1",
		ResourceType: ResourceTopic,
		ResourceName: "test",
		Operation:    OpRead,
		Permission:   PermissionAllow,
	})

	id := &Identity{UserID: "user1"}
	if !acl.Check(id, ResourceTopic, "test", OpRead) {
		t.Error("should be allowed for matching resource type")
	}
	if acl.Check(id, ResourceSchema, "test", OpRead) {
		t.Error("should be denied for different resource type")
	}
}

func TestACLNilIdentity(t *testing.T) {
	acl := NewACLEngine(PermissionAllow)
	if !acl.Check(nil, ResourceTopic, "test", OpRead) {
		t.Error("nil identity with default allow should pass")
	}

	acl2 := NewACLEngine(PermissionDeny)
	if acl2.Check(nil, ResourceTopic, "test", OpRead) {
		t.Error("nil identity with default deny should fail")
	}
}

func TestACLRoleMatching(t *testing.T) {
	acl := NewACLEngine(PermissionDeny)
	acl.AddEntry(ACLEntry{
		Principal:    "admin",
		ResourceType: ResourceCluster,
		ResourceName: "*",
		Operation:    OpAll,
		Permission:   PermissionAllow,
	})

	id := &Identity{UserID: "user1", Roles: []string{"admin"}}
	if !acl.Check(id, ResourceCluster, "anything", OpCreate) {
		t.Error("user with admin role should be allowed")
	}
}

func TestACLGlobPattern(t *testing.T) {
	acl := NewACLEngine(PermissionDeny)
	acl.AddEntry(ACLEntry{
		Principal:    "*",
		ResourceType: ResourceTopic,
		ResourceName: "public.*",
		Operation:    OpRead,
		Permission:   PermissionAllow,
	})

	id := &Identity{UserID: "user1"}
	if !acl.Check(id, ResourceTopic, "public.events", OpRead) {
		t.Error("public.* should match public.events")
	}
	if acl.Check(id, ResourceTopic, "private.events", OpRead) {
		t.Error("public.* should not match private.events")
	}
}

func TestACLSetEntries(t *testing.T) {
	acl := NewACLEngine(PermissionDeny)
	acl.SetEntries([]ACLEntry{
		{
			Principal:    "user1",
			ResourceType: ResourceTopic,
			ResourceName: "*",
			Operation:    OpRead,
			Permission:   PermissionAllow,
		},
	})

	id := &Identity{UserID: "user1"}
	if !acl.Check(id, ResourceTopic, "any", OpRead) {
		t.Error("set entries should work")
	}
}

func TestParsePermission(t *testing.T) {
	if ParsePermission("allow") != PermissionAllow {
		t.Error("allow")
	}
	if ParsePermission("deny") != PermissionDeny {
		t.Error("deny")
	}
	if ParsePermission("unknown") != PermissionAllow {
		t.Error("default should be allow")
	}
}

func TestParseResourceType(t *testing.T) {
	tests := map[string]ResourceType{
		"topic":   ResourceTopic,
		"group":   ResourceConsumerGroup,
		"cluster": ResourceCluster,
		"schema":  ResourceSchema,
		"wasm":    ResourceWASM,
	}
	for input, expected := range tests {
		if ParseResourceType(input) != expected {
			t.Errorf("ParseResourceType(%q) != %d", input, expected)
		}
	}
}

func TestParseOperation(t *testing.T) {
	tests := map[string]Operation{
		"read":     OpRead,
		"write":    OpWrite,
		"create":   OpCreate,
		"delete":   OpDelete,
		"all":      OpAll,
		"describe": OpDescribe,
	}
	for input, expected := range tests {
		if ParseOperation(input) != expected {
			t.Errorf("ParseOperation(%q) != %d", input, expected)
		}
	}
}
