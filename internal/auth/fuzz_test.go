package auth

import (
	"context"
	"testing"
)

// FuzzParsePermission verifies that permission parsing handles arbitrary
// input safely.
func FuzzParsePermission(f *testing.F) {
	f.Add("allow")
	f.Add("deny")
	f.Add("")
	f.Add("ALLOW")
	f.Add("Deny")
	f.Add("read")
	f.Add("unknown")

	f.Fuzz(func(t *testing.T, perm string) {
		_ = ParsePermission(perm)
	})
}

// FuzzParseOperation verifies that operation parsing handles arbitrary input.
func FuzzParseOperation(f *testing.F) {
	f.Add("read")
	f.Add("write")
	f.Add("create")
	f.Add("delete")
	f.Add("")
	f.Add("READ")
	f.Add("unknown")

	f.Fuzz(func(t *testing.T, op string) {
		_ = ParseOperation(op)
	})
}

// FuzzParseResourceType verifies that resource type parsing handles arbitrary input.
func FuzzParseResourceType(f *testing.F) {
	f.Add("topic")
	f.Add("queue")
	f.Add("broker")
	f.Add("")
	f.Add("TOPIC")
	f.Add("unknown")

	f.Fuzz(func(t *testing.T, res string) {
		_ = ParseResourceType(res)
	})
}

// FuzzOAuthProviderAuthenticate verifies that OAuth provider handles
// arbitrary JWT tokens without panics.
func FuzzOAuthProviderAuthenticate(f *testing.F) {
	provider, err := NewOAuthProvider(
		"https://example.com",
		"test-client",
		"",
		"",
	)
	if err != nil {
		f.Skipf("cannot create OAuth provider: %v", err)
	}

	f.Add("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0KsRvBA")
	f.Add("")
	f.Add("not-a-jwt")
	f.Add("header.payload")
	f.Add("header.payload.signature.extra")

	f.Fuzz(func(t *testing.T, token string) {
		_, _ = provider.Authenticate(context.Background(), Credentials{Token: token})
	})
}

// FuzzSCRAMSessionVerifyClientFinal verifies that SCRAM client final
// verification handles arbitrary input safely.
func FuzzSCRAMSessionVerifyClientFinal(f *testing.F) {
	f.Add("c=biws,r=nonce,s=salt,i=4096")
	f.Add("")
	f.Add("c=,r=,s=,i=0")
	f.Add("no-equals-signs")
	f.Add("c=biws,r=x,s=y,i=1")

	f.Fuzz(func(t *testing.T, data string) {
		s := &SCRAMSession{
			clientFirstBare: "n=test",
			serverNonce:     "nonce",
			salt:            []byte("salt"),
			iterations:      4096,
			storedKey:       make([]byte, 32),
			authMessage:     "prefix",
		}
		_, _ = s.VerifyClientFinal(data)
	})
}

// FuzzStaticProviderAuthenticate verifies that static auth provider
// handles arbitrary credentials safely.
func FuzzStaticProviderAuthenticate(f *testing.F) {
	provider := NewStaticProvider(
		map[string]string{"admin": "admin-pass"},
		map[string]string{"token123": "admin"},
	)

	f.Add("token123")
	f.Add("")
	f.Add("invalid-token")
	f.Add("Bearer token123")

	f.Fuzz(func(t *testing.T, token string) {
		_, _ = provider.Authenticate(context.Background(), Credentials{Token: token})
	})
}

// FuzzACLEngineCheck verifies that ACL engine handles arbitrary identities
// and resource names safely.
func FuzzACLEngineCheck(f *testing.F) {
	engine := NewACLEngine(PermissionAllow)
	engine.AddEntry(ACLEntry{
		Principal:    "admin",
		ResourceType: ResourceTopic,
		ResourceName: "*",
		Operation:    OpRead,
		Permission:   PermissionAllow,
	})

	f.Add("admin", "test-topic")
	f.Add("", "")
	f.Add("unknown", "unknown")

	f.Fuzz(func(t *testing.T, username, name string) {
		identity := &Identity{UserID: username}
		_ = engine.Check(identity, ResourceTopic, name, OpRead)
		_ = engine.Check(identity, ResourceTopic, name, OpWrite)
		_ = engine.Check(identity, ResourceTopic, name, OpCreate)
		_ = engine.Check(identity, ResourceTopic, name, OpDelete)
	})
}
