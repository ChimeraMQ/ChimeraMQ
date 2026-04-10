package chaos

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/chimeramq/chimera/internal/auth"
)

// TestConcurrentACLChecks tests the ACL engine under concurrent access.
func TestConcurrentACLChecks(t *testing.T) {
	acl := auth.NewACLEngine(auth.PermissionDeny)
	acl.AddEntry(auth.ACLEntry{
		Principal:    "*",
		ResourceType: auth.ResourceTopic,
		ResourceName: "public.*",
		Operation:    auth.OpRead,
		Permission:   auth.PermissionAllow,
	})
	acl.AddEntry(auth.ACLEntry{
		Principal:    "admin",
		ResourceType: auth.ResourceTopic,
		ResourceName: "*",
		Operation:    auth.OpAll,
		Permission:   auth.PermissionAllow,
	})

	var wg sync.WaitGroup
	var errors atomic.Int64

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			userID := "user"
			if id%10 == 0 {
				userID = "admin"
			}
			identity := &auth.Identity{UserID: userID}
			for j := 0; j < 1000; j++ {
				result := acl.Check(identity, auth.ResourceTopic, "public.events", auth.OpRead)
				if !result {
					errors.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()
	if errors.Load() > 0 {
		t.Errorf("got %d unexpected denials", errors.Load())
	}
}

// TestConcurrentACLEntryModification tests adding entries while checking.
func TestConcurrentACLEntryModification(t *testing.T) {
	acl := auth.NewACLEngine(auth.PermissionDeny)

	var wg sync.WaitGroup

	// Writer goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				acl.AddEntry(auth.ACLEntry{
					Principal:    "writer",
					ResourceType: auth.ResourceTopic,
					ResourceName: "topic",
					Operation:    auth.OpRead,
					Permission:   auth.PermissionAllow,
				})
			}
		}(i)
	}

	// Reader goroutines
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := &auth.Identity{UserID: "writer"}
			for j := 0; j < 1000; j++ {
				// Just ensure no panic
				acl.Check(id, auth.ResourceTopic, "topic", auth.OpRead)
			}
		}()
	}

	wg.Wait()
}

// TestConcurrentAuth tests concurrent authentication.
func TestConcurrentAuth(t *testing.T) {
	p := auth.NewStaticProvider(
		map[string]string{"user1": "pass1", "user2": "pass2", "admin": "admin123"},
		map[string]string{"token1": "user1", "token2": "user2"},
	)
	defer p.Close()

	var wg sync.WaitGroup
	var successes atomic.Int64
	var failures atomic.Int64

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				var creds auth.Credentials
				switch idx % 3 {
				case 0:
					creds = auth.Credentials{Username: "user1", Password: "pass1"}
				case 1:
					creds = auth.Credentials{Token: "token2"}
				case 2:
					creds = auth.Credentials{Username: "admin", Password: "admin123"}
				}

				_, err := p.Authenticate(context.TODO(), creds)
				if err != nil {
					failures.Add(1)
				} else {
					successes.Add(1)
				}
			}
		}(i)
	}

	wg.Wait()

	if failures.Load() > 0 {
		t.Errorf("got %d auth failures (expected 0)", failures.Load())
	}
	total := successes.Load()
	t.Logf("successful authentications: %d", total)
}

// TestStressACLEntryReplacement tests SetEntries under stress.
func TestStressACLEntryReplacement(t *testing.T) {
	acl := auth.NewACLEngine(auth.PermissionDeny)

	var wg sync.WaitGroup

	// Replacer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			entries := make([]auth.ACLEntry, 10)
			for j := range entries {
				entries[j] = auth.ACLEntry{
					Principal:    "*",
					ResourceType: auth.ResourceTopic,
					ResourceName: "*",
					Operation:    auth.OpRead,
					Permission:   auth.PermissionAllow,
				}
			}
			acl.SetEntries(entries)
			time.Sleep(time.Microsecond)
		}
	}()

	// Checkers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := &auth.Identity{UserID: "anyone"}
			for j := 0; j < 10000; j++ {
				acl.Check(id, auth.ResourceTopic, "test", auth.OpRead)
			}
		}()
	}

	wg.Wait()
}

// TestConcurrentMTLSIdentities tests mTLS provider concurrently.
func TestConcurrentMTLSIdentities(t *testing.T) {
	p := auth.NewMTLSProvider()
	defer p.Close()

	var wg sync.WaitGroup
	var errors atomic.Int64

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// No certs → should return ErrInvalidCredentials
			for j := 0; j < 100; j++ {
				_, err := p.Authenticate(context.TODO(), auth.Credentials{})
				if err != auth.ErrInvalidCredentials {
					errors.Add(1)
				}
			}
		}()
	}

	wg.Wait()
	if errors.Load() > 0 {
		t.Errorf("got %d unexpected results", errors.Load())
	}
}
