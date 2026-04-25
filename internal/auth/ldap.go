package auth

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/go-ldap/ldap/v3"
)

// LDAPProvider authenticates against an LDAP directory.
type LDAPProvider struct {
	url       string
	bindDN    string
	bindPass  string
	baseDN    string
	filter    string
	useTLS    bool
	roleAttr  string // attribute for roles (default: "memberOf")
	groupAttr string // attribute for groups
	tenantID      string          // default tenant ID for authenticated users
	roleAllowlist map[string]bool // optional allowlist for role filtering
}

// NewLDAPProvider creates an LDAP auth provider.
func NewLDAPProvider(url, bindDN, bindPass, baseDN, filter string, useTLS bool, tenantID string) *LDAPProvider {
	return &LDAPProvider{
		url:      url,
		bindDN:   bindDN,
		bindPass: bindPass,
		baseDN:   baseDN,
		filter:   filter,
		useTLS:   useTLS,
		roleAttr: "memberOf",
		tenantID:     tenantID,
	}
}

// NewLDAPProviderWithRoleAllowlist creates an LDAP provider with a role allowlist.
// Only roles in the allowlist are granted; if allowlist is nil, all roles are accepted.
func NewLDAPProviderWithRoleAllowlist(url, bindDN, bindPass, baseDN, filter string, useTLS bool, tenantID string, roleAllowlist []string) *LDAPProvider {
	allowmap := make(map[string]bool)
	for _, r := range roleAllowlist {
		allowmap[r] = true
	}
	return &LDAPProvider{
		url:           url,
		bindDN:        bindDN,
		bindPass:      bindPass,
		baseDN:        baseDN,
		filter:        filter,
		useTLS:        useTLS,
		tenantID:     tenantID,
		roleAttr:      "memberOf",
		roleAllowlist: allowmap,
	}
}

// Authenticate binds to LDAP as the service account, searches for the user,
// then rebinds as the user to verify the password.
func (p *LDAPProvider) Authenticate(ctx context.Context, creds Credentials) (*Identity, error) {
	if creds.Username == "" || creds.Password == "" {
		return nil, ErrInvalidCredentials
	}

	conn, err := p.dial()
	if err != nil {
		return nil, fmt.Errorf("ldap dial: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Bind as service account to search
	if err := conn.Bind(p.bindDN, p.bindPass); err != nil {
		return nil, fmt.Errorf("ldap service bind: %w", err)
	}

	// Search for user
	filter := p.filter
	if filter == "" {
		filter = "(uid={username})"
	}
	filter = strings.ReplaceAll(filter, "{username}", ldap.EscapeFilter(creds.Username))

	attrs := []string{"dn", "cn", "uid"}
	if p.roleAttr != "" {
		attrs = append(attrs, p.roleAttr)
	}
	if p.groupAttr != "" {
		attrs = append(attrs, p.groupAttr)
	}

	searchReq := ldap.NewSearchRequest(
		p.baseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 1, 0, false,
		filter,
		attrs,
		nil,
	)

	sr, err := conn.Search(searchReq)
	if err != nil {
		return nil, fmt.Errorf("ldap search: %w", err)
	}

	if len(sr.Entries) == 0 {
		return nil, ErrInvalidCredentials
	}

	entry := sr.Entries[0]
	userDN := entry.DN

	// Rebind as user to verify password
	if err := conn.Bind(userDN, creds.Password); err != nil {
		return nil, ErrInvalidCredentials
	}

	// Extract roles from memberOf (with optional allowlist filtering)
	var roles []string
	if p.roleAttr != "" {
		rawRoles := entry.GetAttributeValues(p.roleAttr)
		for _, dn := range rawRoles {
			cn := extractCN(dn)
			if cn == "" {
				continue
			}
			if p.roleAllowlist != nil {
				if p.roleAllowlist[cn] {
					roles = append(roles, cn)
				}
			} else {
				roles = append(roles, cn)
			}
		}
	}

	// Extract groups
	var groups []string
	if p.groupAttr != "" {
		groups = entry.GetAttributeValues(p.groupAttr)
	}

	userID := creds.Username
	if cn := entry.GetAttributeValue("cn"); cn != "" {
		userID = cn
	}

	return &Identity{
		UserID: userID,
		TenantID:    p.tenantID,
		Roles:  roles,
		Groups: groups,
		Source: "ldap",
	}, nil
}

// Close is a no-op for LDAP.
func (p *LDAPProvider) Close() error {
	return nil
}

func (p *LDAPProvider) dial() (*ldap.Conn, error) {
	conn, err := ldap.DialURL(p.url)
	if err != nil {
		return nil, fmt.Errorf("ldap dial: %w", err)
	}
	if p.useTLS {
		host := strings.TrimPrefix(p.url, "ldap://")
		host = strings.TrimSuffix(host, ":"+extractPort(p.url))
		if err := conn.StartTLS(&tls.Config{
			ServerName:         host,
			MinVersion:         tls.VersionTLS12,
			InsecureSkipVerify: false,
		}); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("ldap StartTLS: %w", err)
		}
	}
	return conn, nil
}

// extractPort pulls the port number from an ldap:// URL.
func extractPort(url string) string {
	if idx := strings.LastIndex(url, ":"); idx >= 0 {
		return url[idx+1:]
	}
	return "389"
}

// extractCN extracts the CN from a DN string.
func extractCN(dn string) string {
	parts := strings.Split(dn, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(strings.ToLower(part), "cn=") {
			return part[3:]
		}
	}
	return dn
}
