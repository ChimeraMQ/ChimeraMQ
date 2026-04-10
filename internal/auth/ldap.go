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
}

// NewLDAPProvider creates an LDAP auth provider.
func NewLDAPProvider(url, bindDN, bindPass, baseDN, filter string, useTLS bool) *LDAPProvider {
	return &LDAPProvider{
		url:      url,
		bindDN:   bindDN,
		bindPass: bindPass,
		baseDN:   baseDN,
		filter:   filter,
		useTLS:   useTLS,
		roleAttr: "memberOf",
	}
}

// SetRoleAttr sets the LDAP attribute used for roles.
func (p *LDAPProvider) SetRoleAttr(attr string) {
	p.roleAttr = attr
}

// SetGroupAttr sets the LDAP attribute used for groups.
func (p *LDAPProvider) SetGroupAttr(attr string) {
	p.groupAttr = attr
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
	defer conn.Close()

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

	// Extract roles from memberOf
	var roles []string
	if p.roleAttr != "" {
		roles = entry.GetAttributeValues(p.roleAttr)
		// Extract CN from DN values
		for i, dn := range roles {
			if cn := extractCN(dn); cn != "" {
				roles[i] = cn
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
	if p.useTLS {
		return ldap.DialTLS("tcp", p.url, &tls.Config{
			ServerName: strings.TrimPrefix(strings.TrimPrefix(p.url, "ldaps://"), "ldap://"),
		})
	}
	return ldap.DialURL(p.url)
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
