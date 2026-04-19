package connector

import (
	"fmt"
	"net"
	"sort"
	"strings"
)

type AccessDecision struct {
	Allowed bool
	Reason  string
}

type ConnectorPolicy struct {
	Name         string   `yaml:"name"`
	Enabled      bool     `yaml:"enabled"`
	AllowedDNs   []string `yaml:"allowed_dns"`
	AllowedCIDRs []string `yaml:"allowed_cidrs"`
}

type policyRuntime struct {
	cfg   ConnectorPolicy
	dnSet map[string]struct{}
	nets  []*net.IPNet
}

type PolicySet struct {
	byName map[string]*policyRuntime
}

func NewPolicySet(policies []ConnectorPolicy) (*PolicySet, error) {
	ps := &PolicySet{byName: make(map[string]*policyRuntime, len(policies))}
	for _, p := range policies {
		name := strings.TrimSpace(p.Name)
		if name == "" {
			return nil, fmt.Errorf("connector policy: empty connector name")
		}
		if _, exists := ps.byName[name]; exists {
			return nil, fmt.Errorf("connector policy: duplicate connector name %q", name)
		}

		rt := &policyRuntime{
			cfg:   p,
			dnSet: map[string]struct{}{},
		}
		// default true unless explicitly false in YAML
		if !p.Enabled && p.Enabled != false {
			rt.cfg.Enabled = true
		}

		for _, dn := range p.AllowedDNs {
			norm := normalizeDN(dn)
			if norm == "" {
				continue
			}
			rt.dnSet[norm] = struct{}{}
		}
		for _, raw := range p.AllowedCIDRs {
			_, cidr, err := net.ParseCIDR(strings.TrimSpace(raw))
			if err != nil {
				return nil, fmt.Errorf("connector policy %q: invalid cidr %q: %w", name, raw, err)
			}
			rt.nets = append(rt.nets, cidr)
		}

		ps.byName[name] = rt
	}
	return ps, nil
}

func (ps *PolicySet) Evaluate(connectorName, peerDN string, peerIP net.IP) AccessDecision {
	rt, ok := ps.byName[connectorName]
	if !ok {
		return AccessDecision{Allowed: false, Reason: "connector not found"}
	}
	if !rt.cfg.Enabled {
		return AccessDecision{Allowed: false, Reason: "connector disabled"}
	}

	if len(rt.dnSet) > 0 {
		if _, ok := rt.dnSet[normalizeDN(peerDN)]; !ok {
			return AccessDecision{Allowed: false, Reason: "dn not allowed"}
		}
	}

	if len(rt.nets) > 0 {
		if peerIP == nil {
			return AccessDecision{Allowed: false, Reason: "missing peer ip"}
		}
		allowed := false
		for _, n := range rt.nets {
			if n.Contains(peerIP) {
				allowed = true
				break
			}
		}
		if !allowed {
			return AccessDecision{Allowed: false, Reason: "ip not allowed"}
		}
	}

	return AccessDecision{Allowed: true, Reason: "ok"}
}

func (ps *PolicySet) AllowedConnectors(peerDN string, peerIP net.IP) []string {
	out := make([]string, 0, len(ps.byName))
	for name := range ps.byName {
		if ps.Evaluate(name, peerDN, peerIP).Allowed {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func normalizeDN(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}
