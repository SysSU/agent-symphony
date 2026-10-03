package main

import "strings"

// The source includes its format version: identities from different sources
// cannot establish that the host rebooted.
type hostBootIdentity struct {
	Source string `json:"source"`
	UUID   string `json:"uuid"`
}

func parseHostBootIdentity(source, value string) hostBootIdentity {
	if source != "linux-boot-id-v1" && source != "darwin-bootsessionuuid-v1" {
		return hostBootIdentity{}
	}
	value = strings.ToLower(value)
	if len(value) != 36 || value == "00000000-0000-0000-0000-000000000000" {
		return hostBootIdentity{}
	}
	for i, c := range value {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return hostBootIdentity{}
			}
		} else if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return hostBootIdentity{}
		}
	}
	return hostBootIdentity{Source: source, UUID: value}
}

func (identity hostBootIdentity) valid() bool {
	return identity != (hostBootIdentity{}) && parseHostBootIdentity(identity.Source, identity.UUID) == identity
}

func (identity hostBootIdentity) laterThan(previous hostBootIdentity) bool {
	return identity.valid() && previous.valid() && identity.Source == previous.Source && identity.UUID != previous.UUID
}
