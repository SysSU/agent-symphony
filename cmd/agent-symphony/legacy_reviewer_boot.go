package main

import (
	"encoding/json"
	"errors"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type legacyReviewerBaseline struct {
	Boot     hostBootIdentity `json:"boot"`
	Evidence string           `json:"evidence"`
}

// These certificates relax historical format validation only. They grant no
// process, filesystem cleanup, effect completion, or launch authority.
type legacyReviewerRelease struct {
	Before hostBootIdentity `json:"before"`
	After  hostBootIdentity `json:"after"`
}

func legacyReviewerDigest(kind string, value any) string {
	body, _ := json.Marshal(value) // Only concrete ledger records enter here.
	return digestText(kind + "\x00" + string(body))
}

func legacyManifestDigest(manifest agentruntime.Manifest, generation uint64) string {
	// Bind the old reviewer identity and attempt generation, while permitting
	// ordinary status/result updates to the same historical attempt.
	identity := agentruntime.Manifest{
		Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt,
		LaunchID: manifest.LaunchID, CreatedAt: manifest.CreatedAt,
		ReviewMode: manifest.ReviewMode, ReviewTarget: manifest.ReviewTarget,
		ReviewRunID: manifest.ReviewRunID, ReviewBase: manifest.ReviewBase, ReviewHead: manifest.ReviewHead,
		ReviewSnapshot: manifest.ReviewSnapshot, ReviewSession: manifest.ReviewSession,
	}
	return legacyReviewerDigest("manifest", struct {
		Generation uint64
		Identity   agentruntime.Manifest
	}{generation, identity})
}

func legacyEffectDigest(effect runtimeEffectIntent) string {
	return legacyReviewerDigest("effect", []any{
		effect.ID, runtimeEffectID(effect), effect.RequestDigest,
		effect.ReviewerSourceRevision, effect.ReviewerGateProtocol,
		effect.ReviewerGroupPID, effect.ReviewerProfileDigest, effect.ReviewerConfinementVersion,
		effect.SupersededReviewerGroupPID, effect.SupersededReviewerGateProtocol,
		effect.SupersededReviewerRequestDigest, effect.SupersededReviewerProfileDigest,
		effect.SupersededReviewerConfinementVersion,
	})
}

func legacyRecordReleased(state runtimeOwnerState, digest string) bool {
	release, ok := state.LegacyReviewerReleases[digest]
	return ok && release.After.laterThan(release.Before)
}

func admitLegacyReviewerQuarantine(state *runtimeOwnerState, issueKey, diagnostic string) {
	if state.LegacyReviewerQuarantines == nil {
		state.LegacyReviewerQuarantines = map[string]string{}
	}
	state.LegacyReviewerQuarantines[issueKey] = diagnostic
	// Even the same diagnostic can describe newly admitted evidence.
	delete(state.LegacyReviewerBaselines, issueKey)
}

func legacyTombstoneDigest(tombstone runtimeTombstone) string {
	manifest := ""
	if tombstone.Manifest != nil {
		manifest = legacyManifestDigest(*tombstone.Manifest, tombstone.InvalidatedGeneration)
	}
	return legacyReviewerDigest("tombstone", []any{tombstone.Repository, tombstone.Issue, tombstone.Attempt, tombstone.Generation, tombstone.InvalidatedGeneration, manifest})
}

func legacyReviewerEvidence(state runtimeOwnerState, issueKey string, historicalOnly bool) map[string]bool {
	records := map[string]bool{}
	for _, record := range state.Attempts {
		if ownerIssueKey(record.Manifest.Repository, record.Manifest.Issue) == issueKey && (!historicalOnly || record.Manifest.ReviewState != "" && !validDigest(record.Manifest.ReviewRunID) && !record.Manifest.ReviewRunCleaned) {
			records[legacyManifestDigest(record.Manifest, record.Generation)] = true
		}
	}
	for _, tombstone := range state.Tombstones {
		if ownerIssueKey(tombstone.Repository, tombstone.Issue) == issueKey {
			records[legacyTombstoneDigest(tombstone)] = true
			if tombstone.Manifest != nil && (!historicalOnly || tombstone.Manifest.ReviewState != "" && !validDigest(tombstone.Manifest.ReviewRunID) && !tombstone.Manifest.ReviewRunCleaned) {
				records[legacyManifestDigest(*tombstone.Manifest, tombstone.InvalidatedGeneration)] = true
			}
		}
	}
	for _, effect := range state.Effects {
		legacy := effect.Reconciliation != nil && effect.Reconciliation.Reviewer != nil && !validDigest(effect.Reconciliation.Reviewer.RunID) || effect.ReviewerGateProtocol && effect.ReviewerConfinementVersion != reviewerConfinementVersion || effect.SupersededReviewerID != "" && (!validDigest(effect.SupersededReviewerRunID) || !validDigest(effect.SupersededReviewerProfileDigest) || effect.SupersededReviewerConfinementVersion != reviewerConfinementVersion)
		if ownerIssueKey(effect.Repository, effect.Issue) == issueKey && (!historicalOnly || legacy) {
			records[legacyEffectDigest(effect)] = true
		}
	}
	for _, proof := range state.ReviewerProofs {
		if ownerIssueKey(proof.Repository, proof.Issue) == issueKey && (!historicalOnly || !proof.NeverRan && (proof.LegacyUnverified || !validDigest(proof.RunID) || proof.ConfinementVersion != reviewerConfinementVersion)) {
			records[legacyReviewerDigest("proof", proof)] = true
		}
	}
	return records
}

func reconcileLegacyReviewerBoot(state *runtimeOwnerState, boot hostBootIdentity) {
	state.LegacyBootUnavailable = !boot.valid()
	// ponytail: scan the bounded ledger per quarantined issue; index evidence if startup recovery becomes slow.
	for key, diagnostic := range state.LegacyReviewerQuarantines {
		records := legacyReviewerEvidence(*state, key, false)
		evidence := legacyReviewerDigest(diagnostic, records)
		baseline, bound := state.LegacyReviewerBaselines[key]
		if !boot.valid() {
			// Preserve an existing baseline unless the evidence changed.
			if !bound || baseline.Evidence != evidence {
				if state.LegacyReviewerBaselines == nil {
					state.LegacyReviewerBaselines = map[string]legacyReviewerBaseline{}
				}
				state.LegacyReviewerBaselines[key] = legacyReviewerBaseline{Evidence: evidence}
			}
			continue
		}
		if bound && baseline.Evidence == evidence && boot.laterThan(baseline.Boot) {
			if state.LegacyReviewerReleases == nil {
				state.LegacyReviewerReleases = map[string]legacyReviewerRelease{}
			}
			for digest := range legacyReviewerEvidence(*state, key, true) {
				state.LegacyReviewerReleases[digest] = legacyReviewerRelease{Before: baseline.Boot, After: boot}
			}
			delete(state.LegacyReviewerQuarantines, key)
			delete(state.LegacyReviewerBaselines, key)
			continue
		}
		if state.LegacyReviewerBaselines == nil {
			state.LegacyReviewerBaselines = map[string]legacyReviewerBaseline{}
		}
		state.LegacyReviewerBaselines[key] = legacyReviewerBaseline{Boot: boot, Evidence: evidence}
	}
}

func validateLegacyReviewerBoot(state runtimeOwnerState) error {
	for key, baseline := range state.LegacyReviewerBaselines {
		if state.LegacyReviewerQuarantines[key] == "" || !validDigest(baseline.Evidence) || baseline.Boot != (hostBootIdentity{}) && !baseline.Boot.valid() {
			return errors.New("runtime owner legacy reviewer boot provenance is invalid")
		}
	}
	for digest, release := range state.LegacyReviewerReleases {
		if !validDigest(digest) || !release.After.laterThan(release.Before) {
			return errors.New("runtime owner legacy reviewer release is invalid")
		}
	}
	return nil
}

func legacyQuarantineDiagnostic(state runtimeOwnerState, issueKey string) string {
	diagnostic := state.LegacyReviewerQuarantines[issueKey]
	if diagnostic != "" && state.LegacyBootUnavailable {
		return "legacy reviewer quarantine remains active; verified reboot recovery unavailable: restore the host boot identity source, start to persist a baseline, then reboot and start again"
	}
	return diagnostic
}
