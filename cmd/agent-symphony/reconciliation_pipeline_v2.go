package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type reconciliationV2Collector struct {
	API    internalgithub.API
	Config internalgithub.PRAdapterConfig
	Scope  reconciliationScope
}

type reconciliationIssueUpdateMaterial struct {
	Proposal            reconciliationIssueUpdateProposal
	Config              internalgithub.PRAdapterConfig
	ControlSnapshotBody string
	Issue               *internalgithub.RecoveryIssueFact
	Attempt             *internalgithub.RecoveryAttemptFact
}

type reconciliationV2Batch struct {
	Input        reconciliationInput
	IssueUpdates []reconciliationIssueUpdateMaterial
}

type reconciliationPlannedEffect struct {
	Identity stateResultIdentity
	Request  reconciliationEffectRequest
	Material reconciliationIssueUpdateMaterial
	Attempt  *internalgithub.RecoveryAttemptFact
}

type reviewerExecutionMaterial struct {
	Issue   internalgithub.RecoveryIssueFact
	Source  string
	HeadSHA string
	Env     []string
	Command []string
}

type handoffExecutionMaterial struct{ Command []string }

type publicationExecutionMaterial struct {
	Issue                                internalgithub.RecoveryIssueFact
	Config                               internalgithub.PRAdapterConfig
	Root                                 string
	Head                                 string
	Validation, Documentation, Decisions string
}

func planReconciliationPublications(snapshot stateOwnerSnapshot, candidates []publicationExecutionMaterial) ([]reconciliationPlannedEffect, map[string]publicationExecutionMaterial, error) {
	var plans []reconciliationPlannedEffect
	material := map[string]publicationExecutionMaterial{}
	seen := map[string]bool{}
	for _, candidate := range candidates {
		key := ownerAttemptKey(candidate.Issue.Repository, candidate.Issue.Issue, candidate.Issue.Attempt)
		if seen[key] {
			return nil, nil, errors.New("duplicate publication candidate")
		}
		seen[key] = true
		record, ok := snapshot.State.Attempts[key]
		observation := snapshot.State.Observations[ownerIssueKey(candidate.Issue.Repository, candidate.Issue.Issue)]
		if !ok || !observation.Present || record.Generation != snapshot.State.AttemptGenerations[key] || digestText(candidate.Issue.Body) != observation.Fact.BodyDigest || candidate.Config.Repository != candidate.Issue.Repository || candidate.Config.ActorID < 1 || record.Manifest.State != "completed" || record.Manifest.ReviewState != "clean" || record.Manifest.ReviewHead != candidate.Head {
			continue
		}
		request := reconciliationEffectRequest{Action: reconciliationGitHubPublish, Repository: candidate.Issue.Repository, Issue: candidate.Issue.Issue, Attempt: candidate.Issue.Attempt, Manifest: ptrManifest(record.Manifest), ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, GitHubPublish: &githubPublishEffectRequest{Title: candidate.Issue.Title, BaseBranch: candidate.Issue.BaseBranch, HeadSHA: candidate.Head, Validation: candidate.Validation, Documentation: candidate.Documentation, Decisions: candidate.Decisions}}
		if recovery, recovered := snapshot.State.Recoveries[key]; recovered && recovery.State.PreparedPublication != nil {
			request.GitHubPublish.Prepared = clonePreparedPublication(recovery.State.PreparedPublication)
		} else if recovered {
			if recovery.State.HeadSHA == candidate.Head {
				continue
			}
			var prepared []internalgithub.PreparedPublication
			for _, effect := range snapshot.State.Effects {
				if effect.State != "completed" || effect.Reconciliation == nil || effect.ReconciliationResult == nil || effect.Reconciliation.Action != reconciliationHandoffDeliver || effect.Reconciliation.Handoff == nil || effect.Reconciliation.Handoff.Recovery == nil || effect.Repository != request.Repository || effect.Issue != request.Issue || effect.Attempt != request.Attempt || effect.IssueGeneration != recovery.IssueGeneration || effect.AttemptGeneration != recovery.AttemptGeneration {
					continue
				}
				handoff := *effect.Reconciliation.Handoff.Recovery
				outcome := completedHandoffPublicationOutcome(handoff, candidate.Head)
				if effect.Reconciliation.Handoff.Outcome != nil {
					outcome = cloneHandoffOutcome(*effect.Reconciliation.Handoff.Outcome)
				}
				candidatePrepared := internalgithub.PreparedPublication{Handoff: handoff, Outcome: outcome, HeadSHA: candidate.Head}
				state := clonePRState(recovery.State)
				if internalgithub.PreparePublicationState(&state, candidatePrepared) != nil {
					continue
				}
				prepared = append(prepared, candidatePrepared)
			}
			if len(prepared) > 1 {
				return nil, nil, errors.New("multiple completed handoff outcomes match publication")
			}
			if len(prepared) == 0 {
				candidatePrepared := internalgithub.PreparedPublication{Handoff: internalgithub.RecoveryHandoff{Repository: request.Repository, PR: recovery.State.Number, Issue: request.Issue, Attempt: request.Attempt, HeadSHA: recovery.State.HeadSHA}, HeadSHA: candidate.Head}
				state := clonePRState(recovery.State)
				if internalgithub.PreparePublicationState(&state, candidatePrepared) != nil {
					continue
				}
				prepared = append(prepared, candidatePrepared)
			}
			request.GitHubPublish.Prepared = &prepared[0]
		}
		request.ExecutionDigest = publicationExecutionDigest(request, candidate)
		plans = append(plans, reconciliationPlannedEffect{Identity: ownerReconciliationBeginIdentity(snapshot, request), Request: request})
		material[key] = candidate
	}
	sortReconciliationPlans(plans)
	return plans, material, nil
}

func completedHandoffPublicationOutcome(handoff internalgithub.RecoveryHandoff, head string) internalgithub.HandoffOutcome {
	outcome := internalgithub.HandoffOutcome{Key: handoff.Key}
	if handoff.Validation {
		outcome.ValidationResult = "blocked"
		outcome.ValidationEvidence = "pull request head changed to " + head + "; validation must run against the published feedback head"
	}
	for _, feedback := range handoff.Feedback {
		outcome.Feedback = append(outcome.Feedback, internalgithub.FeedbackOutcome{ID: feedback.ID, Source: feedback.Source, State: internalgithub.FeedbackAddressed, Evidence: "published in head " + head})
	}
	return outcome
}

func publicationExecutionDigest(request reconciliationEffectRequest, material publicationExecutionMaterial) string {
	request.ExecutionDigest = ""
	body, _ := json.Marshal(struct {
		Request reconciliationEffectRequest
		Config  internalgithub.PRAdapterConfig
		Root    string
		Head    string
	}{request, material.Config, material.Root, material.Head})
	return digestText(string(body))
}

func (c *runtimeEffectCoordinator) executePublication(_ context.Context, api internalgithub.API, plan reconciliationPlannedEffect, material publicationExecutionMaterial) (reconciliationEffectResult, error) {
	request := plan.Request
	if request.Action != reconciliationGitHubPublish || request.GitHubPublish == nil || material.Config.Repository != request.Repository || material.Config.ActorID < 1 || publicationExecutionDigest(request, material) != request.ExecutionDigest || material.Head != request.GitHubPublish.HeadSHA {
		return reconciliationEffectResult{}, errStateConflict
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	run, err := c.acquireKey(c.lifecycle, key, plan.Identity.IssueGeneration, plan.Identity.AttemptGeneration, request.ObservationGeneration)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	defer c.releaseKey(key, run)
	if head, err := gitSingleLine(run.ctx, material.Root, "rev-parse", request.GitHubPublish.HeadSHA+"^{commit}"); err != nil || head != request.GitHubPublish.HeadSHA {
		return reconciliationEffectResult{}, errStaleStateResult
	}
	user, err := api.AuthenticatedUser(run.ctx)
	if err != nil || user.ID != material.Config.ActorID {
		if err == nil {
			err = errors.New("authenticated GitHub actor changed")
		}
		return reconciliationEffectResult{}, err
	}
	verified, pr, err := verifyPublishedAttempt(run.ctx, api, request, user.ID)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	if err := c.owner.authorizeReconciliationEffect(run.ctx, authorizeReconciliationEffectCommand{Identity: plan.Identity, Action: request.Action}); err != nil {
		return reconciliationEffectResult{}, err
	}
	if !verified {
		if err := pushPublishedHead(run.ctx, material.Root, request.GitHubPublish.HeadSHA, request.Manifest.Branch); err != nil {
			return reconciliationEffectResult{}, err
		}
		body, _ := internalgithub.PullRequestBody(request.Issue, request.Attempt, request.GitHubPublish.Validation, request.GitHubPublish.Documentation, request.GitHubPublish.Decisions)
		mutation := internalgithub.Mutation{Issue: request.Issue, Attempt: request.Attempt}
		if pr.Number == 0 {
			pr, err = api.CreatePullRequest(run.ctx, request.Repository, request.GitHubPublish.Title, request.Manifest.Branch, request.GitHubPublish.BaseBranch, body, mutation)
			if err != nil {
				pr, _, _ = internalgithub.FindPublishedAttempt(run.ctx, api, request.Repository, request.Manifest.Branch, request.GitHubPublish.HeadSHA, user.ID)
				if pr.Number == 0 {
					return reconciliationEffectResult{}, err
				}
			}
		}
		bound, _ := internalgithub.BindPullRequestBody(body, request.Issue, request.Attempt, request.Manifest.Branch, request.GitHubPublish.HeadSHA, pr.Number)
		fresh, currentBody, err := internalgithub.FindPublishedAttempt(run.ctx, api, request.Repository, request.Manifest.Branch, request.GitHubPublish.HeadSHA, user.ID)
		if err != nil || fresh.Number != pr.Number {
			return reconciliationEffectResult{}, errors.New("pull request identity changed before binding")
		}
		if currentBody != bound {
			if err := api.UpdatePullRequest(run.ctx, request.Repository, pr.Number, bound, mutation); err != nil {
				return reconciliationEffectResult{}, err
			}
		}
		if err := api.EnsureEvidence(run.ctx, request.Repository, request.Issue, request.Attempt, request.GitHubPublish.HeadSHA, user.ID); err != nil {
			return reconciliationEffectResult{}, err
		}
		marker, _ := internalgithub.AttemptMarker(request.Issue, request.Attempt, request.Manifest.Branch, request.GitHubPublish.HeadSHA, pr.Number, "review")
		present, err := internalgithub.HasAttemptComment(run.ctx, api, request.Repository, request.Issue, marker, user.ID)
		if err != nil {
			return reconciliationEffectResult{}, err
		}
		if !present {
			comment, _ := internalgithub.AttributedBody(request.Issue, request.Attempt, "Attempt published for review.")
			if err := api.CreateIssueComment(run.ctx, request.Repository, request.Issue, comment+"\n\n"+marker, mutation); err != nil {
				return reconciliationEffectResult{}, err
			}
		}
	}
	verified, pr, err = verifyPublishedAttempt(run.ctx, api, request, user.ID)
	if err != nil || !verified {
		if err == nil {
			err = errors.New("publication aggregate proof is incomplete")
		}
		return reconciliationEffectResult{}, err
	}
	result := reconciliationEffectResult{Action: request.Action, GitHubPublish: &githubPublishEffectResult{PR: pr.Number, HeadSHA: request.GitHubPublish.HeadSHA, BoundBodyDigest: expectedPublishedBodyDigest(request, pr.Number), Evidence: true, PublishedComment: true}}
	if err := c.finishReconciliationWithMarker(plan.Identity, request, result); err != nil {
		return reconciliationEffectResult{}, err
	}
	return result, nil
}

func pushPublishedHead(ctx context.Context, root, head, branch string) error {
	gitArgs := []string{"--no-optional-locks", "-c", "core.hooksPath=/dev/null", "-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential", "-C", root, "push", "origin", head + ":refs/heads/" + branch}
	if output, err := exec.CommandContext(ctx, "git", gitArgs...).CombinedOutput(); err != nil {
		return fmt.Errorf("publish reviewed head: %w: %s", err, strings.TrimSpace(string(output)))
	}
	return nil
}

func verifyPublishedAttempt(ctx context.Context, api internalgithub.API, request reconciliationEffectRequest, actorID int) (bool, internalgithub.PullRequest, error) {
	pr, body, err := internalgithub.FindPublishedAttempt(ctx, api, request.Repository, request.Manifest.Branch, request.GitHubPublish.HeadSHA, actorID)
	if err != nil || pr.Number == 0 {
		return false, pr, err
	}
	base, _ := internalgithub.PullRequestBody(request.Issue, request.Attempt, request.GitHubPublish.Validation, request.GitHubPublish.Documentation, request.GitHubPublish.Decisions)
	bound, _ := internalgithub.BindPullRequestBody(base, request.Issue, request.Attempt, request.Manifest.Branch, request.GitHubPublish.HeadSHA, pr.Number)
	if body != bound {
		return false, pr, nil
	}
	for _, kind := range []string{"validation", "documentation"} {
		evidence, _ := internalgithub.EvidenceBody(request.Issue, request.Attempt, kind, request.GitHubPublish.HeadSHA)
		present, err := internalgithub.HasAttemptComment(ctx, api, request.Repository, request.Issue, evidence, actorID)
		if err != nil || !present {
			return false, pr, err
		}
	}
	marker, _ := internalgithub.AttemptMarker(request.Issue, request.Attempt, request.Manifest.Branch, request.GitHubPublish.HeadSHA, pr.Number, "review")
	present, err := internalgithub.HasAttemptComment(ctx, api, request.Repository, request.Issue, marker, actorID)
	return present, pr, err
}

func planReconciliationHandoffs(snapshot stateOwnerSnapshot, command, humanInstructions []string) ([]reconciliationPlannedEffect, map[string]handoffExecutionMaterial, error) {
	var plans []reconciliationPlannedEffect
	material := map[string]handoffExecutionMaterial{}
	for key, record := range snapshot.State.Attempts {
		manifest := record.Manifest
		observation, ok := snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)]
		if !ok || !observation.Present || record.Generation != snapshot.State.AttemptGenerations[key] {
			continue
		}
		expanded, err := config.ExpandManagedWorkspace(command, manifest.Worktree)
		if err != nil {
			return nil, nil, err
		}
		var handoff *handoffEffectRequest
		if manifest.ReviewState == "findings-queued" && !manifest.ReviewHandoffAck {
			reviewKey := "independent-review-" + manifest.ReviewHead
			handoff = &handoffEffectRequest{Kind: "review-findings", HeadSHA: manifest.ReviewHead, Key: reviewKey, Findings: slices.Clone(manifest.ReviewFindings), HumanInstructions: slices.Clone(humanInstructions), OutcomePath: handoffReceiptPath(manifest.Worktree, reviewKey), OutcomeToken: manifest.ReviewHead}
		} else if recovery, ok := snapshot.State.Recoveries[key]; ok {
			candidate := clonePRState(recovery.State)
			claimed, runnable, err := internalgithub.ClaimHandoffState(&candidate)
			if err != nil {
				return nil, nil, err
			}
			if runnable {
				token := digestText("handoff-outcome\x00" + claimed.Key)
				handoff = &handoffEffectRequest{Kind: "recovery", Key: claimed.Key, Recovery: &claimed, OutcomePath: handoffReceiptPath(manifest.Worktree, claimed.Key), OutcomeToken: token}
			}
		}
		if handoff == nil {
			continue
		}
		request := reconciliationEffectRequest{Action: reconciliationHandoffDeliver, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Manifest: ptrManifest(manifest), ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, Handoff: handoff}
		value := handoffExecutionMaterial{Command: expanded}
		request.ExecutionDigest = handoffExecutionDigest(request, value)
		plans = append(plans, reconciliationPlannedEffect{Identity: ownerReconciliationBeginIdentity(snapshot, request), Request: request})
		material[key] = value
	}
	sortReconciliationPlans(plans)
	return plans, material, nil
}

func handoffExecutionDigest(request reconciliationEffectRequest, material handoffExecutionMaterial) string {
	request.ExecutionDigest = ""
	body, _ := json.Marshal(struct {
		Request reconciliationEffectRequest
		Command []string
	}{request, material.Command})
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func (c *runtimeEffectCoordinator) executeHandoff(_ context.Context, boundary boundaryCaller, plan reconciliationPlannedEffect, material handoffExecutionMaterial) (reconciliationEffectResult, error) {
	request := plan.Request
	if request.Action != reconciliationHandoffDeliver || request.Handoff == nil || handoffExecutionDigest(request, material) != request.ExecutionDigest {
		return reconciliationEffectResult{}, errStateConflict
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	run, err := c.acquireKey(c.lifecycle, key, plan.Identity.IssueGeneration, plan.Identity.AttemptGeneration, request.ObservationGeneration)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	defer c.releaseKey(key, run)
	payload, err := handoffBoundaryPayload(request, material.Command)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	applied, err := verifyHandoffBoundary(run.ctx, boundary, payload, request.Handoff)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	if err := c.owner.authorizeReconciliationEffect(run.ctx, authorizeReconciliationEffectCommand{Identity: plan.Identity, Action: request.Action}); err != nil {
		return reconciliationEffectResult{}, err
	}
	if !applied {
		accepted, err := boundary.call(run.ctx, "accept-handoff", agentruntime.Command{Stdin: bytes.NewReader(payload)})
		if err != nil || !validHandoffAck(accepted.Output, request.Handoff) {
			if err == nil {
				err = errors.New("handoff acceptance binding mismatch")
			}
			return reconciliationEffectResult{}, err
		}
	}
	applied, err = verifyHandoffBoundary(run.ctx, boundary, payload, request.Handoff)
	if err != nil || !applied {
		if err == nil {
			err = errors.New("handoff was not observable")
		}
		return reconciliationEffectResult{}, err
	}
	result := reconciliationEffectResult{Action: request.Action, Handoff: &handoffEffectResult{Kind: request.Handoff.Kind, Key: request.Handoff.Key, OutcomePath: request.Handoff.OutcomePath, OutcomeToken: request.Handoff.OutcomeToken, Observed: true}}
	if err := c.finishReconciliationWithMarker(plan.Identity, request, result); err != nil {
		return reconciliationEffectResult{}, err
	}
	return result, nil
}

func handoffBoundaryPayload(request reconciliationEffectRequest, command []string) ([]byte, error) {
	var handoff []byte
	if request.Handoff.Kind == "review-findings" {
		handoff, _ = json.Marshal(struct {
			Type, Key, Findings string
			HumanInstructions   []string `json:"human_instructions,omitempty"`
		}{"agent-symphony-handoff-v1", request.Handoff.Key, strings.Join(request.Handoff.Findings, "\n"), request.Handoff.HumanInstructions})
	} else {
		value := request.Handoff.Recovery
		handoff, _ = json.Marshal(struct {
			Type, Key  string
			PR         int
			HeadSHA    string
			Validation bool
			Feedback   []internalgithub.Feedback
		}{"agent-symphony-handoff-v1", value.Key, value.PR, value.HeadSHA, value.Validation, value.Feedback})
	}
	body, err := json.Marshal(struct {
		Manifest     agentruntime.Manifest `json:"manifest"`
		Handoff      json.RawMessage       `json:"handoff"`
		OutcomePath  string                `json:"outcome_path"`
		OutcomeToken string                `json:"outcome_token"`
		Command      []string              `json:"command"`
	}{*request.Manifest, handoff, request.Handoff.OutcomePath, request.Handoff.OutcomeToken, command})
	return body, err
}

func verifyHandoffBoundary(ctx context.Context, boundary boundaryCaller, payload []byte, request *handoffEffectRequest) (bool, error) {
	result, err := boundary.call(ctx, "verify-handoff", agentruntime.Command{Stdin: bytes.NewReader(payload)})
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(result.Output) == "" {
		return false, nil
	}
	return validHandoffAck(result.Output, request), nil
}

func validHandoffAck(output string, request *handoffEffectRequest) bool {
	var ack handoffReceipt
	decoder := json.NewDecoder(strings.NewReader(output))
	decoder.DisallowUnknownFields()
	return decoder.Decode(&ack) == nil && decoder.Decode(&struct{}{}) == io.EOF && ack.Type == "agent-symphony-handoff-executed-v1" && ack.Key == request.Key && ack.OutcomePath == request.OutcomePath && ack.OutcomeToken == request.OutcomeToken
}

func planReconciliationReviewers(snapshot stateOwnerSnapshot, stateRoot string, candidates []reviewerExecutionMaterial) ([]reconciliationPlannedEffect, map[string]reviewerExecutionMaterial, error) {
	byAttempt := make(map[string]reviewerExecutionMaterial, len(candidates))
	for _, candidate := range candidates {
		key := ownerAttemptKey(candidate.Issue.Repository, candidate.Issue.Issue, candidate.Issue.Attempt)
		if _, duplicate := byAttempt[key]; duplicate {
			return nil, nil, errors.New("duplicate reviewer candidate")
		}
		byAttempt[key] = candidate
	}
	plans, material := []reconciliationPlannedEffect{}, map[string]reviewerExecutionMaterial{}
	for key, record := range snapshot.State.Attempts {
		manifest, candidate := record.Manifest, byAttempt[key]
		issue := candidate.Issue
		observation, ok := snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)]
		if !ok || !observation.Present || record.Generation != snapshot.State.AttemptGenerations[key] || issue.Repository != manifest.Repository || digestText(issue.Body) != observation.Fact.BodyDigest {
			continue
		}
		phase, mode, target, base, head := "run-observe", manifest.ReviewMode, manifest.ReviewTarget, manifest.ReviewBase, manifest.ReviewHead
		snapshotPath, session := reviewIdentity(agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt}, productionSnapshotRoot(stateRoot))
		if manifest.ReviewState == "clean" || manifest.ReviewState == "findings-queued" {
			phase, mode, target, base, head, snapshotPath, session = "cleanup", manifest.ReviewMode, manifest.ReviewTarget, manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewSnapshot, manifest.ReviewSession
		} else if manifest.ReviewState == "preparing" || manifest.ReviewState == "running" {
			expected := reviewerEffectRequest{Mode: mode, Target: target, BaseSHA: base, HeadSHA: head, Snapshot: snapshotPath, Session: session}
			if !reviewManifestMatches(manifest, &expected) {
				continue
			}
		} else if manifest.State == "completed" && manifest.ReviewState == "" && validOptionalObjectID(candidate.HeadSHA) && candidate.HeadSHA != "" && candidate.HeadSHA != manifest.BaseSHA {
			mode, base, head, target = agentruntime.ReviewModeImplementation, manifest.BaseSHA, candidate.HeadSHA, manifest.BaseSHA+".."+candidate.HeadSHA
		} else {
			continue
		}
		request := reconciliationEffectRequest{Action: reconciliationReviewer, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Manifest: ptrManifest(manifest), ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, Reviewer: &reviewerEffectRequest{Phase: phase, Mode: mode, Target: target, BaseSHA: base, HeadSHA: head, Snapshot: snapshotPath, Session: session}}
		value := candidate
		value.Env, value.Command = slices.Clone(candidate.Env), slices.Clone(candidate.Command)
		request.ExecutionDigest = reviewerExecutionDigest(request, value)
		plan := reconciliationPlannedEffect{Identity: ownerReconciliationBeginIdentity(snapshot, request), Request: request}
		plans = append(plans, plan)
		material[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)] = value
	}
	sortReconciliationPlans(plans)
	return plans, material, nil
}

func digestText(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func reviewerExecutionDigest(request reconciliationEffectRequest, material reviewerExecutionMaterial) string {
	request.ExecutionDigest = ""
	body, _ := json.Marshal(struct {
		Request reconciliationEffectRequest
		Issue   internalgithub.RecoveryIssueFact
		Source  string
		HeadSHA string
		Env     []string
		Command []string
	}{request, material.Issue, material.Source, material.HeadSHA, material.Env, material.Command})
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func (c *runtimeEffectCoordinator) executeReviewer(_ context.Context, boundary boundaryCaller, plan reconciliationPlannedEffect, material reviewerExecutionMaterial) (reconciliationEffectResult, bool, error) {
	return c.executeReviewerMode(boundary, plan, material, false)
}

func (c *runtimeEffectCoordinator) executeOperatorReviewer(boundary boundaryCaller, plan reconciliationPlannedEffect, material reviewerExecutionMaterial) (reconciliationEffectResult, bool, error) {
	return c.executeReviewerMode(boundary, plan, material, true)
}

func (c *runtimeEffectCoordinator) executeReviewerMode(boundary boundaryCaller, plan reconciliationPlannedEffect, material reviewerExecutionMaterial, operator bool) (reconciliationEffectResult, bool, error) {
	request := plan.Request
	if request.Action != reconciliationReviewer || request.Reviewer == nil || reviewerExecutionDigest(request, material) != request.ExecutionDigest || digestText(material.Issue.Body) != request.BodyDigest {
		return reconciliationEffectResult{}, false, errStateConflict
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	run, err := c.acquireKey(c.lifecycle, key, plan.Identity.IssueGeneration, plan.Identity.AttemptGeneration, request.ObservationGeneration)
	if err != nil {
		return reconciliationEffectResult{}, false, err
	}
	defer c.releaseKey(key, run)
	if err := c.owner.authorizeReconciliationEffect(run.ctx, authorizeReconciliationEffectCommand{Identity: plan.Identity, Action: request.Action}); err != nil {
		return reconciliationEffectResult{}, false, err
	}
	attempt := agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt, BaseSHA: request.Manifest.BaseSHA}
	if request.Reviewer.Phase == "cleanup" {
		if err := cleanupReviewResources(run.ctx, boundary, material.Env, attempt, request.Reviewer.HeadSHA, request.Reviewer.Target, request.Reviewer.Snapshot, request.Reviewer.Session, productionSnapshotRoot(c.owner.stateRoot)); err != nil {
			return reconciliationEffectResult{}, false, err
		}
		session, sessionErr := boundary.call(run.ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"has-session", "-t", "=" + request.Reviewer.Session}, Dir: filepath.Dir(request.Reviewer.Snapshot), Env: material.Env})
		if !session.Exited || session.Code != 1 {
			if sessionErr == nil {
				sessionErr = errors.New("reviewer session remains after cleanup")
			}
			return reconciliationEffectResult{}, false, sessionErr
		}
		for _, path := range []string{request.Reviewer.Snapshot, filepath.Dir(reviewResultPath(request.Reviewer.Snapshot, request.Reviewer.Target))} {
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				return reconciliationEffectResult{}, false, errors.New("reviewer resources remain after cleanup")
			}
		}
		result := reconciliationEffectResult{Action: request.Action, Reviewer: &reviewerEffectResult{Phase: request.Reviewer.Phase, Status: "cleaned", Mode: request.Reviewer.Mode, Target: request.Reviewer.Target, BaseSHA: request.Reviewer.BaseSHA, HeadSHA: request.Reviewer.HeadSHA, Snapshot: request.Reviewer.Snapshot, Session: request.Reviewer.Session}}
		if err := c.finishReconciliationMarker(plan.Identity, request, result, operator); err != nil {
			return reconciliationEffectResult{}, false, err
		}
		return result, false, nil
	}
	executionManifest := cloneManifest(*request.Manifest)
	executionManifest.ReviewState, executionManifest.ReviewMode, executionManifest.ReviewTarget = "running", request.Reviewer.Mode, request.Reviewer.Target
	executionManifest.ReviewBase, executionManifest.ReviewHead = request.Reviewer.BaseSHA, request.Reviewer.HeadSHA
	executionManifest.ReviewSnapshot, executionManifest.ReviewSession = request.Reviewer.Snapshot, request.Reviewer.Session
	review, pending, err := runIndependentReviewV2(run.ctx, attempt, boundary, material.Env, material.Command, material.Issue, executionManifest, material.Source, request.Reviewer.HeadSHA, productionSnapshotRoot(c.owner.stateRoot), request.Reviewer.Mode)
	if err != nil || pending {
		return reconciliationEffectResult{}, pending, err
	}
	result := reconciliationEffectResult{Action: request.Action, Reviewer: &reviewerEffectResult{Phase: request.Reviewer.Phase, Status: review.Status, Mode: request.Reviewer.Mode, Target: request.Reviewer.Target, BaseSHA: request.Reviewer.BaseSHA, HeadSHA: request.Reviewer.HeadSHA, Snapshot: request.Reviewer.Snapshot, Session: request.Reviewer.Session, Findings: slices.Clone(review.Findings)}}
	if err := c.finishReconciliationMarker(plan.Identity, request, result, operator); err != nil {
		return reconciliationEffectResult{}, false, err
	}
	return result, false, nil
}

func planReconciliationBinds(snapshot stateOwnerSnapshot, cfg internalgithub.PRAdapterConfig) ([]reconciliationPlannedEffect, error) {
	if cfg.Repository == "" || cfg.Repository != snapshot.State.Repository || cfg.ActorID < 1 {
		return nil, errors.New("GitHub bind config does not match owner")
	}
	var plans []reconciliationPlannedEffect
	for key, record := range snapshot.State.Attempts {
		manifest := record.Manifest
		observation, ok := snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)]
		if !ok || !observation.Present || observation.OwnerGeneration != snapshot.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)] || record.Generation != snapshot.State.AttemptGenerations[key] || manifest.State != "preparing" || !observation.Fact.DispatchAuthorized || observation.Fact.Attempt != manifest.Attempt || observation.Fact.BaseSHA != manifest.BaseSHA {
			continue
		}
		detail := "Implementation session reserved.\n\n- Project: `" + manifest.Repository + "`\n- Branch: `" + manifest.Branch + "`\n- Worktree: `" + manifest.Worktree + "`\n- Session: `" + manifest.Session + "`"
		request := reconciliationEffectRequest{Action: reconciliationGitHubBind, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Manifest: ptrManifest(manifest), ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, GitHubBind: &githubBindEffectRequest{BaseSHA: manifest.BaseSHA, Branch: manifest.Branch, Detail: detail}}
		material := reconciliationIssueUpdateMaterial{Config: cfg}
		request.ExecutionDigest = githubBindExecutionDigest(request, cfg)
		plans = append(plans, reconciliationPlannedEffect{Identity: ownerReconciliationBeginIdentity(snapshot, request), Request: request, Material: material})
	}
	sortReconciliationPlans(plans)
	return plans, nil
}

func planReconciliationRetirements(snapshot stateOwnerSnapshot) []reconciliationPlannedEffect {
	var plans []reconciliationPlannedEffect
	for key, record := range snapshot.State.Attempts {
		manifest := record.Manifest
		observation, ok := snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)]
		fact, observed := observedReconciliationAttempt(observation, manifest.Attempt)
		if !ok || !observed || record.Generation != snapshot.State.AttemptGenerations[key] || fact.State != "completed" || fact.PR < 1 || fact.BaseSHA != manifest.BaseSHA || fact.HeadSHA == "" || manifest.ReviewHead != fact.HeadSHA || manifest.ReviewSnapshot != "" || manifest.ReviewSession != "" {
			continue
		}
		mode := "cleanup"
		if manifest.State == "preparing" || manifest.State == "running" {
			mode = "abandon"
		}
		request := reconciliationEffectRequest{Action: reconciliationRetireCompleted, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Manifest: ptrManifest(manifest), ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, Retire: &retireCompletedEffectRequest{Mode: mode, HeadSHA: fact.HeadSHA}}
		request.ExecutionDigest = retirementExecutionDigest(request)
		plans = append(plans, reconciliationPlannedEffect{Identity: ownerReconciliationBeginIdentity(snapshot, request), Request: request})
	}
	sortReconciliationPlans(plans)
	return plans
}

func (c *runtimeEffectCoordinator) executeRetirement(_ context.Context, boundary boundaryCaller, plan reconciliationPlannedEffect) (reconciliationEffectResult, error) {
	request := plan.Request
	if request.Action != reconciliationRetireCompleted || request.Retire == nil || retirementExecutionDigest(request) != request.ExecutionDigest {
		return reconciliationEffectResult{}, errStateConflict
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	run, err := c.acquireKey(c.lifecycle, key, plan.Identity.IssueGeneration, plan.Identity.AttemptGeneration, request.ObservationGeneration)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	defer c.releaseKey(key, run)
	gone, err := retiredResourcesGone(run.ctx, boundary, *request.Manifest, c.owner.stateRoot)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	if err := c.owner.authorizeReconciliationEffect(run.ctx, authorizeReconciliationEffectCommand{Identity: plan.Identity, Action: request.Action}); err != nil {
		return reconciliationEffectResult{}, err
	}
	if !gone {
		body, _ := json.Marshal(*request.Manifest)
		if _, err := boundary.call(run.ctx, request.Retire.Mode, agentruntime.Command{Stdin: bytes.NewReader(body)}); err != nil {
			return reconciliationEffectResult{}, err
		}
	}
	gone, err = retiredResourcesGone(run.ctx, boundary, *request.Manifest, c.owner.stateRoot)
	if err != nil || !gone {
		if err == nil {
			err = errors.New("attempt resources remain after retirement")
		}
		return reconciliationEffectResult{}, err
	}
	result := reconciliationEffectResult{Action: request.Action, Retire: &retireCompletedEffectResult{ResourcesGone: true}}
	if err := c.finishReconciliationWithMarker(plan.Identity, request, result); err != nil {
		return reconciliationEffectResult{}, err
	}
	return result, nil
}

func retirementExecutionDigest(request reconciliationEffectRequest) string {
	request.ExecutionDigest = ""
	return reconciliationEffectDigest(request)
}

func retiredResourcesGone(ctx context.Context, boundary boundaryCaller, manifest agentruntime.Manifest, stateRoot string) (bool, error) {
	for _, path := range []string{manifest.Worktree, agentruntime.ResultPath(manifest.Worktree)} {
		if _, err := os.Lstat(path); err == nil {
			return false, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	result, err := boundary.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"has-session", "-t", "=" + manifest.Session}, Dir: stateRoot})
	if result.Exited && result.Code == 1 {
		return true, nil
	}
	if err == nil {
		err = errors.New("attempt session remains")
	}
	return false, err
}

func githubBindExecutionDigest(request reconciliationEffectRequest, cfg internalgithub.PRAdapterConfig) string {
	request.ExecutionDigest = ""
	body, _ := json.Marshal(struct {
		Request reconciliationEffectRequest
		Config  internalgithub.PRAdapterConfig
	}{request, cfg})
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func (c *runtimeEffectCoordinator) executeGitHubBind(_ context.Context, api internalgithub.API, plan reconciliationPlannedEffect) (reconciliationEffectResult, error) {
	request, cfg := plan.Request, plan.Material.Config
	if request.Action != reconciliationGitHubBind || request.GitHubBind == nil || githubBindExecutionDigest(request, cfg) != request.ExecutionDigest {
		return reconciliationEffectResult{}, errStateConflict
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	run, err := c.acquireKey(c.lifecycle, key, plan.Identity.IssueGeneration, plan.Identity.AttemptGeneration, request.ObservationGeneration)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	defer c.releaseKey(key, run)
	present, err := observeGitHubBind(run.ctx, api, cfg, request)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	if err := c.owner.authorizeReconciliationEffect(run.ctx, authorizeReconciliationEffectCommand{Identity: plan.Identity, Action: request.Action}); err != nil {
		return reconciliationEffectResult{}, err
	}
	if !present {
		if err := internalgithub.EnsureActiveAttempt(run.ctx, api, cfg, request.Issue, request.Attempt, request.GitHubBind.BaseSHA, request.GitHubBind.Detail); err != nil {
			return reconciliationEffectResult{}, err
		}
	}
	present, err = observeGitHubBind(run.ctx, api, cfg, request)
	if err != nil || !present {
		if err == nil {
			err = errors.New("GitHub bind was not observable")
		}
		return reconciliationEffectResult{}, err
	}
	result := reconciliationEffectResult{Action: request.Action, GitHubBind: &githubBindEffectResult{Observed: true}}
	if err := c.finishReconciliationWithMarker(plan.Identity, request, result); err != nil {
		return reconciliationEffectResult{}, err
	}
	return result, nil
}

func observeGitHubBind(ctx context.Context, api internalgithub.API, cfg internalgithub.PRAdapterConfig, request reconciliationEffectRequest) (bool, error) {
	return internalgithub.ActiveAttemptPresent(ctx, api, cfg, request.Issue, request.Attempt, request.GitHubBind.BaseSHA)
}

func planReconciliationGovernance(snapshot stateOwnerSnapshot, cfg internalgithub.PRAdapterConfig) ([]reconciliationPlannedEffect, error) {
	if cfg.Repository == "" || cfg.Repository != snapshot.State.Repository {
		return nil, errors.New("PR governance config does not match owner")
	}
	var plans []reconciliationPlannedEffect
	for issueKey, observation := range snapshot.State.Observations {
		if !observation.Present || observation.OwnerGeneration != snapshot.State.IssueGenerations[issueKey] {
			continue
		}
		for attemptKey, accepted := range observation.Attempts {
			if !accepted.Present || accepted.SourceIssueGeneration != observation.Generation || accepted.OwnerGeneration != snapshot.State.AttemptGenerations[attemptKey] {
				continue
			}
			fact := accepted.Fact
			record, owned := snapshot.State.Attempts[attemptKey]
			if !owned || (fact.State != "active" && fact.State != "review-ready") || !fact.PublicationConfirmed || fact.PR < 1 || fact.HeadSHA == "" || fact.BaseSHA != record.Manifest.BaseSHA {
				continue
			}
			remote := expandAttemptFact(fact)
			request := reconciliationEffectRequest{Action: reconciliationGitHubPRGovernance, Repository: fact.Repository, Issue: fact.Issue, Attempt: fact.Attempt, Manifest: ptrManifest(record.Manifest), ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, GitHubPRGovernance: &githubGovernanceEffectRequest{PR: fact.PR, HeadSHA: fact.HeadSHA, Policy: cfg}}
			request.ExecutionDigest = governanceExecutionDigest(request, remote)
			plans = append(plans, reconciliationPlannedEffect{Identity: ownerReconciliationBeginIdentity(snapshot, request), Request: request, Attempt: &remote})
		}
	}
	sortReconciliationPlans(plans)
	return plans, nil
}

func planReconciliationAttemptIssueUpdates(snapshot stateOwnerSnapshot, batch reconciliationV2Batch, cfg internalgithub.PRAdapterConfig) ([]reconciliationPlannedEffect, error) {
	if cfg.Repository == "" || cfg.Repository != snapshot.State.Repository || cfg.ActorID < 1 {
		return nil, errors.New("GitHub issue update config does not match owner")
	}
	issues := map[string]internalgithub.RecoveryIssueFact{}
	for _, issue := range batch.Input.Issues {
		key := ownerIssueKey(issue.Repository, issue.Issue)
		if _, duplicate := issues[key]; duplicate {
			return nil, errStateConflict
		}
		issues[key] = issue
	}
	attempts := map[string]internalgithub.RecoveryAttemptFact{}
	for _, attempt := range batch.Input.Attempts {
		key := ownerAttemptKey(attempt.Repository, attempt.Issue, attempt.Attempt)
		if _, duplicate := attempts[key]; duplicate {
			return nil, errStateConflict
		}
		attempts[key] = attempt
	}
	var plans []reconciliationPlannedEffect
	for key, record := range snapshot.State.Attempts {
		manifest := record.Manifest
		observation, ok := snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)]
		issue, supplied := issues[ownerIssueKey(manifest.Repository, manifest.Issue)]
		if !ok || !supplied || !observation.Present || record.Generation != snapshot.State.AttemptGenerations[key] || digestText(issue.Body) != observation.Fact.BodyDigest {
			continue
		}
		var update *githubIssueUpdateEffectRequest
		remote, remotelyObserved := observedReconciliationAttempt(observation, manifest.Attempt)
		switch {
		case manifest.State == "failed" || manifest.State == "cancelled":
			terminal := slices.ContainsFunc(observation.Fact.TerminalAttempts, func(attempt reconciliationAttemptFact) bool {
				return attempt.Attempt == manifest.Attempt
			})
			kind := githubIssueTerminalFailure
			if terminal && observation.Fact.RecoveryAuthorized && !observation.Fact.Retry {
				kind = githubIssueRetry
			} else if terminal {
				continue
			}
			update = &githubIssueUpdateEffectRequest{Kind: kind, FailedAtUnixNano: manifest.UpdatedAt.UnixNano()}
			if kind == githubIssueTerminalFailure {
				update.Diagnostic = internalgithub.Redact(manifest.Diagnostic)
				if update.Diagnostic == "" {
					update.Diagnostic = "attempt failed closed"
				}
			}
		case manifest.ReviewState == "findings-queued" && !manifest.ReviewHandoffQueued && !manifest.ReviewHandoffAck:
			update = &githubIssueUpdateEffectRequest{Kind: githubIssueFindings, HeadSHA: manifest.ReviewHead, Findings: slices.Clone(manifest.ReviewFindings)}
		case manifest.State == "completed" && remotelyObserved && remote.PR > 0 && remote.HeadSHA == manifest.ReviewHead:
			update = &githubIssueUpdateEffectRequest{Kind: githubIssueEvidence, HeadSHA: manifest.ReviewHead}
		default:
			continue
		}
		request := reconciliationEffectRequest{Action: reconciliationGitHubIssueUpdate, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Manifest: ptrManifest(manifest), ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, GitHubIssueUpdate: update}
		material := reconciliationIssueUpdateMaterial{Config: cfg, Issue: ptrRecoveryIssue(issue)}
		if attempt, exists := attempts[key]; exists {
			material.Attempt = ptrRecoveryAttempt(attempt)
		}
		request.ExecutionDigest = issueUpdateExecutionDigest(request, material)
		if !validReconciliationEffectRequest(snapshot.State.Repository, request) || !validReconciliationEffectStateBindings("", snapshot.State, request) {
			continue
		}
		if completedReconciliationRequest(snapshot.State, request) {
			continue
		}
		plans = append(plans, reconciliationPlannedEffect{Identity: ownerReconciliationBeginIdentity(snapshot, request), Request: request, Material: material})
	}
	sortReconciliationPlans(plans)
	return plans, nil
}

func sortReconciliationPlans(plans []reconciliationPlannedEffect) {
	slices.SortFunc(plans, func(a, b reconciliationPlannedEffect) int {
		if ordered := cmp.Compare(a.Request.Issue, b.Request.Issue); ordered != 0 {
			return ordered
		}
		if ordered := cmp.Compare(a.Request.Attempt, b.Request.Attempt); ordered != 0 {
			return ordered
		}
		if ordered := cmp.Compare(a.Request.Action, b.Request.Action); ordered != 0 {
			return ordered
		}
		return cmp.Compare(a.Request.ExecutionDigest, b.Request.ExecutionDigest)
	})
}

func ptrRecoveryIssue(issue internalgithub.RecoveryIssueFact) *internalgithub.RecoveryIssueFact {
	copy := issue
	return &copy
}

func ptrRecoveryAttempt(attempt internalgithub.RecoveryAttemptFact) *internalgithub.RecoveryAttemptFact {
	copy := attempt
	return &copy
}

func completedReconciliationRequest(state runtimeOwnerState, request reconciliationEffectRequest) bool {
	digest := reconciliationEffectDigest(request)
	for _, effect := range state.Effects {
		if effect.State == "completed" && effect.Reconciliation != nil && effect.RequestDigest == digest {
			return true
		}
	}
	return false
}

func expandAttemptFact(fact reconciliationAttemptFact) internalgithub.RecoveryAttemptFact {
	return internalgithub.RecoveryAttemptFact{Repository: fact.Repository, Issue: fact.Issue, Attempt: fact.Attempt, PR: fact.PR, BaseSHA: fact.BaseSHA, HeadSHA: fact.HeadSHA, State: fact.State, PublicationConfirmed: fact.PublicationConfirmed, Diagnostic: fact.Diagnostic, Checks: slices.Clone(fact.Checks)}
}

func ptrManifest(manifest agentruntime.Manifest) *agentruntime.Manifest {
	clone := cloneManifest(manifest)
	return &clone
}

func governanceExecutionDigest(request reconciliationEffectRequest, attempt internalgithub.RecoveryAttemptFact) string {
	request.ExecutionDigest = ""
	body, _ := json.Marshal(struct {
		Request reconciliationEffectRequest
		Attempt internalgithub.RecoveryAttemptFact
	}{request, attempt})
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func (c *runtimeEffectCoordinator) executeGovernance(ctx context.Context, api internalgithub.API, plan reconciliationPlannedEffect) (reconciliationEffectResult, error) {
	request := plan.Request
	if request.Action != reconciliationGitHubPRGovernance || request.GitHubPRGovernance == nil || plan.Attempt == nil || governanceExecutionDigest(request, *plan.Attempt) != request.ExecutionDigest {
		return reconciliationEffectResult{}, errStateConflict
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	run, err := c.acquireKey(c.lifecycle, key, plan.Identity.IssueGeneration, plan.Identity.AttemptGeneration, request.ObservationGeneration)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	defer c.releaseKey(key, run)
	fresh, err := internalgithub.FetchAttemptFacts(run.ctx, api, request.Repository, request.GitHubPRGovernance.Policy.ActorID)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	if !slices.ContainsFunc(fresh, func(candidate internalgithub.RecoveryAttemptFact) bool {
		return reflect.DeepEqual(candidate, *plan.Attempt)
	}) {
		return reconciliationEffectResult{}, errStaleStateResult
	}
	if err := c.owner.authorizeReconciliationEffect(run.ctx, authorizeReconciliationEffectCommand{Identity: plan.Identity, Action: request.Action}); err != nil {
		return reconciliationEffectResult{}, err
	}
	if err := internalgithub.RunPRGovernance(run.ctx, api, request.GitHubPRGovernance.Policy, ownerAttemptRecovery{owner: c.owner, identity: plan.Identity}, *plan.Attempt); err != nil {
		return reconciliationEffectResult{}, err
	}
	result := reconciliationEffectResult{Action: request.Action, GitHubPRGovernance: &githubGovernanceEffectResult{PR: request.GitHubPRGovernance.PR, HeadSHA: request.GitHubPRGovernance.HeadSHA, Observed: true}}
	if err := c.finishReconciliationWithMarker(plan.Identity, request, result); err != nil {
		return reconciliationEffectResult{}, err
	}
	return result, nil
}

func (p reconciliationPlannedEffect) issueUpdateProposal() internalgithub.IssueUpdateProposal {
	return internalgithub.IssueUpdateProposal{
		Kind:                internalgithub.IssueUpdateProposalKind(p.Material.Proposal.Kind),
		Repository:          p.Request.Repository,
		Issue:               p.Request.Issue,
		AttributionAttempt:  p.Request.GitHubIssueUpdate.AttributionAttempt,
		Dependency:          p.Request.GitHubIssueUpdate.Dependency,
		PullRequest:         p.Request.GitHubIssueUpdate.PullRequest,
		ControlSnapshotBody: p.Material.ControlSnapshotBody,
	}
}

func (c *runtimeEffectCoordinator) beginReconciliation(ctx context.Context, plan reconciliationPlannedEffect) (reconciliationPlannedEffect, error) {
	if c == nil || c.owner == nil {
		return reconciliationPlannedEffect{}, errors.New("runtime effect coordinator is unavailable")
	}
	_, effect, err := c.owner.beginReconciliationEffect(ctx, beginReconciliationEffectCommand{Identity: plan.Identity, Request: plan.Request})
	if err != nil {
		return reconciliationPlannedEffect{}, err
	}
	plan.Identity = ownerReconciliationEffectIdentity(*effect)
	return plan, nil
}

func (c *runtimeEffectCoordinator) executeIssueUpdate(_ context.Context, api internalgithub.API, plan reconciliationPlannedEffect) (reconciliationEffectResult, error) {
	return c.executeIssueUpdateMode(api, plan, false)
}

func (c *runtimeEffectCoordinator) executeOperatorIssueUpdate(api internalgithub.API, plan reconciliationPlannedEffect) (reconciliationEffectResult, error) {
	return c.executeIssueUpdateMode(api, plan, true)
}

func (c *runtimeEffectCoordinator) executeIssueUpdateMode(api internalgithub.API, plan reconciliationPlannedEffect, operator bool) (reconciliationEffectResult, error) {
	request := plan.Request
	if request.Action != reconciliationGitHubIssueUpdate || request.GitHubIssueUpdate == nil || issueUpdateExecutionDigest(request, plan.Material) != request.ExecutionDigest {
		return reconciliationEffectResult{}, errStateConflict
	}
	issueScoped := reconciliationEffectIssueScoped(request)
	key, attemptGeneration := ownerIssueKey(request.Repository, request.Issue), uint64(0)
	if !issueScoped {
		key, attemptGeneration = ownerAttemptKey(request.Repository, request.Issue, request.Attempt), plan.Identity.AttemptGeneration
	}
	run, err := c.acquireKey(c.lifecycle, key, plan.Identity.IssueGeneration, attemptGeneration, request.ObservationGeneration)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	defer c.releaseKey(key, run)
	var applied bool
	if issueScoped {
		applied, err = internalgithub.RevalidateIssueUpdateProposal(run.ctx, api, plan.Material.Config, plan.issueUpdateProposal())
	} else {
		applied, err = revalidateAttemptIssueUpdate(run.ctx, api, request, plan.Material)
	}
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	if err := c.owner.authorizeReconciliationEffect(run.ctx, authorizeReconciliationEffectCommand{Identity: plan.Identity, Action: request.Action}); err != nil {
		return reconciliationEffectResult{}, err
	}
	if !applied {
		if issueScoped {
			err = internalgithub.ExecuteIssueUpdateProposal(run.ctx, api, plan.Material.Config, plan.issueUpdateProposal())
		} else {
			err = executeAttemptIssueUpdate(run.ctx, api, request, plan.Material.Config)
		}
		if err != nil {
			return reconciliationEffectResult{}, err
		}
	}
	if !issueScoped {
		applied, err = attemptIssueUpdateApplied(run.ctx, api, request, plan.Material.Config)
		if err != nil || !applied {
			if err == nil {
				err = errors.New("GitHub issue update was not observable")
			}
			return reconciliationEffectResult{}, err
		}
	}
	result := reconciliationEffectResult{Action: request.Action, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: request.GitHubIssueUpdate.Kind, Observed: true}}
	if err := c.finishReconciliationMarker(plan.Identity, request, result, operator); err != nil {
		return reconciliationEffectResult{}, err
	}
	return result, nil
}

func revalidateAttemptIssueUpdate(ctx context.Context, api internalgithub.API, request reconciliationEffectRequest, material reconciliationIssueUpdateMaterial) (bool, error) {
	if material.Issue == nil || material.Config.Repository != request.Repository || material.Config.ActorID < 1 || material.Issue.Repository != request.Repository || material.Issue.Issue != request.Issue || digestText(material.Issue.Body) != request.BodyDigest {
		return false, errStateConflict
	}
	applied, err := attemptIssueUpdateApplied(ctx, api, request, material.Config)
	if err != nil || applied {
		return applied, err
	}
	user, err := api.AuthenticatedUser(ctx)
	if err != nil || user.ID != material.Config.ActorID {
		if err == nil {
			err = errors.New("authenticated GitHub actor changed")
		}
		return false, err
	}
	attempts, err := internalgithub.FetchAttemptFacts(ctx, api, request.Repository, material.Config.ActorID)
	if err != nil {
		return false, err
	}
	collected, err := internalgithub.CollectIssueFactsForIssueV2(ctx, api, material.Config, attempts, request.Issue)
	if err != nil || len(collected.Facts) != 1 || digestText(collected.Facts[0].Body) != request.BodyDigest || collected.Facts[0].Attempt != request.Attempt && collected.Facts[0].CurrentAttempt != request.Attempt {
		if err == nil {
			err = errStaleStateResult
		}
		return false, err
	}
	if request.GitHubIssueUpdate.Kind == githubIssueEvidence {
		if material.Attempt == nil || !slices.ContainsFunc(attempts, func(attempt internalgithub.RecoveryAttemptFact) bool {
			return reflect.DeepEqual(attempt, *material.Attempt)
		}) {
			return false, errStaleStateResult
		}
	}
	return false, nil
}

func executeAttemptIssueUpdate(ctx context.Context, api internalgithub.API, request reconciliationEffectRequest, cfg internalgithub.PRAdapterConfig) error {
	update := request.GitHubIssueUpdate
	switch update.Kind {
	case githubIssueTerminalFailure:
		return durableAttemptFailure(ctx, api, internalgithub.RecoveryIssueFact{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt}, *request.Manifest, errors.New(update.Diagnostic))
	case githubIssueEvidence:
		return api.EnsureEvidence(ctx, request.Repository, request.Issue, request.Attempt, update.HeadSHA, cfg.ActorID)
	case githubIssueFindings:
		return api.EnsureReviewFindings(ctx, request.Repository, request.Issue, request.Attempt, update.HeadSHA, update.Findings, cfg.ActorID)
	case githubIssueRetry:
		return internalgithub.EnsureRetryCommand(ctx, api, cfg, request.Issue, request.Attempt)
	default:
		return errStateConflict
	}
}

func attemptIssueUpdateApplied(ctx context.Context, api internalgithub.API, request reconciliationEffectRequest, cfg internalgithub.PRAdapterConfig) (bool, error) {
	update := request.GitHubIssueUpdate
	var bodies []string
	switch update.Kind {
	case githubIssueTerminalFailure:
		marker, err := internalgithub.TerminalFailureMarker(request.Issue, request.Attempt, time.Unix(0, update.FailedAtUnixNano))
		if err != nil {
			return false, err
		}
		bodies = []string{marker}
	case githubIssueEvidence:
		for _, kind := range []string{"validation", "documentation"} {
			body, err := internalgithub.EvidenceBody(request.Issue, request.Attempt, kind, update.HeadSHA)
			if err != nil {
				return false, err
			}
			bodies = append(bodies, body)
		}
	case githubIssueFindings:
		body, err := internalgithub.ReviewFindingsBody(request.Issue, request.Attempt, update.HeadSHA, update.Findings)
		if err != nil {
			return false, err
		}
		bodies = []string{body}
	case githubIssueRetry:
		return internalgithub.RetryCommandApplied(ctx, api, cfg, request.Issue, request.Attempt, time.Unix(0, update.FailedAtUnixNano))
	default:
		return false, errStateConflict
	}
	for _, body := range bodies {
		present, err := internalgithub.HasAttemptComment(ctx, api, request.Repository, request.Issue, body, cfg.ActorID)
		if err != nil || !present {
			return false, err
		}
	}
	return true, nil
}

func (c *runtimeEffectCoordinator) finishReconciliationWithMarker(identity stateResultIdentity, request reconciliationEffectRequest, result reconciliationEffectResult) error {
	return c.finishReconciliationMarker(identity, request, result, false)
}

func (c *runtimeEffectCoordinator) finishReconciliationMarker(identity stateResultIdentity, request reconciliationEffectRequest, result reconciliationEffectResult, operator bool) error {
	if c == nil || c.owner == nil {
		return errors.New("runtime effect coordinator is unavailable")
	}
	if err := writeReconciliationEffectMarker(c.owner.stateRoot, identity, request, result); err != nil {
		return err
	}
	finish := finishReconciliationEffectCommand{Identity: identity, Result: result}
	var err error
	if operator {
		_, err = c.owner.finishOperatorReconciliationEffect(c.lifecycle, finishOperatorReconciliationEffectCommand{Finish: finish})
	} else {
		_, err = c.owner.finishReconciliationEffect(c.lifecycle, finish)
	}
	return err
}

// verifyPendingReconciliation commits a previously verified result without
// reconstructing ephemeral execution input or repeating external work.
func (c *runtimeEffectCoordinator) verifyPendingReconciliation(ctx context.Context, effect runtimeEffectIntent) (*reconciliationEffectResult, error) {
	return c.verifyPendingReconciliationMode(ctx, effect, false)
}

func (c *runtimeEffectCoordinator) verifyPendingOperatorReconciliation(ctx context.Context, effect runtimeEffectIntent) (*reconciliationEffectResult, error) {
	return c.verifyPendingReconciliationMode(ctx, effect, true)
}

func (c *runtimeEffectCoordinator) verifyPendingReconciliationMode(ctx context.Context, effect runtimeEffectIntent, operator bool) (*reconciliationEffectResult, error) {
	if c == nil || c.owner == nil || effect.Reconciliation == nil || effect.State != "pending" && effect.State != "completed" {
		return nil, errStateConflict
	}
	identity := ownerReconciliationEffectIdentity(effect)
	result, err := readReconciliationEffectMarker(c.owner.stateRoot, identity, *effect.Reconciliation)
	if err != nil || result == nil {
		return result, err
	}
	if effect.State == "pending" {
		finish := finishReconciliationEffectCommand{Identity: identity, Result: *result}
		var finishErr error
		if operator {
			_, finishErr = c.owner.finishOperatorReconciliationEffect(ctx, finishOperatorReconciliationEffectCommand{Finish: finish})
		} else {
			_, finishErr = c.owner.finishReconciliationEffect(ctx, finish)
		}
		if finishErr != nil {
			return nil, finishErr
		}
	} else if effect.ReconciliationResult == nil || !reflect.DeepEqual(*effect.ReconciliationResult, *result) {
		return nil, errStateConflict
	}
	if err := cleanupCompletedHandoffOutcome(c.owner.stateRoot, *effect.Reconciliation); err != nil {
		return nil, err
	}
	return result, nil
}

func (c reconciliationV2Collector) collect(ctx context.Context, snapshot stateOwnerSnapshot) (reconciliationV2Batch, error) {
	if ctx == nil || c.Config.Repository == "" || c.Config.Repository != snapshot.State.Repository || !validReconciliationScope(snapshot.State.Repository, c.Scope) {
		return reconciliationV2Batch{}, errors.New("v2 reconciliation collector is invalid")
	}
	attempts, err := internalgithub.FetchAttemptFacts(ctx, c.API, c.Config.Repository, c.Config.ActorID)
	if err != nil {
		return reconciliationV2Batch{}, err
	}
	var collected internalgithub.RecoveryIssueCollection
	if c.Scope.Kind == reconciliationIssueScope {
		collected, err = internalgithub.CollectIssueFactsForIssueV2(ctx, c.API, c.Config, attempts, c.Scope.Issue)
	} else {
		collected, err = internalgithub.CollectIssueFactsV2(ctx, c.API, c.Config, attempts)
	}
	if err != nil {
		return reconciliationV2Batch{}, err
	}
	batch := reconciliationV2Batch{Input: reconciliationInput{Scope: c.Scope, Complete: true, Issues: collected.Facts, Attempts: attempts}}
	for _, proposal := range collected.Proposals {
		accepted, material, err := reduceIssueUpdateProposal(proposal)
		if err != nil {
			return reconciliationV2Batch{}, err
		}
		batch.Input.IssueUpdates = append(batch.Input.IssueUpdates, accepted)
		material.Config = c.Config
		batch.IssueUpdates = append(batch.IssueUpdates, material)
	}
	slices.SortFunc(batch.IssueUpdates, func(a, b reconciliationIssueUpdateMaterial) int {
		if ordered := cmp.Compare(a.Proposal.Issue, b.Proposal.Issue); ordered != 0 {
			return ordered
		}
		if ordered := cmp.Compare(a.Proposal.Kind, b.Proposal.Kind); ordered != 0 {
			return ordered
		}
		return cmp.Compare(a.Proposal.Dependency, b.Proposal.Dependency)
	})
	return batch, nil
}

func reduceIssueUpdateProposal(raw internalgithub.IssueUpdateProposal) (reconciliationIssueUpdateProposal, reconciliationIssueUpdateMaterial, error) {
	proposal := reconciliationIssueUpdateProposal{Repository: raw.Repository, Issue: raw.Issue}
	switch raw.Kind {
	case internalgithub.IssueUpdateControlSnapshot:
		digest := sha256.Sum256([]byte(raw.ControlSnapshotBody))
		proposal.Kind = githubIssueControlSnapshot
		proposal.ControlSnapshotDigest = hex.EncodeToString(digest[:])
	case internalgithub.IssueUpdateDependencyClear:
		proposal.Kind = githubIssueDependencyClear
		proposal.AttributionAttempt = raw.AttributionAttempt
		proposal.Dependency = raw.Dependency
		proposal.PullRequest = raw.PullRequest
	default:
		return reconciliationIssueUpdateProposal{}, reconciliationIssueUpdateMaterial{}, errors.New("unknown issue update proposal")
	}
	material := reconciliationIssueUpdateMaterial{Proposal: proposal, ControlSnapshotBody: raw.ControlSnapshotBody}
	if !validReconciliationIssueUpdateProposal(raw.Repository, proposal) || proposal.Kind == githubIssueControlSnapshot && (raw.AttributionAttempt != 0 || raw.ControlSnapshotBody == "") || proposal.Kind == githubIssueDependencyClear && raw.ControlSnapshotBody != "" {
		return reconciliationIssueUpdateProposal{}, reconciliationIssueUpdateMaterial{}, errors.New("invalid issue update proposal")
	}
	return proposal, material, nil
}

func planReconciliationIssueUpdates(snapshot stateOwnerSnapshot, batch reconciliationV2Batch) ([]reconciliationPlannedEffect, error) {
	if snapshot.State.Repository == "" {
		return nil, errors.New("owner snapshot is invalid")
	}
	var plans []reconciliationPlannedEffect
	for _, material := range batch.IssueUpdates {
		proposal := material.Proposal
		key := ownerIssueKey(proposal.Repository, proposal.Issue)
		observation, ok := snapshot.State.Observations[key]
		if !ok || !observation.Present || observation.OwnerGeneration != snapshot.State.IssueGenerations[key] || !slices.Contains(observation.IssueUpdates, proposal) {
			continue
		}
		if proposal.Kind == githubIssueControlSnapshot {
			digest := sha256.Sum256([]byte(material.ControlSnapshotBody))
			if hex.EncodeToString(digest[:]) != proposal.ControlSnapshotDigest {
				return nil, errors.New("control snapshot material does not match the accepted proposal")
			}
		} else if material.ControlSnapshotBody != "" || proposal.AttributionAttempt != max(1, observation.Fact.CurrentAttempt) {
			return nil, errors.New("dependency clear material does not match the accepted observation")
		}
		if material.Config.Repository != proposal.Repository {
			return nil, errors.New("issue update config does not match the accepted repository")
		}
		request := reconciliationEffectRequest{
			Action: reconciliationGitHubIssueUpdate, Repository: proposal.Repository, Issue: proposal.Issue,
			ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest,
			GitHubIssueUpdate: &githubIssueUpdateEffectRequest{Kind: proposal.Kind, ControlSnapshotDigest: proposal.ControlSnapshotDigest, AttributionAttempt: proposal.AttributionAttempt, Dependency: proposal.Dependency, PullRequest: proposal.PullRequest},
		}
		request.ExecutionDigest = issueUpdateExecutionDigest(request, material)
		if !validReconciliationEffectRequest(snapshot.State.Repository, request) {
			return nil, errors.New("planned issue update is invalid")
		}
		plans = append(plans, reconciliationPlannedEffect{
			Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: observation.OwnerGeneration},
			Request:  request, Material: material,
		})
	}
	return plans, nil
}

func issueUpdateExecutionDigest(request reconciliationEffectRequest, material reconciliationIssueUpdateMaterial) string {
	payload := struct {
		Repository, Body string
		Issue            int
		Config           internalgithub.PRAdapterConfig
		Update           githubIssueUpdateEffectRequest
		IssueFact        *internalgithub.RecoveryIssueFact
		AttemptFact      *internalgithub.RecoveryAttemptFact
	}{Repository: request.Repository, Body: material.ControlSnapshotBody, Issue: request.Issue, Config: material.Config, Update: *request.GitHubIssueUpdate, IssueFact: material.Issue, AttemptFact: material.Attempt}
	encoded, _ := json.Marshal(payload)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}
