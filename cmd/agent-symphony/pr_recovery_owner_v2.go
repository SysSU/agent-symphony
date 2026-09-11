package main

import (
	"context"
	"errors"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

// ownerAttemptRecovery is the v2 AttemptRecovery adapter used only while an
// exact github-pr-governance effect is pending.
type ownerAttemptRecovery struct {
	owner    *stateOwner
	identity stateResultIdentity
}

func (r ownerAttemptRecovery) PullRequestState(ctx context.Context, repository string, number, issue, attempt int, head string) (internalgithub.PRState, error) {
	if r.owner == nil {
		return internalgithub.PRState{}, errors.New("owner recovery is unavailable")
	}
	snapshot, err := r.owner.snapshot(ctx)
	if err != nil {
		return internalgithub.PRState{}, err
	}
	effect, ok := snapshot.State.Effects[r.identity.EffectID]
	if !ok || effect.Reconciliation == nil || effect.State != "pending" || effect.Reconciliation.Action != reconciliationGitHubPRGovernance || !reconciliationEffectIdentityMatches(effect, r.identity) {
		return internalgithub.PRState{}, errStaleStateResult
	}
	recovery, ok := snapshot.State.Recoveries[ownerAttemptKey(repository, issue, attempt)]
	if !ok || recovery.State.Number != number || recovery.State.HeadSHA == "" {
		return internalgithub.PRState{}, errStaleStateResult
	}
	state := clonePRState(recovery.State)
	state.Facts.BranchModifiedOutsideAttempt = state.Facts.BranchModifiedOutsideAttempt || state.HeadSHA != head
	return state, nil
}

func (r ownerAttemptRecovery) ClaimFeedback(ctx context.Context, state internalgithub.PRState, feedback internalgithub.Feedback) (bool, error) {
	if r.owner == nil {
		return false, errors.New("owner recovery is unavailable")
	}
	_, err := r.owner.mutatePRRecovery(ctx, mutatePRRecoveryCommand{Identity: r.identity, Kind: prRecoveryClaimFeedback, State: clonePRState(state), Feedback: feedback})
	if errors.Is(err, errPRFeedbackAlreadyClaimed) {
		return false, nil
	}
	return err == nil, err
}

func (r ownerAttemptRecovery) QueueValidation(ctx context.Context, state internalgithub.PRState) error {
	if r.owner == nil {
		return errors.New("owner recovery is unavailable")
	}
	_, err := r.owner.mutatePRRecovery(ctx, mutatePRRecoveryCommand{Identity: r.identity, Kind: prRecoveryQueueValidation, State: clonePRState(state)})
	return err
}

var _ internalgithub.AttemptRecovery = ownerAttemptRecovery{}
