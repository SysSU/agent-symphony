package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
)

type supervisorProposalServiceV2 struct {
	agent    *orchestratoragent.Supervisor
	owner    *stateOwner
	effects  *runtimeEffectCoordinator
	operator *operatorMutationService
	trigger  *reconciliationTriggerRunner
	capacity int
}

type supervisorProposalRunnerV2 struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func startSupervisorProposalRunnerV2(parent context.Context, service *supervisorProposalServiceV2, log io.Writer) (*supervisorProposalRunnerV2, error) {
	if parent == nil || service == nil || log == nil {
		return nil, errors.New("v2 supervisor proposal runner is incomplete")
	}
	ctx, cancel := context.WithCancel(parent)
	runner := &supervisorProposalRunnerV2{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(runner.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			if err := service.process(ctx); err != nil && ctx.Err() == nil {
				_, _ = fmt.Fprintln(log, "orchestrator proposal: "+internalgithub.Redact(err.Error()))
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return runner, nil
}

func (r *supervisorProposalRunnerV2) shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.cancel()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *supervisorProposalServiceV2) process(ctx context.Context) error {
	if s == nil || s.agent == nil || s.owner == nil || s.effects == nil || s.operator == nil || s.trigger == nil || s.capacity < 1 {
		return errors.New("v2 supervisor proposal service is incomplete")
	}
	proposal, err := s.agent.MessageProposal(ctx)
	if errors.Is(err, orchestratoragent.ErrNoMessageProposal) {
		return nil
	}
	if err != nil {
		return err
	}
	snapshot, err := s.owner.snapshot(ctx)
	if err != nil {
		return err
	}
	projection, err := projectOwnerStatus(snapshot, s.capacity, time.Now().UTC())
	if err != nil {
		return err
	}
	if err := s.agent.ValidateAttentionProposal(proposal, projection.Statuses); err != nil {
		return s.resolve(ctx, proposal.Binding, "refused", err)
	}
	var checkIn reconciliationPlannedEffect
	switch proposal.Action {
	case orchestratoragent.ProposalActionRetry:
		err = validateTransitionRetry(proposal, projection.Statuses)
	case orchestratoragent.ProposalActionRecover:
		_, _, err = operatorAttempt(snapshot, controlRequest{Version: controlVersion, RequestID: proposal.RequestID, Repository: proposal.Repository, Action: "recover", Issue: proposal.Issue, Attempt: proposal.Attempt})
	case orchestratoragent.ProposalActionAttention:
	case orchestratoragent.ProposalActionCheckIn:
		if err = validateMonitoringCheckIn(proposal, projection.Statuses); err == nil {
			checkIn, err = planMonitoringCheckIn(snapshot, proposal)
		}
	default:
		err = errors.New("unsupported orchestrator proposal action")
	}
	if err != nil {
		return errors.Join(err, s.resolve(ctx, proposal.Binding, "refused", err))
	}
	if err := s.agent.ResolveMessageProposal(ctx, proposal.Binding, "running", "the coordinator validated the exact owner projection and durably admitted the requested action"); err != nil {
		return err
	}
	succeeded := "the owner accepted the exact proposal"
	switch proposal.Action {
	case orchestratoragent.ProposalActionRetry:
		err = s.trigger.triggerAndWait(ctx)
		succeeded = "the exact owner-bound reconciliation cycle completed"
	case orchestratoragent.ProposalActionRecover:
		result := s.operator.performSynchronously(ctx, controlRequest{Version: controlVersion, RequestID: proposal.RequestID, Repository: proposal.Repository, Action: "recover", Issue: proposal.Issue, Attempt: proposal.Attempt})
		if !result.OK || result.Status != http.StatusOK {
			err = fmt.Errorf("owner recovery did not reach a durable terminal receipt: %s", result.Error)
		}
		succeeded = "the stable owner recovery receipt completed durably"
	case orchestratoragent.ProposalActionAttention:
		succeeded = proposal.Detail
	case orchestratoragent.ProposalActionCheckIn:
		if checkIn, err = s.effects.beginReconciliation(ctx, checkIn); err == nil {
			_, err = s.effects.executeMonitoringCheckIn(checkIn)
		}
		succeeded = "the generation-bound monitoring check-in was delivered and durably recorded"
	}
	if err != nil {
		return errors.Join(err, s.resolve(ctx, proposal.Binding, "failed", err))
	}
	return s.agent.ResolveMessageProposal(ctx, proposal.Binding, "succeeded", succeeded)
}

func (s *supervisorProposalServiceV2) resolve(ctx context.Context, binding, resolution string, cause error) error {
	return s.agent.ResolveMessageProposal(ctx, binding, resolution, cause.Error())
}
