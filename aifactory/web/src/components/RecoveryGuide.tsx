import { Modal } from "../ui";

/** What the Factory recovers from on its own, when it stops for a person, and what a person can always do. */
export function RecoveryGuide({ onClose }: { onClose: () => void }) {
  return (
    <Modal title="How recovery works" onClose={onClose}>
      <div className="guide">
        <section>
          <h3>Handled without you</h3>
          <dl>
            <dt>Checks or review fail</dt>
            <dd>The next attempt starts with the failing output and every reviewer finding, up to the attempt limit in Settings.</dd>
            <dt>CI fails on the pull request</dt>
            <dd>The failing job logs go to the next attempt; the branch is rebuilt and CI runs again.</dd>
            <dt>Main moved before merging</dt>
            <dd>The branch is rebased, checks rerun and CI must pass again before it merges. A conflict becomes a new attempt.</dd>
            <dt>The Factory restarts</dt>
            <dd>Work resumes at the stage it reached. An attempt that was interrupted is run again and not counted.</dd>
            <dt>Smoke fails after a merge</dt>
            <dd>With <code>recovery.smoke_failure: revert</code> the Factory opens and merges a revert; otherwise it asks you.</dd>
          </dl>
        </section>
        <section>
          <h3>When it stops and asks</h3>
          <dl>
            <dt>A plan or merge needs approval</dt>
            <dd>Only when your autonomy policy says so. Approving a merge covers that exact candidate, not a later rebuild.</dd>
            <dt>A product decision</dt>
            <dd>The planner or builder found a genuine ambiguity. Your answer is delivered to the agent and can be remembered as a precedent.</dd>
            <dt>Attempts used up</dt>
            <dd>Try again with fresh attempts (add guidance first), take it over yourself, or abandon the task.</dd>
            <dt>Budget reached or the model provider failed</dt>
            <dd>Nothing continues until you raise the budget, fix the credential, or retry.</dd>
          </dl>
        </section>
        <section>
          <h3>What you can always do</h3>
          <dl>
            <dt>Guide</dt><dd>Post a message on an outcome; the running agent receives it on its next step.</dd>
            <dt>Take over</dt><dd>Stop the Factory on a task. Its latest candidate is pushed as a draft pull request, marked unverified.</dd>
            <dt>Cancel</dt><dd>Stop an outcome or a task. Nothing already merged is changed.</dd>
            <dt>Pause</dt><dd>Settings → Workspace pauses new plans and tasks; running steps finish.</dd>
          </dl>
        </section>
      </div>
    </Modal>
  );
}
