# supaflow-salesforce-to-s3 contributor guidelines

- Follow workspace instructions when available; preserve unrelated work and secrets.
- Develop on a task branch and describe requirements, intentional constraints, non-goals, and verification in the PR template. Review concrete requirement violations separately from optional improvements.
- Commit, push and merge only with user authorization; use merge commits and never bypass gates. A merge does not authorize deployment, database access, or external publication.

## Main branch enforcement

- The main-branch protection rollout requires PRs, resolved conversations, merge commits, `PR checks` and `Codex review / main`, with no bypass actors. Verify live rule activation and successful current-head evidence before claiming enforcement.
- `.github/workflows/pr-checks.yml` defines the offline CI coverage. These checks do not prove live credentials, deployments, or database/data correctness. No live test or publication is authorized by CI.
- Keep the Codex connector enabled for this repository and request a fresh review when necessary. Missing or incomplete review is not a pass.

- Choose the intended target branch when opening a PR. If its base branch is changed, close it and open a new PR against the intended base; the review gate rejects retargeted PRs because review summaries identify the head but not the reviewed base.
