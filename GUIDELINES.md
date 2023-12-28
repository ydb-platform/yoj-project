YOJ Project Guidelines
======================

1. Basic PR Quality
    - For a **ready feature, improvement or a bugfix**: Ensure that everything builds locally, and passes integration and unit tests before making a PR.
To run integration tests, use Maven `integration-test` and `lombok` profiles (`mvn -Pintegration-test -Plombok <...>`).
    - For a **prototype, suggested or partly done feature** that won't necessarily build or work as intended: Mark your PR with "WIP: " prefix, e.g.: `WIP: New session manager`.
Remove the "WIP: " prefix when your code is ready to be reviewed and merged.
2. Branching
    - You SHOULD NOT commit directly to `main`. You SHOULD create a branch for your feature, bugfix or improvement.
    - Before finally merging, you SHOULD use interactive rebase + `git push --force` to get nice linear history.
Force-pushing into a remote branch is perfectly fine, provided that no one works on it except you 😄
    - Branch Names:
        - Branch names MUST use lowercase `kebab-case` and ASCII characters only, to work well across all file systems. Underscores (`_`) MUST NOT be used.
        - Branch names SHOULD clearly describe the feature/bugfix/improvement made in the branch. Other than that, we don't enforce any strict naming pattern. E.g., the `security-update-jackson-cve-2023-xxxx` branch name is good; `issue-152` is bad
        - For multiple PRs involving the same logical change, you SHOULD use the name format `<logical-change-description>/<concrete-branch-description>`, e.g. `new-session-manager/move-dependencies`
5. For commit messages, follow the rules established in https://cbea.ms/git-commit/ because GitHub UI honors them. Basically:
    1. Use imperative mood ("Make ...", "Add ...", etc.) in the commit message
    2. First line of the commit message MUST be a concise summary of the changes. The first line MUST begin with a capital letter. It SHOULD be <= 50 characters, MUST be <= 72 characters.
    3. Add body to the commit message if you need to clarify your intent or technical implementation details.
To do this, add a second *empty* line to the message, then put the details on the 3rd and the following lines.
      - Each body line MUST be <= 72 characters long.
      - If bulleted lists are used in the commit message body, Markdown syntax MUST be used for them.
    6. If your commit is related to a GitHub issue, you SHOULD add the issue number as a message prefix: `#1: Use New Session Mananager`.
If the commit message also has a body, you SHOULD finish the body with the line: `Fixes: #<issue number>[, #<issue number>, ...]`
6. Code Review Comments: You SHOULD use comment format recommended by https://conventionalcomments.org/.
