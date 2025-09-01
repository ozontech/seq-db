# Contributing Guidelines

We appreciate your interest in contributing to this project! This document establishes the mandatory standards that ensure high-quality contributions and efficient collaboration.
By following these guidelines, you help us maintain code quality and project consistency.

## Code of Conduct

All contributors must adhere to our project's code of conduct. We expect professional communication, respectful collaboration, and constructive feedback in all interactions.

## Before You Start

### Development Environment Setup

**Required dependencies:**
- Go version must be equal to the one in `go.mod` or higher;
- Git for version control;
- Make for build automation.

### Pre-Implementation Discussion

**All non-trivial changes must be discussed in an issue before implementation.** This prevents duplicate work and ensures alignment with project goals.

**Required steps:**
1. **Search existing issues** to avoid duplicates;
2. **Create a detailed issue** describing your proposed changes;
3. **Wait for maintainer approval** before beginning implementation;
4. **Reference the issue number** in your pull request.

**Exception:** Minor fixes (typos, documentation corrections) may skip this requirement.

## Mandatory Commit Message Convention

All commits merged into the main branch **must** strictly follow [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).
While temporary commits may use alternative formats, conventional commits are strongly recommended throughout development.

Commit scope is optional but encouraged for clarity.

Definition of scope:
- the LCA (Lowest Common Ancestor) of the directories modified in the PR;
- the name of the feature implemented/improved in the PR.

### Required Commit Types

**You must** use one of the following commit types:

- **`build`** - Changes related to build system (Dockerfile, build scripts, etc.);
- **`chore`** - Repository maintenance tasks (Makefile changes, code comments, bump of dependencies etc.);
- **`ci`** - CI/CD pipeline modifications;
- **`docs`** - Documentation updates (excluding code comments);
- **`feat`** - New user-facing functionality implementation;
- **`fix`** - Bug resolution (e.g., fixing race conditions in single-node version due to buffer reuse);
- **`perf`** - Performance optimizations (e.g., adding ASCII ToLower array and using it as hot-path);
- **`refactor`** - Code restructuring for improved readability without functionality changes (e.g., refactoring sealing logic);
- **`revert`** - Reverting previous commits (e.g., fixing broken MergeQPRs from previous commit);
- **`style`** - Code style corrections (linter compliance);
- **`test`** - Test additions or modifications.

### Commit Message Examples

**Good examples:**
```text
feat: add `ip_range` function
fix: resolve buffer reuse race condition in single-node mode
docs: update benchmarks results in the documentation
perf(query): optimize ASCII character processing
refactor: improve sealing logic readability
```

**Bad examples:**
```text
updated stuff
fix bug
changes
WIP
```

## Mandatory Go Style Compliance

**All Go code must comply** with the following style guides in this exact order of priority:

1. [Google Go Style Guide](https://google.github.io/styleguide/go/guide);
2. [Google Go Style Decisions](https://google.github.io/styleguide/go/decisions);
3. [Google Go Best Practices](https://google.github.io/styleguide/go/best-practices);
4. [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md);
5. [Effective Go](https://go.dev/doc/effective_go).

**Code that does not comply with these standards will require revision before acceptance.**

## Mandatory Branch Naming Convention

Branch names must use `kebab-case` and strictly follow this format:

```text
{issue-number}-{branch-name}
```

For branches not associated with any issue, **you must** use:

```text
0-{branch-name}
```

### Branch Naming Examples

**Good examples:**
```text
123-fix-datarace-in-single-mode
789-update-dependencies
0-fix-typo-in-readme
```

**Bad examples:**
```text
feature-branch
my-changes
fix
update
```

**Non-compliant branch names will require revision.**

## Required Issue Classification and Labeling

### Mandatory Issue Types

**You must** classify all issues using one of these types:

- **Bug** - All bug reports and defects;
- **Feature** - New feature requests and enhancements;
- **Security** - Security-related vulnerabilities and concerns;
- **Question** - General inquiries (will be redirected to GitHub Discussions);
- **Blank** - All other issue types.

### Required Labeling Strategy

**Apply appropriate labels** in combination with issue types, for example:

- Code optimization issues: **Must** use **Blank** type with `performance` label;
- Documentation fixes: **Must** use **Blank** type with `documentation` label;
- **Apply additional relevant labels** as required for proper categorization.

### Issue Examples

**Good issue descriptions:**
- Bug reports: **Bug** type + `bug` label + detailed reproduction steps;
- Performance optimization: **Blank** type + `performance` label + benchmarks;
- Documentation updates: **Blank** type + `documentation` label + specific section reference.

## Mandatory Pull Request Requirements

### Pull Request Standards

**Every pull request must include:**
- **Appropriate labels** corresponding to the type of changes (to group several PRs one can use additional labels e.g. `epic-offloading`);
- **Reference to related issue** (e.g., "Closes #123", "Fixes #123");
- **Clear description** of changes made;
- **Test coverage** for new functionality (generated automatically);
- **Updated documentation** when applicable.

### Code Review Process

**Pull request review requirements:**
- **All automated checks** must pass (CI, linting, tests);
- **At least two maintainer approvals** are required;
- **Address all review feedback** before merge;
- **Squash commits** during merge to maintain clean history.

**Pull requests that do not meet these standards will require revision.**

## Getting Started for New Contributors

### Finding Good First Issues

Look for issues labeled with:
- `good-first-issue` - Beginner-friendly tasks;
- `help-wanted` - Community contributions welcome;
- `documentation` - Non-code contributions.

### Contribution Process

**Follow these mandatory steps:**

1. **Fork the repository** and clone your fork locally;
2. **Create a new branch** following the mandatory naming convention;
3. **Set up your development environment** using the instructions above;
4. **Implement your changes** in strict compliance with the Go style guides;
5. **Write or update tests** as required for your changes;
6. **Ensure all commits** follow the conventional commit format;
7. **Run all tests locally** to verify your changes;
8. **Submit a pull request** with appropriate labels and complete description.

**Contributions that do not follow these steps will require revision.**

## AI Assistance Notice

> [!IMPORTANT]
>
> If you are using **any kind of AI assistance** to contribute to `seq-db`,
> it must be disclosed in the pull request.

If you are using any kind of AI assistance while contributing to `seq-db`,
**this must be disclosed in the pull request**, along with the extent to
which AI assistance was used (e.g. docs only vs. code generation).
If PR responses are being generated by an AI, disclose that as well.
As a small exception, trivial tab-completion doesn't need to be disclosed,
so long as it is limited to single keywords or short phrases.

An example disclosure:

> This PR was written primarily by Claude Code.

Or a more detailed disclosure:

> I consulted ChatGPT to understand the codebase but the solution
> was fully authored manually by myself.

Failure to disclose this is first and foremost rude to the human operators
on the other end of the pull request, but it also makes it difficult to
determine how much scrutiny to apply to the contribution.

Please be respectful to maintainers and disclose AI assistance.

> This section is adapted from [ghostty](https://github.com/ghostty-org/ghostty/blob/main/CONTRIBUTING.md).
> Credit goes to the original authors!

## Recognition and Community

### Contributor Recognition

**We value all contributions** and recognize contributors through:
- Acknowledgment in release notes;
- Listing in project credits and README for significant contributions;

### Communication Channels

**Get help and stay connected:**
- **GitHub Issues** - Bug reports and feature requests;
- **GitHub Discussions** - General questions and community discussions;
- **Code Reviews** - Technical discussions on pull requests.

## Support and Questions

For questions regarding contribution requirements:

- **Review existing issues and discussions** before creating new ones;
- **Create a Question-type issue** (will be redirected to GitHub Discussions);
- **Thoroughly review** this contributing guide and all referenced style guides;
- **Check the project README** for additional context and setup information.

**All contributions must meet these standards. Non-compliant submissions will require revision before acceptance.**

## Resources

**Helpful links:**
- [Conventional Commits Specification](https://www.conventionalcommits.org/en/v1.0.0/);
- [Go Style Guides](https://google.github.io/styleguide/go/guide);
- [How to Write a Git Commit Message](https://chris.beams.io/posts/git-commit/);
- [GitHub Flow Guide](https://guides.github.com/introduction/flow/).

Thank you for contributing to our project!
