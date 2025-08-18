# Contributing Guidelines

This document establishes the mandatory standards and procedures for contributing to this project. All contributors must adhere to these guidelines without exception.

## Mandatory Commit Message Convention

All commits merged into the main branch **must** strictly follow [Conventional Commits v1.0.0](https://www.conventionalcommits.org/en/v1.0.0/). While temporary commits may use alternative formats, conventional commits are strongly recommended throughout development.

Commit scope is optional but encouraged for clarity.

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

### Examples

```
feat: add user authentication
fix: resolve buffer reuse race condition
docs: update API documentation
perf(query): optimize ASCII character processing
```

## Mandatory Go Style Compliance

**All Go code must comply** with the following style guides in this exact order of priority:

1. [Google Go Style Guide](https://google.github.io/styleguide/go/guide);
2. [Google Go Style Decisions](https://google.github.io/styleguide/go/decisions);
3. [Google Go Best Practices](https://google.github.io/styleguide/go/best-practices);
4. [Uber Go Style Guide](https://github.com/uber-go/guide/blob/master/style.md);
5. [Effective Go](https://go.dev/doc/effective_go).

## Mandatory Branch Naming Convention

Branch names **must** strictly follow this format:

```
{issue-number}-{branch-name}
```

For branches not associated with any issue, **you must** use:

```
0-{branch-name}
```

### Examples

```
123-fix-authentication-bug
456-add-user-dashboard
0-update-readme
```

## Required Issue Classification and Labeling

### Mandatory Issue Types

**You must** classify all issues using one of these types:

- **Bug** - All bug reports and defects;
- **Feature** - New feature requests and enhancements;
- **Security** - Security-related vulnerabilities and concerns;
- **Question** - General inquiries (will be redirected to GitHub Discussions);
- **Blank** - All other issue types.

### Required Labeling Strategy

**Apply appropriate labels** in combination with issue types e.g.:

- Code optimization issues: **Must** use **Blank** type with `performance` label;
- Documentation fixes: **Must** use **Blank** type with `documentation` label;
- **Apply additional relevant labels** as required for proper categorization.

## Mandatory Pull Request Requirements

**Every pull request must include appropriate labels** corresponding to the type of changes being made.

## Contribution Process

**Follow these mandatory steps:**

1. **Fork the repository**;
2. **Create a new branch** following the mandatory naming convention;
3. **Implement your changes** in strict compliance with the Go style guides;
4. **Write or update tests** as required for your changes;
5. **Ensure all commits** follow the conventional commit format;
6. **Submit a pull request** with appropriate labels and complete description.

## Support and Questions

For questions regarding contribution requirements:

- **Review existing issues and discussions** before creating new ones;
- **Create a Question-type issue** (will be redirected to GitHub Discussions);
- **Thoroughly review** this contributing guide and all referenced style guides;
- **Ensure compliance** with all requirements before submitting contributions.
