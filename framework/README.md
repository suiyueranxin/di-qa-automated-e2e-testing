# Contributing to the E2E Testframework

This is where we maintain information for developers who would like to contribute
to the test framework itself. If you are looking for information on how to
write tests for test DI systems please look at the README.md at the root of the
repository.

## Testing

We have two kind of tests for the framework coding: unit tests and integration
test. While the unit test are fast running and self contained the integration
test have dependencies to other systems.

At the moment only the unit test are automatically triggered by changes to the
repository. Since changes can only be done via pull requests (PR) we have build
validation for all PRs that have the main branch as target. The tests are run on
a Jenkins instance.
