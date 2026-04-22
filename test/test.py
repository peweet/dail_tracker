# This is a place holder .py file for now but it will be
# the starting point for writing tests for the project. We will use pytest as our testing framework and we will write tests for each of the services and utility functions in the project to ensure that they are working correctly and to catch any bugs or issues early on in the development process. We will also set up a test suite that can be run automatically as part of our CI/CD pipeline to ensure that all tests are run and that any issues are caught before they make it to production. This will help us to maintain
# a high level of code quality and to ensure that our project is reliable and robust as it evolves over time.
# TODO: write tests for the services and utility functions in the project, and set up a test suite that can be run automatically as part of our CI/CD pipeline. This will help us to maintain a high level of code quality and to ensure that our project is reliable and robust as it evolves over time. We can start by writing tests for the most critical functions and services in the project, and then expand our test coverage over time as we add more features and functionality to the project. We can also use tools like pytest-cov to measure our test coverage and to identify any areas of the code that are not being tested adequately.
# Check also pydantic, dataclass, and data expectations for future testing and validation of data structures and types across the codebase, to ensure that we are working with the correct data formats and to catch any issues early on in the development process. This will help us to maintain a high level of code quality and to ensure that our project is reliable and robust as it evolves over time.

# Take care to have the test folder mirror the productive folder to ensure ease of navigation and comprehension when writing and running tests, and to make it easier to identify which tests correspond to which services and utility functions in the project. This will help us to maintain a high level of organization and to ensure that our tests are easy to find and run as part of our development process. For example, we can have a directory structure like this:
# test/
#     attendance/
#     payments/
#     lobbying/
#     utility/


def test_td_count(): ...  # >= 127 unique TDs in attendance CSV
def test_no_null_names(): ...  # no empty identifiers
def test_max_attendance(): ...  # no TD exceeds 103 sitting days
def test_enriched_has_party(): ...  # all rows have a non-null party after join
