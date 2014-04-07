# A simple mongod test harness

This is some Python code I wrote to spin up MongoDB replica sets and
run automated tests on them. It's pretty quick-and-dirty but it's been
useful for me to submit reproducible test cases for a handful of
MongoDB issues I've run into. The existing test cases are:

- `index.py`: Benchmark foreground index builds against background
  builds. [SERVER-13320](https://jira.mongodb.org/browse/SERVER-13320)

- `push.py`: Benchmark replicating `$push`
  operations. [SERVER-10595](https://jira.mongodb.org/browse/SERVER-10595)

- `test-failover.py`: Benchmark failovers after
  `rs.stepDown()`. [SERVER-9934](https://jira.mongodb.org/browse/SERVER-9934)
