Airflow Problem
========
A solution to the Airflow problem is in `./airflow_problem`.

Solution: `./airflow_problem/dags/anomaly_detect.py`
<a href="https://github.com/jacksonhoyt/lennar/blob/main/airflow_problem/dags/anomaly_detect.py" target="_blank">Jump to solution</a>

Explanation: `./airflow_problem/README.md`
<a href="https://github.com/jacksonhoyt/lennar/tree/main/airflow_problem#overview" target="_blank">Jump to solution summary</a>


SQL Problem
========
A solution to the SQL problem is in `./sql_problem`.

Solution: `./airflow_problem/dags/anomaly_detect.py`
<a href="https://github.com/jacksonhoyt/lennar/blob/main/sql_problem/user_activity_summary.sql" target="_blank">Jump to solution</a>

Explanation: `./airflow_problem/README.md`
<a href="https://github.com/jacksonhoyt/lennar/tree/main/sql_problem#solution" target="_blank">Jump to solution summary</a>


DBT Quality
========

To support a team of analysts contributing to the DBT project I would approach quality standards in the following ways, and personally look to improve my DevOps knowledge to implement and streamline CI/CD.

1. Define and document standards
*Ensure the team is aware of coding standards and CI/CD process*
- Code formatting, style, best practices (adhere to DBT style guide)
- Detail dbt specific standards (naming conventions, model organization, etc)

2. Enforcement
*Changes must be opened as a PR in GitHub, code cannot be merged if linting fails.*
- SQL Linting: dbt recommends SQLFluff
- Python Linting: dbt recommends Black or Ruff

3. CI/CD Automation
*Code also cannot be merged without passing CI checks.*
- GitHub Actions to run automated checks on every pull request
- Integrate dbt Cloud with GitHub to listen for PR's, using dbt Cloud's Slim CI to automate checks that the code builds without error
- A data diff tool like Datafold can check how the data will be impacted by the changes in the PR, which may be empowered by Snowflake's zero-copy cloning by creating a temporary clone of the data to perform tests on.