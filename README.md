# Operation-Analytics-and-Investigating-Metric-Spike---SQL-Workbench

Project Overview
This project focuses on Operations Analytics using SQL. The goal was to analyze user engagement, retention, and behavioral patterns from operational datasets. By leveraging SQL queries, I derived insights into weekly activity trends, user retention, device usage, and throughput, simulating real-world business metrics tracking.

Dataset
The project uses three main tables:
- users → user_id, created_at, company_id, language, activated_at, state
- events → user_id, occured_at, event_type, event_name, location, device, user_type
- email_events → user_id, occured_at, action, user_type

The dataset contained ~9k users, ~325k events, and ~90k email logs.

Tech Stack
- SQL Workbench – Querying and analysis
- MySQL – Database engine

Key Analyses Performed
- User Retention Analysis: Weekly cohort-based retention after signup.
- Engagement Analysis: Measuring active users by week (logins, actions).
- Throughput Analysis: Calculated 7-day rolling averages of events per second to smooth fluctuations.
- Language Share: Percentage breakdown of user languages over the last 30 days.
- Duplicate Detection: Identified duplicate rows using window functions.
- Ad-hoc Metrics: Explored day-of-week activity patterns and device usage trends.

SQL Concepts Applied
- Joins (INNER, LEFT)
- Aggregations (COUNT, SUM, AVG)
- Window Functions (ROW_NUMBER, RANK, Rolling Averages)
- Cohort Analysis
- Date/Time Functions (WEEK, MONTH, DATE_SUB)
- CTEs (WITH queries)

Insights Gained
- Clear identification of active vs. inactive users for targeted engagement.
- Measured weekly retention trends, crucial for product growth analysis.
- Rolling average of throughput highlighted performance stability over time.
- Language usage analysis suggested localized marketing opportunities.
