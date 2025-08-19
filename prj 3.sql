create database project_3;

use project_3;


create table users (
user_id int,
created_at varchar(50),
company_id int,
`language` varchar(15),
activated_at varchar(50),
state varchar(10));

SHOW VARIABLES LIKE 'secure_file_priv';

LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv"
INTO TABLE 	users
FIELDS TERMINATED BY ","
ENCLOSED BY '"'
LINES TERMINATED BY "\n"
IGNORE 1 ROWS;

SELECT * FROM users;

alter table users 
add column created_at_dt datetime,
add column activated_at_dt datetime;

update users
set 
created_at_dt = str_to_date(created_at, '%d-%m-%Y %H:%i') ,
activated_at_dt = str_to_date(activated_at, '%d-%m-%Y %H:%i');

alter table users
drop created_at,
drop activated_at;

ALTER TABLE users
CHANGE created_at_dt created_at DATETIME,
CHANGE activated_at_dt activated_at DATETIME;

ALTER TABLE users
MODIFY created_at DATETIME AFTER user_id,
MODIFY activated_at DATETIME AFTER `language`;

create table `events` (
user_id int,
occured_at varchar(50),
event_type varchar(15),
event_name varchar(35),
device varchar(30),
user_type int);

alter table `events` 
modify device varchar(40),
modify user_type int;
select * from events;
SHOW CREATE TABLE events;

alter table `events`
add column location varchar(20) after event_name;


DESC events;


LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv'
INTO TABLE `events`
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

alter table `events` 
add column occured_at_dt datetime after occured_at;

update `events`
set
occured_at_dt = str_to_date(occured_at, "%d-%m-%Y %H:%i");

alter table `events`
drop column occured_at;

alter table `events`
change occured_at_dt occured_at datetime;

create table email_events (
user_id int,
occurred_at varchar(30),
`action` varchar(25),
user_type int);

select * from email_events;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv'
INTO TABLE email_events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

update  email_events
set
occured_at_dt = str_to_date(occurred_at, '%d-%m-%Y %H:%i');

ALTER TABLE email_events
CHANGE occured_at_dt occured_at DATETIME AFTER user_id;

create table job_data (
ds varchar(10),
job_id int,
actor_id int,
`event` varchar(20),
`language` varchar(15),
time_spent int,
org char(1));



select * from job_data;

update job_data
set
ds = str_to_date(ds, "%m/%d/%Y")
;

desc job_data;

alter table job_data
modify ds date;

-- Q: How many hours did each job_id spend daily in Nov 2020?
SELECT 
  ds,
  job_id,
  ROUND(SUM(time_spent)/60, 2) AS  total_hrs
FROM job_data
WHERE 
  MONTH(ds) = 11 AND
  YEAR(ds) = 2020
GROUP BY job_id,ds
order by job_id;

Select ds,job_id,
	count(`event`)/sum(time_spent) as throughput,
    avg(count(`event`)/sum(time_spent)) over(order by ds rows between 6 preceding and current row) as `7 day rolling avg`
    from job_data
    group by ds,job_id;

-- Calculate the 7-day rolling average of throughput (number of events per second).
with throughput_measure as (
	select ds, job_id, 
    count(`event`)/sum(time_spent) as throughput
    from job_data
    group by ds,job_id
    )
    select ds,job_id, throughput,
    avg(throughput) 
    over(order by ds rows between 6 preceding and current row) AS `7_day_rolling_avg`
    from throughput_measure;
    
 select ds, max(ds) from job_data 
 group by ds;
 
 -- : Calculate the percentage share of each language in the last 30 days
with recent_lang as (   
	select `language`
    from job_data
    where ds between date_sub('2020-11-30', interval 29 day) and '2020-11-30'
    ),
    language_count as (
		select `language`, count(*) as lang_count
        from recent_lang
        group by `language`
        ),
	total_count as (
		select count(*) as total_c from recent_lang
        )
	select lc.`language` , lc.lang_count ,
		round((lc.lang_count/ tc.total_c)*100 ,2) as percentage
        from language_count lc
        cross join total_count tc
        order by percentage desc;
-- Write an SQL query to display duplicate rows from the job_data table.	
select * 
from (
	select * ,
		row_number() over(PARTITION BY ds, actor_id, job_id,
        `event`, `language`, time_spent, org 
           ORDER BY job_id ) as row_num 
from job_data ) as sub
where row_num > 1
;

select * from users;
select * from `events`;
select * from email_events;
alter table  email_events
drop column occurred_at ;
--  Write an SQL query to calculate the weekly user engagement.
select count(distinct user_id)as active_users,
		week(occured_at,1) as weeks,   -- Week starts on Monday
        year(occured_at) as years
	from `events` 
    group by years,weeks
	order by weeks
    ;


-- Analyze the growth of users over time for a product. (month)
SELECT 
  YEAR(created_at) AS signup_year,
  MONTH(created_at) AS signup_month,
  COUNT(DISTINCT user_id) AS new_users,
  SUM(COUNT(DISTINCT user_id)) OVER (
    ORDER BY YEAR(created_at), MONTH(created_at)
  ) AS cumulative_users
FROM users
GROUP BY signup_year, signup_month
ORDER BY signup_year, signup_month;

-- WEEK
SELECT 
  YEAR(created_at) AS signup_year,
  week(created_at) AS signup_week,
  COUNT(DISTINCT user_id) AS new_users,
  SUM(COUNT(DISTINCT user_id)) OVER (
    ORDER BY YEAR(created_at), week(created_at)
  ) AS cumulative_users
FROM users
GROUP BY signup_year, signup_week
ORDER BY signup_year, signup_week;

--  Write an SQL query to calculate the weekly retention of users based 
--    on their sign-up cohort.
SELECT 
    COUNT(DISTINCT u.user_id) AS user_signup,
    YEAR(u.activated_at) AS signup_year,
    WEEK(u.activated_at, 1) AS signup_week_number,
    YEAR(e.occured_at) AS active_year,
    WEEK(e.occured_at, 1) AS active_week_number,
    TIMESTAMPDIFF(WEEK, u.activated_at, e.occured_at) AS weeks_since_signup
FROM users u
JOIN events e ON u.user_id = e.user_id
GROUP BY 
    signup_year, signup_week_number, 
    active_year, active_week_number, 
    weeks_since_signup;
--  Write an SQL query to calculate the weekly engagement per device.

SELECT 
  device, 
  WEEK(occured_at, 1) AS weeks,
  year(occured_at) as active_year,
  COUNT(DISTINCT user_id) AS active_users 
FROM `events`
GROUP BY device, weeks,active_year
order by active_year,weeks;

--  Write an SQL query to calculate the email engagement metrics.

select count(distinct user_id) as users,
	WEEK(occured_at, 1) AS weeks,
  year(occured_at) as active_year,
  `action`
    from email_events
    group by `action`,weeks,active_year;
    
    
    
SELECT 
  YEAR(occured_at) AS year,
  WEEK(occured_at, 1) AS week,
  -- Counts
  COUNT(CASE WHEN action LIKE 'sent_%' THEN 1 END) AS emails_sent,
  COUNT(CASE WHEN action = 'email_open' THEN 1 END) AS email_opens,
  COUNT(CASE WHEN action = 'email_clickthrough' THEN 1 END) AS email_clicks,
  COUNT(DISTINCT user_id) AS active_users,
  -- Open Rate: (email_opens / emails_sent) * 100
  ROUND(
    COUNT(CASE WHEN action = 'email_open' THEN 1 END) * 100.0 /
    NULLIF(COUNT(CASE WHEN action LIKE 'sent_%' THEN 1 END), 0), 2
  ) AS open_rate_pct,
  -- Click Rate: (email_clicks / emails_sent) * 100
  ROUND(
    COUNT(CASE WHEN action = 'email_clickthrough' THEN 1 END) * 100.0 /
    NULLIF(COUNT(CASE WHEN action LIKE 'sent_%' THEN 1 END), 0), 2
  ) AS click_rate_pct
FROM email_events
GROUP BY year, week
ORDER BY year, week;
