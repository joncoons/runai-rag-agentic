-- A crawl may durably admit many page jobs before its scan finishes.
CREATE TABLE IF NOT EXISTS application.website_run_jobs (
 run_id uuid NOT NULL REFERENCES application.website_source_runs,
 job_id uuid NOT NULL REFERENCES application.jobs,
 PRIMARY KEY(run_id,job_id)
);
