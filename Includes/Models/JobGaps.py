# Databricks notebook source
class JobGaps:

    def __init__(self, jobs):
        self.jobs = jobs
        self.jobs_df = self.jobs.get_df()
        self.jobs_df.createOrReplaceTempView('job_gaps_raw')

        sql("""CREATE OR REPLACE TEMP VIEW JobGaps AS
                SELECT Completion_Time AS Gap_Start, Submission_time AS Gap_End
                FROM
                    (
                        SELECT DISTINCT Submission_time, ROW_NUMBER() OVER (ORDER BY Submission_time) RN
                        FROM job_gaps_raw T1
                        WHERE
                            NOT EXISTS (
                                SELECT *
                                FROM job_gaps_raw T2
                                WHERE T1.Submission_time > T2.Submission_time AND T1.Submission_time < T2.Completion_Time
                            )
                        ) T1
                    JOIN (
                        SELECT DISTINCT Completion_Time, ROW_NUMBER() OVER (ORDER BY Completion_Time) RN
                        FROM job_gaps_raw T1
                        WHERE
                            NOT EXISTS (
                                SELECT *
                                FROM job_gaps_raw T2
                                WHERE T1.Completion_Time > T2.Submission_time AND T1.Completion_Time < T2.Completion_Time
                            )
                    ) T2
                    ON T1.RN - 1 = T2.RN
                WHERE
                    Completion_Time < Submission_time"""
        )

        self.df = spark.table('JobGaps')
        self.df_cached = self.df.cache()

    def display(self):
        self.df_cached.display()

    def get_gap_stats(self):
        return sql('select max(Gap_End - Gap_Start) AS Max_Gap, sum(Gap_End - Gap_Start) AS Total_Gap from JobGaps').collect()[0]


    # from start of 1st job to end of last
    def get_max_job_gap_secs(self):
        max_gap = self.get_gap_stats()['Max_Gap']
        if max_gap is None:
            return 0
        else:
            return max_gap/1000

    def get_total_job_gap_secs(self):
        total_gap = self.get_gap_stats()['Total_Gap']
        if total_gap is None:
            return 0
        else:
            return total_gap/1000

# COMMAND ----------


