SELECT * FROM cedm.ce_scoring_engine WHERE scoreDate = REGEXP_REPLACE('{start_date}',"-","")
