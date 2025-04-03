
  
  create view "duckdb_database"."main"."debug__suites__dbt_tmp" as (
    -- debug__suites.sql
select * from "duckdb_database"."main"."staging__sa_siicea_suites" limit 5
  );
