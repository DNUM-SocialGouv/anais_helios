{{ config(
    materialized='view'
) }}

SELECT * FROM {{ source('main', 'sa_siicea_missions') }}