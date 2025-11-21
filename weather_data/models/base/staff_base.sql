{{
  config(
    materialized = 'view',
    )
}}

SELECT * FROM {{ source('public', 'staff') }}