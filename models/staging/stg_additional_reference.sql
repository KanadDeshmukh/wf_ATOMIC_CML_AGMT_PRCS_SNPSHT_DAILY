-- Stages reference tables such as work tables or control tables joined in the mart model per mapping.
select * from {{ source('raw','INMY_AGMT_AGGR') }} -- Add more as needed with unions or multiple CTEs
