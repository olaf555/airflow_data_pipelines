select 
title,view_count 
from {{ source('dpu','youtube_stats')}}
-- youtube_stats
