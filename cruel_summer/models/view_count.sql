select 
channel_title ,
title,
published_at , 
category_id , 
tags ,
updated_at ,
view_count  
from {{ source('dpu','youtube_stats')}}
-- youtube_stats

 
--  video_id TEXT PRIMARY KEY,
--             title TEXT,
--             description TEXT,
--             channel_title TEXT,
--             published_at TIMESTAMP,
--             view_count BIGINT,
--             like_count BIGINT,
--             comment_count BIGINT,
--             category_id TEXT,
--             tags TEXT,
--             updated_at TIMESTAMP