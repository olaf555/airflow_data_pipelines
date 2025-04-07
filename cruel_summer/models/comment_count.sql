select 
video_id ,
title , 
comment_count

from {{ source('dpu','youtube_stats')}}

-- video_id	text	
-- title	text NULL	
-- view_count	bigint NULL	
-- updated_at	timestamp NULL	
-- description	text NULL	
-- channel_title	text NULL	
-- published_at	timestamp NULL	
-- like_count	bigint NULL	
-- comment_count	bigint NULL	
-- category_id	text NULL	
-- tags	text NULL