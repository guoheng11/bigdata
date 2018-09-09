with base as(
select explode(split(regexp_replace('[{a:b,c:d},{a:f,b:g}]','(^\\[\\{)|(\\}\\]$)|(^\\{)|(^\\}$)',''),"\\}\\,\\{")) as nc
)
select b.nn['a']
from 
(select str_to_map(nc) as nn
from base) as b