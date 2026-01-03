select concat('`', table_catalog,'.',table_schema, '.',table_name,'`') as tables 
from `amz-atlas-client-warehouse`.Atlas.INFORMATION_SCHEMA.TABLES 
where table_name like '%_Prod_FBAManageInventory'
and lower(table_name) not like '%audit%'
