create table my_catalog.my_schema.my_table as
select *
from another_catalog.schema_a.table_a
left join another_catalog.schema_b.table_b;
SELECT CustomerID, TransactionID, TransactionAmount, TransactionDate
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY TransactionDate) AS rn
    FROM test.data.Transactions
) t
WHERE rn = 1
ORDER BY CustomerID;
DROP TABLE test.data.Transactions;

drop table my_catalog.my_schema.temporary_table;