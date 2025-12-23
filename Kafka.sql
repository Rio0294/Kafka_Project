use master

use KafkaProject

IF OBJECT_ID('stocks_info', 'U') IS NOT NULL
	DROP TABLE stocks_info
CREATE TABLE stocks_info(
    symbol NVARCHAR(MAX),
    [name] NVARCHAR(MAX),
    currency NVARCHAR(MAX),
    exchangeFullName NVARCHAR(MAX),
    exchange NVARCHAR(MAX)
);

TRUNCATE TABLE stocks_info;

BULK INSERT stocks_info
FROM 'C:\BULKCSV\stocks_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ','
);

