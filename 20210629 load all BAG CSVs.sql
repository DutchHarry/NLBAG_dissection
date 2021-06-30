USE BAG;
GO

DECLARE @sql varchar(max) ='';
DECLARE @quote varchar(5) = '''';
DECLARE @crlf varchar(5) = CHAR(13)+CHAR(10);
DECLARE @filedirectory varchar(2000) = 'D:\NLDATA\BAG\20210608 BAGNLDL-08062021'
DECLARE @filename varchar(2000) = '9999PND08062021'
DECLARE @datesuffix varchar(255) = '08062021'
DECLARE @fileextension varchar(255) = '.csv'
DECLARE @debug varchar(1) = 'Y' --Y,N,I
DECLARE @msg varchar(max) = '';


DECLARE bag_csv_file CURSOR 
  FAST_FORWARD
  FOR 
	SELECT t.v FROM (VALUES 
  ('9999PND')
, ('9999VBO')
, ('9999NUM')
, ('9999OPR')
, ('9999WPL')
, ('9999LIG')
, ('9999STA')
	) AS t(v);
OPEN bag_csv_file  
FETCH NEXT FROM bag_csv_file
INTO @filename; 
WHILE @@FETCH_STATUS = 0  
BEGIN  
  PRINT @filename;
  --BEGIN DO SOMETHING WITHIN CURSOR
	--BEGIN CSV FILE LOADING
  DROP TABLE IF EXISTS [tmp_One_Column];
  CREATE TABLE [tmp_One_Column](
    OneColumn varchar(max) NULL
  );
  SET @sql = '
  BULK INSERT [tmp_One_Column]
  FROM '+@quote+@filedirectory+'\'+@filename+@datesuffix+@fileextension+@quote+'
  WITH (
    CODEPAGE = '+@quote+'RAW'+@quote+'
  , DATAFILETYPE = '+@quote+'char'+@quote+'
  , ROWTERMINATOR = '+@quote+'\n'+@quote+'
  , FIRSTROW = 1
  , LASTROW = 1
  , TABLOCK
  );'
	IF @debug = 'Y' 
		BEGIN
		IF LEN(@sql) > 2047
			PRINT @sql;
		ELSE
			RAISERROR (@sql, 0, 1) WITH NOWAIT; 
		END;
	EXEC (@sql);
  --drop table
  SET @sql = 'DROP TABLE IF EXISTS [tblBAGCSV_'+@filename+@datesuffix+'];'
  IF @debug = 'Y' 
	  BEGIN
	  IF LEN(@sql) > 2047
		  PRINT @sql;
	  ELSE
		  RAISERROR (@sql, 0, 1) WITH NOWAIT; 
	  END;
  EXEC (@sql);
  --create table
  SELECT @sql = 'CREATE TABLE [tblBAGCSV_'+@filename+@datesuffix+'] ('+@crlf+'['+REPLACE(SUBSTRING(t1.OneColumn,2,LEN(t1.OneColumn)-2),'","','] varchar(max) NULL,'+@crlf+'[')+'] varchar(max) NULL);'
  FROM [tmp_One_Column] t1
  ;
  IF @debug = 'Y' 
	  BEGIN
	  IF LEN(@sql) > 2047
		  PRINT @sql;
	  ELSE
		  RAISERROR (@sql, 0, 1) WITH NOWAIT; 
	  END;
  EXEC (@sql);
  --load data
  SET @sql = '
  BULK INSERT [tblBAGCSV_'+@filename+@datesuffix+']
  FROM '+@quote+@filedirectory+'\'+@filename+@datesuffix+@fileextension+@quote+'
  WITH (
    CODEPAGE = '+@quote+'RAW'+@quote+'
  , FORMAT = '+@quote+'CSV'+@quote+'
  --, DATAFILETYPE = '+@quote+'char'+@quote+'
  , ROWTERMINATOR = '+@quote+'\n'+@quote+'
  , FIRSTROW = 2
  , TABLOCK
  );'
  IF @debug = 'Y' 
	  BEGIN
	  IF LEN(@sql) > 2047
		  PRINT @sql;
	  ELSE
		  RAISERROR (@sql, 0, 1) WITH NOWAIT; 
	  END;
  EXEC (@sql);
-- update SYNONYMS
  SET @sql = 'DROP SYNONYM IF EXISTS [tblBAGCSV_'+SUBSTRING(@filename,5,3)+'_Current];'
  IF @debug = 'Y' 
	  BEGIN
	  IF LEN(@sql) > 2047
		  PRINT @sql;
	  ELSE
		  RAISERROR (@sql, 0, 1) WITH NOWAIT; 
	  END;
  EXEC (@sql);
  SET @sql = 'CREATE SYNONYM [tblBAGCSV_'+SUBSTRING(@filename,5,3)+'_Current] FOR [tblBAGCSV_'+@filename+@datesuffix+'];'
  IF @debug = 'Y' 
	  BEGIN
	  IF LEN(@sql) > 2047
		  PRINT @sql;
	  ELSE
		  RAISERROR (@sql, 0, 1) WITH NOWAIT; 
	  END;
  EXEC (@sql);
	--END CSV FILE LOADING
  --END   DO SOMETHING WITHIN CURSOR
  FETCH NEXT FROM bag_csv_file   
    INTO @filename;
END   
CLOSE bag_csv_file;  
DEALLOCATE bag_csv_file;  

-- finished
SET @msg = 'all BAG csv loaded'
IF @debug = 'Y' 
	BEGIN
	IF LEN(@msg) > 2047
		PRINT @msg;
	ELSE
		RAISERROR (@msg, 0, 1) WITH NOWAIT; 
	END;

--ALL BAG loaded



