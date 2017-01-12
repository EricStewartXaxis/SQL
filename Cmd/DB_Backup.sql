DECLARE @name VARCHAR(50) -- database name  
DECLARE @path VARCHAR(256) -- path for backup files  
DECLARE @fileName VARCHAR(256) -- filename for backup  
DECLARE @fileDate VARCHAR(20) -- used for file name

 
-- specify database backup directory
SET @path = 'C:\projects\SQL\Backup\'  
--SET @path = '\\pscfpsp00106.ad.insidemedia.net\dfs\xax\users\eric.stewart\Projects-Backup\SQL_Server\'

 
-- specify filename format
SELECT @fileDate = REPLACE(REPLACE(CONVERT(VARCHAR(20),GETDATE(),107), ' ', '_'), ',', '')

 
DECLARE db_cursor CURSOR FOR  
SELECT name 
FROM master.dbo.sysdatabases 
--WHERE name NOT IN ('master','model','msdb','tempdb')  -- exclude these databases
WHERE name IN ('Production', 'XaxisETL')

 
OPEN db_cursor   
FETCH NEXT FROM db_cursor INTO @name   

 
WHILE @@FETCH_STATUS = 0   
BEGIN   
       SET @fileName = @path + @name + '_' + @fileDate + '.BAK'  
       BACKUP DATABASE @name TO DISK = @fileName  

 
       FETCH NEXT FROM db_cursor INTO @name   
END   

 
CLOSE db_cursor   
DEALLOCATE db_cursor