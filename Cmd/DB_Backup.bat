CALL sqlcmd -i"C:/projects/SQL/Cmd/DB_Backup.sql" -o"C:/projects/SQL/Backup/Log.txt" -W -w 1024 -s","
net use h: /d /y
net use h: \\pscfpsp00106.ad.insidemedia.net\dfs\xax\users\%username%
XCOPY C:\projects\SQL\Backup\*.BAK H:\Projects-Backup\SQL_Server\*.BAK /D
