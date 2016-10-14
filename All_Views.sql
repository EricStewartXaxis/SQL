EXEC sp_MSforeachdb '
    IF DB_ID(''?'') > 4
    BEGIN
        USE [?]
        SELECT DB_NAME()
        SELECT  c.text
            FROM    sysobjects o
                    JOIN syscomments c
                        ON c.id = o.id
            WHERE   o.type = ''V'';
    END'
GO
