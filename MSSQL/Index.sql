IF OBJECT_ID('dbo.create_SH_fuzzy_search_index') IS NOT NULL EXEC('DROP PROCEDURE dbo.create_SH_fuzzy_search_index')
IF OBJECT_ID('dbo.create_SH_fuzzy_search_join')  IS NOT NULL EXEC('DROP PROCEDURE dbo.create_SH_fuzzy_search_join')
GO

SET QUOTED_IDENTIFIER, ANSI_NULLS ON
GO

/*
EXEC dbo.create_SH_fuzzy_search_index
    @schema_name            = 'dbo',
    @table_name             = 'customers', @table_id_field_name = 'id',
    --@table_name             = 'employees', @table_id_field_name = 'id',
    @table_mock_field_name  = 'Gender',
    @postfix                = 'full_name',
    @template               = 'ISNULL(first_name, '''') + ''%'' + ISNULL(patronomyc_name, '''') + ''%'' + ISNULL(last_name, '''')',
    @delimiter              = '%',
                            
    @h_table                = '',
    @hc_name                = '',
    @hc_table               = '',
    @ham_dist_func          = '',
    @h_table_join           = '',
    @hc_table_join          = '',
    @hc_table_case_col      = '',
    @hc_table_case_col_name = '',
    @row_size               = NULL,
                            
    @h_schema_name          = 'ix',
    @codepage               = '65001',
    @n                      = '1',
    @hashSize               = '32',
    @hashChunkSize          = '8',
    @kCombCount             = '2',
    @nGramHashModes         = '0',
    @salt_filter            = '0',
                            
    @is_del                 = 0,
    @top                    = 999999,
    @DEBUG                  = 1
*/
CREATE PROCEDURE dbo.create_SH_fuzzy_search_index
    @schema_name            /*n*/nvarchar(100),
    @table_name             /*n*/nvarchar(500),
    @table_id_field_name    /*n*/nvarchar(100),
    @table_mock_field_name  /*n*/nvarchar(100),
    @postfix                /*n*/nvarchar(100) = 'full_name',
    @template               /*n*/nvarchar(500),
    @delimiter              /*n*/nvarchar(50)  = '%',
                            
    @h_table                /*n*/nvarchar(200) OUT,
    @hc_name                /*n*/nvarchar(200) OUT,
    @hc_table               /*n*/nvarchar(200) OUT,
    @ham_dist_func          /*n*/nvarchar(100) OUT,
    @h_table_join           /*n*/nvarchar(MAX) OUT,
    @hc_table_join          /*n*/nvarchar(MAX) OUT,
    @hc_table_case_col      /*n*/nvarchar(MAX) OUT,
    @hc_table_case_col_name /*n*/nvarchar(MAX) OUT,
    @row_size               /*n*/int           OUT,
                            
    @h_schema_name               nvarchar(10)  = 'ix',
    @codepage                    nvarchar(10)  = '',
    @n                           nvarchar(10)  = '1',
    @hashSize                    nvarchar(10)  = '32',
    @hashChunkSize               nvarchar(10)  = '8',
    @kCombCount                  nvarchar(10)  = '2',
    @nGramHashModes              nvarchar(MAX) = '0'/*classic*/,         -- SELECT * FROM dbo.GetNGramHashModes('2, 0, 1, 3', ', ', 1)
    @salt_filter                 nvarchar(MAX) = '0'/*without filters*/, -- SELECT * FROM dbo.f_STRING_SPLIT   ('0', ', ', 1)             

    @is_del                      int           = 0,
    @top                         bigint        = 999999999,
    @DEBUG                       int           = 0
AS
BEGIN
    SET XACT_ABORT, NOCOUNT ON;

    IF     (SELECT COUNT(*) FROM dbo.GetNGramHashModes(@nGramHashModes, ', ', 1)) > 1
       AND (SELECT COUNT(*) FROM dbo.f_STRING_SPLIT   (@salt_filter,    ', ', 1)) > 1
    BEGIN
        SELECT 'You can''t pass multiple @nGramHashModes and multiple @salt_filter'
        RETURN
    END

    DECLARE @hashSizeByte      nvarchar(10) = CEILING(CAST(@hashSize      AS float)/8)
    DECLARE @hashChunkSizeByte nvarchar(10) = CEILING(CAST(@hashChunkSize AS float)/8)
    DECLARE @kCombSizeByte     nvarchar(10) = (CAST(@hashChunkSizeByte AS int) + 1/*for i_chunk*/) * CAST(@kCombCount AS int)
    DECLARE @hashChunkCount    nvarchar(10) = CAST(@hashSize AS int)/CAST(@hashChunkSize AS int)
    
  
    SET     @row_size                       = CAST(@hashSizeByte AS int) + CAST(@kCombSizeByte AS int) * (SELECT COUNT(*) AS cnt FROM dbo.SplitSignatureHashKComb(0/*just an int*/, 32/@hashChunkCount, @kCombCount))  

   ;WITH var_templates AS
    (
                  SELECT '@h_table'   AS var_name, '@table_name_@postfix_H_@ngram_x@hashSize'                                                 AS var_template
        UNION ALL SELECT '@hc_name',                                    'H_@ngram_x@hashSize_C_x@hashChunkSize_K_@kCombCount/@hashChunkCount'
        UNION ALL SELECT '@hc_table',              '@table_name_@postfix_H_@ngram_x@hashSize_C_x@hashChunkSize_K_@kCombCount/@hashChunkCount'
    ),
    vars AS
    (
        SELECT var_name,
               REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
               var_template,
               '@postfix',        @postfix),
               '@n',              @n),
               '@hashSize',       @hashSize),
               '@hashChunkSize',  @hashChunkSize),
               '@table_name',     @table_name),
               '@kCombCount',     @kCombCount), 
               '@hashChunkCount', @hashChunkCount) 
               AS var_value
        FROM var_templates
    )
    SELECT @h_table  = MAX(CASE WHEN var_name = '@h_table'  THEN var_value ELSE '' END),
           @hc_name  = MAX(CASE WHEN var_name = '@hc_name'  THEN var_value ELSE '' END),
           @hc_table = MAX(CASE WHEN var_name = '@hc_table' THEN var_value ELSE '' END)
    FROM vars

   ;WITH modes AS
    (
        SELECT CAST(modes.rn AS nvarchar(MAX))                        AS rn,
               CAST(modes.i  AS nvarchar(MAX))                        AS i,
               mode + ISNULL('_' + NULLIF(salt_filters.str, '0'), '') AS mode,
               salt_filters.str                                       AS saltFilter
        FROM       dbo.GetNGramHashModes(@nGramHashModes, ', ', 1) AS modes
        CROSS JOIN dbo.f_STRING_SPLIT   (@salt_filter,    ', ', 1) AS salt_filters
    )
    SELECT * INTO #modes FROM modes

    DECLARE @h_table_hash_fields_create   nvarchar(MAX) = STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ', '    + '[' + mode + '] varbinary(@hashSizeByte)'                                                                                                FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 2, '')
    DECLARE @hc_table_hash_fields_create  nvarchar(MAX) = STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ', '    + '[' + mode + '] varbinary(@kCombSizeByte)'                                                                                               FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 2, '')
    DECLARE @h_table_hash_fields          nvarchar(MAX) = STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ', '    + '[' + mode + ']'                                                                                                                         FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 2, '')
    DECLARE @hc_table_hash_fields         nvarchar(MAX) = STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ', '    + '[' + mode + ']'                                                                                                                         FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 2, '')
    DECLARE @hc_table_hash_fields_ix      nvarchar(MAX) = STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ' '     + 'CREATE INDEX [@hc_table.' + mode + '] ON [@h_schema_name].[@hc_table] ([' + mode + '])'                                                 FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 1, '')
    DECLARE @hc_table_hash_fields_content nvarchar(MAX) = STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ', '    + '[' + mode + '].iChunkKComb'                                                                                                             FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 2, '')
    DECLARE @h_table_hash_fields_content  nvarchar(MAX) = STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ', '    + 'dbo.GetSignatureHash('/*str           =*/ + 'I.template'           + ', '                                                                                          
                                                                                                                                          /*nSize         =*/ + '@n'                   + ', '                                                                                                    
                                                                                                                                          /*delimiter     =*/ + '''@delimiter'''       + ', '                                                                                       
                                                                                                                                          /*codepage      =*/ + '@codepage'            + ', '                                                                                            
                                                                                                                                          /*nGramHashMode =*/ + i + '/*' + mode + '*/' + ', '                                                                                  
                                                                                                                                          /*saltFilter    =*/ + saltFilter             + ', '                                                                                               
                                                                                                                                          /*hashSize      =*/ + '@hashSize'            +                                                                    
                                                                                                                                       ')'                                                                                                   FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 2, '')
                                                                                                                                                                                                                                                           
    DECLARE @hc_table_hash_fields_cross   nvarchar(MAX) = STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ' '     + 'CROSS APPLY dbo.SplitSignatureHashKComb('/*hash      =*/ + '[' + mode + ']' + ', '                                       
                                                                                                                                                             /*chunkSize =*/ + @hashChunkSize   + ', '                                       
                                                                                                                                                             /*kComb     =*/ + @kCombCount      +                                            
                                                                                                                                                          ') AS [' + mode + ']'                                                              FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 1, '')
    DECLARE @hc_table_hash_fields_cross_1 nvarchar(MAX) =       (SELECT TOP(1) '[' + mode + '].i'                                                                                                                                            FROM #modes)           
    SET     @hc_table_hash_fields_cross                += ' WHERE ' +                                                                                                                                                                                      
                                                          STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ' AND ' + @hc_table_hash_fields_cross_1 + ' = [' + mode + '].i '                                                                    FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 4, '')       
    SET     @ham_dist_func                              = CASE                                                                                                                                                                               
                                                              WHEN CAST(@hashSize AS float) <= 64 THEN 'dbo.HammingDistanceX' + @hashSize                                                                                                    
                                                                                                  ELSE 'dbo.HammingDistanceBytes'                                                                                                            
                                                          END                                                                                                                                                                                
    SET     @h_table_join                               = STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ' OR ' + '(@col_num = ' + rn + ' AND @ham_dist_func(h1.' + mode + ', h2.' + mode + ', ' + @kCombCount + ') <= ' + @kCombCount + ')' FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 4, '')
    SET     @hc_table_join                              = STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ' OR ' + '(@col_num = ' + rn + ' AND hc1.' + mode + ' = hc2.' + mode + ')'                                                          FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 4, '')
    SET     @hc_table_case_col                          = 'CASE WHEN' +                                                                                                                                                                      
                                                          STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ' WHEN @col_num = ' + rn + ' THEN ' + mode                                                                                          FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 5, '') 
                                                          + ' ELSE NULL END'                                                                                                                                                                 
    SET     @hc_table_case_col_name                     = 'CASE WHEN' +                                                                                                                                                                      
                                                          STUFF(dbo.parse_xml_illegal_ASCII_char((SELECT ' WHEN @col_num = ' + rn + ' THEN ' + '''' + mode + ''''                                                                            FROM #modes ORDER BY rn FOR XML PATH(''))), 1, 5, '') 
                                                          + ' ELSE NULL END' 


    IF @DEBUG = 1
    BEGIN
        SELECT * FROM #modes ORDER BY rn
                  SELECT '@postfix' AS name,              @postfix AS value
        UNION ALL SELECT '@h_table',                      @h_table
        UNION ALL SELECT '@hc_name',                      @hc_name
        UNION ALL SELECT '@hc_table',                     @hc_table
        UNION ALL SELECT '@h_table_hash_fields_create',   @h_table_hash_fields_create
        UNION ALL SELECT '@hc_table_hash_fields_create',  @hc_table_hash_fields_create
        UNION ALL SELECT '@hc_table_hash_fields_ix',      @hc_table_hash_fields_ix
        UNION ALL SELECT '@h_table_hash_fields_content',  @h_table_hash_fields_content
        UNION ALL SELECT '@hc_table_hash_fields_cross',   @hc_table_hash_fields_cross
        UNION ALL SELECT '@hc_table_hash_fields_cross_1', @hc_table_hash_fields_cross_1
    END
    --https://stackoverflow.com/a/15405571
    DECLARE @sql nvarchar(MAX) =
                'EXEC (''' + 
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                CAST(N'
				IF OBJECT_ID(''[@h_schema_name].[@hc_table]'')    IS NOT NULL EXEC(''DROP TABLE   [@h_schema_name].[@hc_table]'')
				IF OBJECT_ID(''[@h_schema_name].[TR_@hc_table]'') IS NOT NULL EXEC(''DROP TRIGGER [@h_schema_name].[TR_@hc_table]'')
				IF OBJECT_ID(''[@h_schema_name].[@h_table]'')     IS NOT NULL EXEC(''DROP TABLE   [@h_schema_name].[@h_table]'')
				IF OBJECT_ID(''[@schema_name].[TR_@h_table]'')    IS NOT NULL EXEC(''DROP TRIGGER [@schema_name].[TR_@h_table]'')
				
				GO
				CREATE TABLE [@h_schema_name].[@h_table]
				(
				     [row_num] int NOT NULL
				    ,@h_table_hash_fields_create
				    ,CONSTRAINT [PK_@h_table]                                          PRIMARY KEY ([row_num])
				    ,CONSTRAINT [FK_@h_table.row_num-@table_name.@table_id_field_name] FOREIGN KEY ([row_num]) REFERENCES [@schema_name].[@table_name]([@table_id_field_name]) ON DELETE CASCADE
				)
				CREATE TABLE [@h_schema_name].[@hc_table]
				(
				     [row_num] int NOT NULL
				    ,@hc_table_hash_fields_create
				    ,CONSTRAINT [FK_@hc_table.row_num-@h_table.row_num] FOREIGN KEY ([row_num]) REFERENCES [@h_schema_name].[@h_table]([row_num]) ON DELETE CASCADE
				)
				CREATE CLUSTERED INDEX [@hc_table.row_num] ON [@h_schema_name].[@hc_table] ([row_num])
				@hc_table_hash_fields_ix
				
				GO
				-- generated in dbo.p_create_SH_fuzzy_search_index
				CREATE TRIGGER [@schema_name].[TR_@h_table]
				   ON [@schema_name].[@table_name]
				   AFTER INSERT, UPDATE
				AS
				BEGIN
				    SET NOCOUNT ON;
				
				    DELETE FROM [@h_schema_name].[@h_table]
				    WHERE row_num IN
				    (
				    SELECT I.@table_id_field_name
				    FROM      (SELECT @table_id_field_name, @template AS template FROM Inserted) I
				    LEFT JOIN (SELECT @table_id_field_name, @template AS template FROM Deleted)  D ON D.@table_id_field_name  = I.@table_id_field_name
				                                                                             AND D.template                  <> I.template
				    WHERE D.@table_id_field_name IS NOT NULL -- changed rows
				    )
				
				    INSERT INTO [@h_schema_name].[@h_table] ([row_num], @h_table_hash_fields)
				    SELECT I.@table_id_field_name
				          ,@h_table_hash_fields_content
				    FROM      (SELECT @table_id_field_name, @template AS template FROM Inserted) AS I
				    LEFT JOIN (SELECT @table_id_field_name, @template AS template FROM Deleted)  AS D ON D.@table_id_field_name  = I.@table_id_field_name
				                                                                                     AND D.template             <> I.template
				    LEFT JOIN [@h_schema_name].[@h_table]                                        AS H ON H.row_num               = I.@table_id_field_name
				    WHERE (   D.@table_id_field_name IS NOT NULL  -- changed rows
				           OR H.row_num              IS     NULL) -- haven''t been inserted yet
				      AND REPLACE(I.template, ''@delimiter'', '''') <> ''''
				END
				
				GO
				-- generated in dbo.p_create_SH_fuzzy_search_index
				CREATE TRIGGER [@h_schema_name].[TR_@hc_table]
				   ON [@h_schema_name].[@h_table]
				   AFTER INSERT, UPDATE
				AS
				BEGIN
				    SET NOCOUNT ON;
				
				    DELETE FROM [@h_schema_name].[@hc_table] WHERE row_num IN (SELECT row_num FROM Inserted)
				
				    INSERT INTO [@h_schema_name].[@hc_table] ([row_num], @hc_table_hash_fields)
				    SELECT [row_num], @hc_table_hash_fields_content
				    FROM Inserted
				    @hc_table_hash_fields_cross
				END
				
				GO
				
				-- SQLServer query optimizer will execute triggers as a single command, upon completion of which it will rebuild the indexes only 1 time, so that''s ok
				DECLARE @timer datetime = GETDATE()
				
				UPDATE TOP (@top) [@schema_name].[@table_name] SET [@mock_update_column] = [@mock_update_column] WHERE 1 = 1
				
				PRINT N''[@h_schema_name].[@hc_table] index creating: '' + CAST(CAST(DATEDIFF(MILLISECOND, @timer, GETDATE()) AS float)/1000 AS nvarchar(MAX))
				
				GO
				IF @DEBUG = 1
				BEGIN
				    SELECT @template, H.*, HC.*
				    FROM       [@schema_name].[@table_name] AS t
				    INNER JOIN [@h_schema_name].[@h_table]  AS H  ON t.[@table_id_field_name] = H.row_num
				    INNER JOIN [@h_schema_name].[@hc_table] AS HC ON H.row_num = HC.row_num
				END
				' AS nvarchar(MAX))
               ,'@h_table_hash_fields_content',  @h_table_hash_fields_content)
               ,'@h_table_hash_fields_create',   @h_table_hash_fields_create)
               ,'@h_table_hash_fields',          @h_table_hash_fields)
               ,'@hc_table_hash_fields_content', @hc_table_hash_fields_content)
               ,'@hc_table_hash_fields_create',  @hc_table_hash_fields_create)
               ,'@hc_table_hash_fields_cross',   @hc_table_hash_fields_cross)
               ,'@hc_table_hash_fields_ix',      @hc_table_hash_fields_ix)
               ,'@hc_table_hash_fields',         @hc_table_hash_fields)

               ,'@h_table',                      @h_table)
               ,'@schema_name',                  @schema_name)
               ,'@h_schema_name',                @h_schema_name)
               ,'@postfix',                      @postfix)
               ,'@hc_table',                     @hc_table)
               ,'@table_name',                   @table_name)
               ,'@table_id_field_name',          @table_id_field_name)
               ,'@hashSizeByte',                 @hashSizeByte)
               ,'@kCombSizeByte',                @kCombSizeByte)
               ,'@template',                     @template)
               ,'@delimiter',                    @delimiter)
               ,'@codepage',                     @codepage)
               ,'@n' ,                           @n COLLATE SQL_Latin1_General_Cp1_CS_AS)
               ,'@hashSize',                     @hashSize)
               ,'@hashChunkSize',                @hashChunkSize)
               ,'@kCombCount',                   @kCombCount)
               ,'@mock_update_column',           @table_mock_field_name)
               ,'''',                            '''''')
               ,'GO',                            '''); ' + REPLICATE(CHAR(13) + CHAR(10), 2) + 'IF @is_del = 0 EXEC(''') + ''');'
               ,'@is_del',                       @is_del)
               ,'@top',                          @top)
               ,'@DEBUG',                        @DEBUG)
               ,CHAR(9),                         '')

    IF @DEBUG = 1
        SELECT GETDATE(), @sql
    EXEC sp_executesql @sql
END

GO


/*
EXEC dbo.create_SH_fuzzy_search_join
    @schema_name_1           = 'dbo',
    @table_name_1            = 'customers', 
    @table_id_field_name_1   = 'id',
    @table_mock_field_name_1 = 'Gender',
    @postfix_1               = 'full_name',
    @template_1              = 'ISNULL(first_name, '''') + ''%'' + ISNULL(patronomyc_name, '''') + ''%'' + ISNULL(last_name, '''')',
    @delimiter_1             = '%',

    @schema_name_2           = 'dbo',
    @table_name_2            = 'employees', 
    @table_id_field_name_2   = 'id',
    @table_mock_field_name_2 = 'Gender',
    @postfix_2               = 'full_name',
    @template_2              = 'ISNULL(first_name, '''') + ''%'' + ISNULL(patronomyc_name, '''') + ''%'' + ISNULL(last_name, '''')',
    @delimiter_2             = '%',

    @h_schema_name           = 'ix',
    @codepage                = '65001',
    @n                       = '1',
    @hashSize                = '32',
    @hashChunkSize           = '8',
    @kCombCount              = '2',
    @nGramHashModes          = '0',
    @salt_filter             = '0',
    @lev_dist                = '1',
                 
    @is_del                  = 0,
    @top                     = 999999,
    @DEBUG                   = 0
*/
CREATE PROCEDURE dbo.create_SH_fuzzy_search_join
    @schema_name_1           nvarchar(100),
    @table_name_1            nvarchar(500),
    @table_id_field_name_1   nvarchar(100),
    @table_mock_field_name_1 nvarchar(100),
    @postfix_1               nvarchar(100),
    @template_1              nvarchar(500),
    @delimiter_1             nvarchar(50),                                  

    @schema_name_2           nvarchar(100),
    @table_name_2            nvarchar(500),
    @table_id_field_name_2   nvarchar(100),
    @table_mock_field_name_2 nvarchar(100),
    @postfix_2               nvarchar(100),
    @template_2              nvarchar(500),
    @delimiter_2             nvarchar(50),                                   

    @h_schema_name           nvarchar(10)  = 'ix',
    @codepage                nvarchar(10)  = '',
    @n                       nvarchar(10)  = '1',
    @hashSize                nvarchar(10)  = '32',
    @hashChunkSize           nvarchar(10)  = '8',
    @kCombCount              nvarchar(10)  = '2',
    @nGramHashModes          nvarchar(MAX) = '0'/*classic*/,         -- SELECT * FROM dbo.GetNGramHashModes('0, 1, 2, 3', ', ', 1)
    @salt_filter             nvarchar(MAX) = '0'/*without filters*/, -- SELECT * FROM dbo.f_STRING_SPLIT   ('0, 1', ', ', 1)
    @lev_dist                nvarchar(10)  = '1',
    
    @is_del                  int           = 0,
    @top                     bigint        = 999999999,
    @DEBUG                   int           = 0
AS BEGIN
    SET XACT_ABORT, NOCOUNT ON;

    DECLARE @h_table_1              nvarchar(MAX),
            @hc_table_1             nvarchar(MAX),
            @h_table_2              nvarchar(MAX),
            @hc_table_2             nvarchar(MAX),
            @TR_h_table_1           nvarchar(MAX),
            @hc_name                nvarchar(MAX),
            @ham_dist_func          nvarchar(MAX),
            @h_table_join           nvarchar(MAX),
            @hc_table_join          nvarchar(MAX),
            @hc_table_case_col      nvarchar(MAX),
            @hc_table_case_col_name nvarchar(MAX),
            @row_size               int

    EXEC dbo.create_SH_fuzzy_search_index
        @schema_name            = @schema_name_1,          
        @table_name             = @table_name_1,           
        @postfix                = @postfix_1,              
        @template               = @template_1,             
        @delimiter              = @delimiter_1,            
        @h_table                = @h_table_1              OUT, 
        @hc_name                = @hc_name                OUT,
        @hc_table               = @hc_table_1             OUT,
        @ham_dist_func          = @ham_dist_func          OUT,
        @h_table_join           = @h_table_join           OUT,
        @hc_table_join          = @hc_table_join          OUT,
        @hc_table_case_col      = @hc_table_case_col      OUT,
        @hc_table_case_col_name = @hc_table_case_col_name OUT,
        @row_size               = @row_size               OUT,
        @table_id_field_name    = @table_id_field_name_1,  
        @table_mock_field_name  = @table_mock_field_name_1,
                                
        @h_schema_name          = @h_schema_name,        
        @codepage               = @codepage,             
        @n                      = @n,                    
        @hashSize               = @hashSize,             
        @hashChunkSize          = @hashChunkSize,        
        @kCombCount             = @kCombCount,           
        @nGramHashModes         = @nGramHashModes,       
        @salt_filter            = @salt_filter,          
                                
        @is_del                 = @is_del,
        @top                    = @top,
        @DEBUG                  = @DEBUG                

    EXEC dbo.create_SH_fuzzy_search_index
        @schema_name            = @schema_name_2,          
        @table_name             = @table_name_2,           
        @postfix                = @postfix_2,              
        @template               = @template_2,             
        @delimiter              = @delimiter_2,            
        @h_table                = @h_table_2              OUT,
        @hc_name                = @hc_name                OUT,
        @hc_table               = @hc_table_2             OUT,  
        @ham_dist_func          = @ham_dist_func          OUT,
        @h_table_join           = @h_table_join           OUT,
        @hc_table_join          = @hc_table_join          OUT,
        @hc_table_case_col      = @hc_table_case_col      OUT,
        @hc_table_case_col_name = @hc_table_case_col_name OUT,
        @row_size               = @row_size               OUT,
        @table_id_field_name    = @table_id_field_name_2,  
        @table_mock_field_name  = @table_mock_field_name_2,
                                
        @h_schema_name          = @h_schema_name,        
        @codepage               = @codepage,             
        @n                      = @n,                    
        @hashSize               = @hashSize,             
        @hashChunkSize          = @hashChunkSize,        
        @kCombCount             = @kCombCount,           
        @nGramHashModes         = @nGramHashModes,       
        @salt_filter            = @salt_filter,          
                                
        @is_del                 = @is_del,
        @top                    = @top,
        @DEBUG                  = @DEBUG                


    DECLARE @func_name nvarchar(400) = REPLACE(REPLACE(REPLACE(
                                       '@table_name_1_@postfix_1-@hc_table_2',
                                       '@table_name_1', @table_name_1),
                                       '@hc_table_2',   @hc_table_2),
                                       '@postfix_1',    @postfix_1)

    SELECT @TR_h_table_1 = '[TR_' + @h_table_1  + ']',
           @h_table_1    = '['    + @h_table_1  + ']',
           @h_table_2    = '['    + @h_table_2  + ']',
           @hc_table_1   = '['    + @hc_table_1 + ']',
           @hc_table_2   = '['    + @hc_table_2 + ']'

    BEGIN--Aligning/prettyfication
        DECLARE @h_table_len  int = (SELECT MAX([len]) FROM (SELECT LEN(@h_table_1)    AS [len] UNION SELECT LEN(@h_table_2))    [len])
        DECLARE @hc_table_len int = (SELECT MAX([len]) FROM (SELECT LEN(@hc_table_1)   AS [len] UNION SELECT LEN(@hc_table_2))   [len])
        DECLARE @template_len int = (SELECT MAX([len]) FROM (SELECT LEN(@template_1)   AS [len] UNION SELECT LEN(@template_2))   [len])
        DECLARE @table_len    int = (SELECT MAX([len]) FROM (SELECT LEN(@table_name_1) AS [len] UNION SELECT LEN(@table_name_2)) [len])
        DECLARE @table_id_len int = (SELECT MAX([len]) FROM (SELECT LEN(@postfix_1)    AS [len] UNION SELECT LEN(@postfix_2))    [len])

        SET @h_table_1    = LEFT(@h_table_1    + SPACE(@h_table_len),  @h_table_len)
        SET @h_table_2    = LEFT(@h_table_2    + SPACE(@h_table_len),  @h_table_len)
                                               
        SET @hc_table_1   = LEFT(@hc_table_1   + SPACE(@hc_table_len), @hc_table_len)
        SET @hc_table_2   = LEFT(@hc_table_2   + SPACE(@hc_table_len), @hc_table_len)
                                               
        SET @table_name_1 = LEFT(@table_name_1 + SPACE(@table_len),    @table_len)
        SET @table_name_2 = LEFT(@table_name_2 + SPACE(@table_len),    @table_len)
                                               
        SET @postfix_1    = LEFT(@postfix_1    + SPACE(@table_id_len), @table_id_len)
        SET @postfix_2    = LEFT(@postfix_2    + SPACE(@table_id_len), @table_id_len)
                                               
        SET @template_1   = LEFT(@template_1   + SPACE(@template_len), @template_len)
        SET @template_2   = LEFT(@template_2   + SPACE(@template_len), @template_len)
    END

    DECLARE @core          nvarchar(MAX) =
                   'residual_chunks AS
 				    (
 				        SELECT hc1.row_num AS rn1,
 				               hc2.row_num AS rn2
 				        FROM       [@h_schema_name].@hc_table_1 AS hc1
 				        INNER JOIN [@h_schema_name].@hc_table_2 AS hc2 ON @hc_table_join
 				    ),
 				    residual_ham_dist AS
 				    (
 				        SELECT DISTINCT
 				               rn1, rn2
 				        FROM             residual_chunks AS residual
 				        INNER JOIN [@h_schema_name].@h_table_1 AS h1 ON residual.rn1 = h1.row_num
 				        INNER JOIN [@h_schema_name].@h_table_2 AS h2 ON residual.rn2 = h2.row_num
 				        WHERE @h_table_join
 				    ),
 				    residual_lev_dist AS -- query optimizer leaves this CTE for last, because it''s necessary to JOIN the templates, which is what we want
 				    (
 				        SELECT residual.rn1, residual.rn2, t1.templ1, t2.templ2
 				        FROM       residual_ham_dist AS residual
 				        INNER JOIN (SELECT @table_id_field_name_1 AS rn1, @template_1 AS templ1 FROM @schema_name_1.@table_name_1) AS t1 ON t1.rn1 = residual.rn1
 				        INNER JOIN (SELECT @table_id_field_name_2 AS rn2, @template_2 AS templ2 FROM @schema_name_2.@table_name_2) AS t2 ON t2.rn2 = residual.rn2
 				        WHERE (@only_fuzzy = 0 AND dbo.LevenshteinDistanceString(t1.templ1, t2.templ2) <= @lev_dist)
 				           OR (@only_fuzzy = 1 AND dbo.LevenshteinDistanceString(t1.templ1, t2.templ2)  = @lev_dist)
 				    )'

    IF @DEBUG = 1
                  SELECT '@hc_table_join' AS name, @hc_table_join AS value
        UNION ALL SELECT '@h_table_join',          @h_table_join
        UNION ALL SELECT '@core',                  @core


    DECLARE @sql nvarchar(MAX) =
               'EXEC (''' + 
               REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
               CAST(N'
 				IF OBJECT_ID(''[@h_schema_name].[@func_name]'')             IS NOT NULL EXEC(''DROP FUNCTION  [@h_schema_name].[@func_name]'')
 				IF OBJECT_ID(''[@h_schema_name].[@func_name(index_size)]'') IS NOT NULL EXEC(''DROP PROCEDURE [@h_schema_name].[@func_name(index_size)]'')
 				IF OBJECT_ID(''[@h_schema_name].[@func_name(stat)]'')       IS NOT NULL EXEC(''DROP PROCEDURE [@h_schema_name].[@func_name(stat)]'')
 				IF OBJECT_ID(''[@h_schema_name].[@func_name(filter)]'')     IS NOT NULL EXEC(''DROP PROCEDURE [@h_schema_name].[@func_name(filter)]'')
 				
 				GO
 				/*
 				Generated in create_SH_fuzzy_search_join
 				-- usage
 				SELECT * FROM [@h_schema_name].[@func_name](1, 1) AS SH_search
 				*/
 				CREATE FUNCTION [@h_schema_name].[@func_name]
 				(
 				    @col_num    int = 1,
 				    @only_fuzzy int = 1
 				)
 				RETURNS table AS RETURN
 				(
 				 	--DECLARE @col_num int = 1, @only_fuzzy int = 1
 				    WITH
 				    @core
 				    SELECT * FROM residual_lev_dist
 				)
 				
 				GO
 				
 				/*
 				-- generated in create_SH_fuzzy_search_join
 				
 				EXEC [@h_schema_name].[@func_name(index_size)]
 				*/
 				CREATE PROCEDURE [@h_schema_name].[@func_name(index_size)]
 				(
 				    @data     int = NULL OUT,
 				    @row_size int = NULL OUT
 				)
 				AS
 				BEGIN
 				    CREATE TABLE #SpaceUsedResults
 				    (
 				        name       nvarchar(128),
 				        rows       bigint,
 				        reserved   nvarchar(50),
 				        data       nvarchar(50),
 				        index_size nvarchar(50),
 				        unused     nvarchar(50)
 				    );
 				    
 				    EXEC sp_msforeachtable
 				        @command1 = ''INSERT INTO #SpaceUsedResults EXEC sp_spaceused ''''?'''''',
 				        @whereand = ''AND object_id IN (OBJECT_ID(''''[@h_schema_name].' + RTRIM(@h_table_1)  + N'''''),
 				                                        OBJECT_ID(''''[@h_schema_name].' + RTRIM(@hc_table_1) + N'''''),
 				                                        OBJECT_ID(''''[@h_schema_name].' + RTRIM(@h_table_2)  + N'''''),
 				                                        OBJECT_ID(''''[@h_schema_name].' + RTRIM(@hc_table_2) + N'''''))''
 				
 				    INSERT INTO #SpaceUsedResults(name, rows, reserved, data, index_size, unused)
 				    SELECT N''ROLLUP(KB)'', 
 				           CAST(SUM(CAST(REPLACE(rows,       N'' KB'', N'''') AS int)) AS nvarchar(MAX)),
 				           CAST(SUM(CAST(REPLACE(reserved,   N'' KB'', N'''') AS int)) AS nvarchar(MAX)),
 				           CAST(SUM(CAST(REPLACE(data,       N'' KB'', N'''') AS int)) AS nvarchar(MAX)),
 				           CAST(SUM(CAST(REPLACE(index_size, N'' KB'', N'''') AS int)) AS nvarchar(MAX)),
 				           CAST(SUM(CAST(REPLACE(unused,     N'' KB'', N'''') AS int)) AS nvarchar(MAX))
 				    FROM #SpaceUsedResults
 				
 				    IF @data IS NOT NULL
 				    BEGIN
 				        SELECT @data = CAST(data AS int) FROM #SpaceUsedResults WHERE name = N''ROLLUP(KB)'' 
 				    END
 				    ELSE
 				    BEGIN
 				        SELECT * FROM #SpaceUsedResults ORDER BY rows;
 				    END
 				    DROP TABLE #SpaceUsedResults
 				
 				    SET @row_size = @row_size
 				END
 				
 				GO
 				
 				/*
 				-- generated in create_SH_fuzzy_search_join
 				
 				EXEC [@h_schema_name].[@func_name(stat)] @col_num = 1, @only_fuzzy = 1
 				*/
 				CREATE PROCEDURE [@h_schema_name].[@func_name(stat)]
 				(
 				    @col_num    int = 1,
 				    @only_fuzzy int = 1
 				)
 				AS
 				BEGIN
 				    --DECLARE @col_num int = 1, @only_fuzzy int = 1
 				   ;WITH
 				    @core,
 				    residual_stat AS -- in order of ascending selectivity and cost
 				    (
 				        SELECT (SELECT COUNT_BIG(*) FROM [@h_schema_name].@h_table_1) *
 				               (SELECT COUNT_BIG(*) FROM [@h_schema_name].@h_table_2)   AS ''combs'',
 				               (SELECT COUNT_BIG(*) FROM residual_chunks)               AS ''residual_chunks'',
 				               (SELECT COUNT_BIG(*) FROM residual_ham_dist)             AS ''residual_ham_dist'',
 				               (SELECT COUNT_BIG(*) FROM residual_lev_dist)             AS ''residual_lev_dist''
 				    ),
 				    fullness AS
 				    (
 				                  SELECT CAST(@ham_dist_func(CAST(0 AS binary(@hashSizeByte)), @hc_table_case_col, @hashSize) AS float) AS fullness FROM [@h_schema_name].@h_table_1
 				        UNION ALL SELECT CAST(@ham_dist_func(CAST(0 AS binary(@hashSizeByte)), @hc_table_case_col, @hashSize) AS float)             FROM [@h_schema_name].@h_table_2
 				    ),
 				    -- https://stackoverflow.com/a/2026609
 				    med_fullness AS
 				    (
 				        SELECT (
 				                    (SELECT MAX(fullness) FROM (SELECT TOP 50 PERCENT fullness FROM fullness ORDER BY fullness)      AS BottomHalf) +
 				                    (SELECT MIN(fullness) FROM (SELECT TOP 50 PERCENT fullness FROM fullness ORDER BY fullness DESC) AS TopHalf)
 				               ) / 2 AS med_fullness
 				    ),
 				    avg_fullness AS
 				    (
 				        SELECT AVG(fullness) AS avg_fullness FROM fullness
 				    )
 				    SELECT 
 				           ''@hc_name''            AS hash_name,
 				           @hc_table_case_col_name AS salt_name,
 				           combs, 
 				           residual_chunks,   CAST(residual_chunks   AS float)/CAST(NULLIF(combs,             0) AS float) * 100 AS [residual_chunks_%],
 				           residual_ham_dist, CAST(residual_ham_dist AS float)/CAST(NULLIF(residual_chunks,   0) AS float) * 100 AS [residual_ham_dist_%],
 				           residual_lev_dist, CAST(residual_lev_dist AS float)/CAST(NULLIF(residual_ham_dist, 0) AS float) * 100 AS [residual_lev_dist_%],
 				           med_fullness,      CAST(med_fullness      AS float)/CAST(NULLIF(@hashSize,         0) AS float) * 100 AS [med_fullness_%],
 				           avg_fullness,      CAST(avg_fullness      AS float)/CAST(NULLIF(@hashSize,         0) AS float) * 100 AS [avg_fullness_%]
 				    FROM       residual_stat
 				    CROSS JOIN med_fullness
 				    CROSS JOIN avg_fullness
 				
 				END
 				
 				GO
 				/*
 				-- generated in create_SH_fuzzy_search_join
 				
 				-- Usage
 				SELECT ROW_NUMBER() OVER (ORDER BY @table_id_field_name_1) AS [row_number], @table_id_field_name_1 AS rn1
 				INTO [#@func_name(filter)]
 				FROM  [@schema_name_1].[@table_name_1]
 				-- your JOINs
 				WHERE REPLACE(@template_1, ''@delimiter_1'', N'''') <> N'''' -- from [@h_schema_name].@TR_h_table_1
 				-- your predicates
 				
 				CREATE TABLE #output (rn1 int, rn2 int)
 				CREATE TABLE #stat   (i int, datedif int, cnt int)
 				
 				EXEC [@h_schema_name].[@func_name(filter)]
 				    @col_num    = 1,
 				    @only_fuzzy = 1,
 				    @STEP_SIZE  = 10000,
 				    @end_i      = 2
 				
 				SELECT i, SUM(datedif) AS datedif, SUM(cnt) AS cnt FROM #stat GROUP BY i WITH ROLLUP
 				
 				SELECT t.*
 				FROM                       #output            AS o
 				INNER JOIN [@h_schema_name].[@func_name](0, 1) AS t ON o.rn1 = t.rn1
 				                                      AND o.rn2 = t.rn2
 				
 				DROP TABLE [#@func_name(filter)], #output, #stat
 				*/
 				CREATE PROCEDURE [@h_schema_name].[@func_name(filter)]
 				(
 				    @col_num    int = 1,
 				    @only_fuzzy int = 1,
 				    @STEP_SIZE  int = 10000,
 				    @end_i      int = 99999999
 				)
 				AS
 				BEGIN
 				 	--DECLARE @col_num int = 1, @only_fuzzy int = 1, @STEP_SIZE int = 10000, @end_i int = 99999999
 				    DECLARE @sql nvarchar(MAX) =
 				                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
 								''
 								DECLARE @MIN_ROW_NUMBER int = (SELECT MIN([row_number]) FROM [#@func_name(filter)])
 								DECLARE @MAX_ROW_NUMBER int = (SELECT MAX([row_number]) FROM [#@func_name(filter)])
 								
 								DECLARE @CURR_STEP_MIN int = @MIN_ROW_NUMBER
 								DECLARE @CURR_STEP_MAX int
 								
 								DECLARE @timer datetime
 								
 								DECLARE @i int = 1 WHILE @i <= @end_i AND @CURR_STEP_MIN <= @MAX_ROW_NUMBER
 								BEGIN
 								    SET @CURR_STEP_MAX = @CURR_STEP_MIN + @STEP_SIZE
 								
 								    SET @timer = GETDATE();
 								    BEGIN
 								        INSERT INTO #output(rn1, rn2)
 								        SELECT SH_search.rn1, SH_search.rn2
 								        FROM       [@h_schema_name].[@func_name](@col_num, @only_fuzzy) AS SH_search
 								        INNER JOIN                 [#@func_name(filter)]             AS filter    ON SH_search.rn1 = filter.rn1
 								        WHERE filter.[row_number] BETWEEN @CURR_STEP_MIN AND @CURR_STEP_MAX
 								
 								        INSERT INTO #stat(i, datedif, cnt)
 								        SELECT @i, DATEDIFF(SECOND, @timer, GETDATE()), @@ROWCOUNT
 								    END
 								
 								    SELECT @CURR_STEP_MIN = @CURR_STEP_MAX + 1,
 								           @i             = @i             + 1
 								END
 								'',
 				                ''@col_num'',    @col_num),
 				                ''@only_fuzzy'', @only_fuzzy),
 				                ''@STEP_SIZE'',  @STEP_SIZE),
 				                ''@end_i'',      @end_i),
 				                CHAR(9),       '''')
 				
 				    EXEC sp_executesql @sql -- have been made to use dynamic SQL, because otherwise predicates @col_num and @only_fuzzy inside TBF won''t collapse (OR remains), which completely breaks the plan
 				END'
                AS nvarchar(MAX))
                ,'@core',                    @core)

               ,'@schema_name_1',            @schema_name_1)
               ,'@table_name_1',             @table_name_1)
               ,'@table_id_field_name_1',    @table_id_field_name_1)
               ,'@postfix_1',                @postfix_1)
               ,'@template_1',               @template_1)
               ,'@delimiter_1',              @delimiter_1)
               ,'@h_table_1',                @h_table_1)
               ,'@hc_table_1',               @hc_table_1)
               ,'@TR_h_table_1',             @TR_h_table_1)

               ,'@schema_name_2',            @schema_name_2)
               ,'@table_name_2',             @table_name_2)
               ,'@table_id_field_name_2',    @table_id_field_name_2)
               ,'@postfix_2',                @postfix_2)
               ,'@template_2',               @template_2)
               ,'@h_table_2',                @h_table_2)
               ,'@hc_table_2',               @hc_table_2)
               ,'@h_schema_name',            @h_schema_name)
               ,'@func_name',                @func_name)
               ,'@hc_name',                  @hc_name)
               ,'@hc_table_join',            @hc_table_join)
               ,'@h_table_join',             @h_table_join)
               ,'@ham_dist_func',            @ham_dist_func)
               ,'@hc_table_case_col_name',   @hc_table_case_col_name)
               ,'@hc_table_case_col',        @hc_table_case_col)
               ,' = @row_size',              ' = ' + CAST(@row_size AS nvarchar(10)))
               ,'@hashSizeByte',             ROUND(CAST(@hashSize AS float)/8, 0))
               ,'@hashSize',                 @hashSize)
               ,'@lev_dist',                 @lev_dist)

               ,'''',                        '''''')
               ,'GO',                        '''); ' + REPLICATE(CHAR(13) + CHAR(10), 2) + 'IF @is_del = 0 EXEC(''') + ''');'
               ,'@is_del',                   @is_del)
               ,'@DEBUG',                    @DEBUG)
               ,' ' + REPLICATE(CHAR(9), 4), '')
     
     IF @DEBUG = 1
         SELECT GETDATE(), @sql
     EXEC sp_executesql @sql
END

