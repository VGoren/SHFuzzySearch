IF OBJECT_ID('tempdb..#results')               IS NOT NULL EXEC('DROP TABLE #results')
IF OBJECT_ID('dbo.TEST_SH_fuzzy_search_join')  IS NOT NULL EXEC('DROP PROCEDURE dbo.TEST_SH_fuzzy_search_join')
GO
CREATE PROCEDURE dbo.TEST_SH_fuzzy_search_join
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

    @h_schema_name           nvarchar(10),
    @codepage                nvarchar(10),
    @n                       nvarchar(10),
    @hashSize                nvarchar(10),
    @hashChunkSize           nvarchar(10),
    @kCombCount              nvarchar(10),
    @nGramHashModes          nvarchar(MAX),
    @salt_filter             nvarchar(MAX),
    @lev_dist                nvarchar(10),
    
    @is_del                  int           = 0,                             -- change to 1 to clear all
    @top_stat                bigint        = 1000, @top_srch bigint = 5000, -- ...(stat) procedures are expensive 
    @DEBUG                   int,
    --
    @hash_name               nvarchar(MAX)

AS
BEGIN
    SET XACT_ABORT, NOCOUNT ON;
    
    DECLARE @found          int,
            @timer          datetime,
            @data           nvarchar(50) = N'',
            @row_size       float,
            @rn             nvarchar(MAX),
            @nGramHashMode  nvarchar(MAX),
            @sql_stat       nvarchar(MAX),
            @sql_srch_cnt   nvarchar(MAX),
            @sql_srch       nvarchar(MAX),
            @sql_index_size nvarchar(MAX)

    IF OBJECT_ID('tempdb..#nGramHashModes') IS NOT NULL EXEC('DROP TABLE #nGramHashModes')
    CREATE TABLE #nGramHashModes
    (
        rn            int,
        nGramHashMode int
    )
    
    IF @is_del = 0
    BEGIN
        INSERT #nGramHashModes(rn, nGramHashMode) SELECT rn, i FROM dbo.GetNGramHashModes(@nGramHashModes, ', ', 1)
        WHILE(1 = 1)
        BEGIN
            SELECT TOP(1) @rn = rn, @nGramHashMode = nGramHashMode FROM #nGramHashModes; IF @rn IS NULL BREAK;
            BEGIN

                SET @sql_stat       = REPLACE(N'INSERT INTO #results(hash_name, salt_name, stat_combs, residual_chunks, [residual_chunks_%], residual_ham_dist, [residual_ham_dist_%], residual_lev_dist, [residual_lev_dist_%], med_fullness, [med_fullness_%], avg_fullness, [avg_fullness_%]) 
                                                EXEC [ix].[customers_full_name-employees_full_name_@hash_name(stat)] @col_num = 1, @only_fuzzy = 1',                      N'@hash_name', @hash_name)
                SET @sql_srch_cnt   = REPLACE(N'SELECT @found = COUNT(*) FROM [ix].[customers_full_name-employees_full_name_@hash_name](1, 1)',                           N'@hash_name', @hash_name)
                SET @sql_srch       = REPLACE(N'SELECT                *  FROM [ix].[customers_full_name-employees_full_name_@hash_name](1, 1)',                           N'@hash_name', @hash_name)
                SET @sql_index_size = REPLACE(N'EXEC [ix].[customers_full_name-employees_full_name_@hash_name(index_size)] @data = @data OUT, @row_size = @row_size OUT', N'@hash_name', @hash_name)

                --1
                EXEC dbo.create_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1,
                                                     @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, 
                                                     @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashMode, @salt_filter, @lev_dist, @is_del, @top_stat, @DEBUG
                EXEC sp_executesql @sql_stat
                --2
                SET @timer = GETDATE()
                EXEC dbo.create_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1,
                                                     @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, 
                                                     @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashMode, @salt_filter, @lev_dist, @is_del, @top_srch, @DEBUG
                UPDATE #results SET                                                                    time_create = CAST(CAST(DATEDIFF(MILLISECOND, @timer, GETDATE()) AS float)/1000 AS nvarchar(MAX)) WHERE hash_name = @hash_name AND salt_name = (SELECT dbo.GetNGramHashMode(CAST(@nGramHashMode AS tinyint)))
                
                --3
                SET @timer = GETDATE() 
                EXEC sp_executesql @sql_srch_cnt,   N'@found int OUT', @found OUT;

                --EXEC sp_executesql @sql_srch
                UPDATE #results SET lev_dist = @lev_dist, combs = @top_srch*@top_srch, found = @found, time_srch   = CAST(CAST(DATEDIFF(MILLISECOND, @timer, GETDATE()) AS float)/1000 AS nvarchar(MAX)) WHERE hash_name = @hash_name AND salt_name = (SELECT dbo.GetNGramHashMode(CAST(@nGramHashMode AS tinyint)))
                
                --4
                EXEC sp_executesql @sql_index_size, N'@data int OUT, @row_size float OUT', @data OUT, @row_size OUT; 
                
                UPDATE #results SET [data(KB)] = @data, [data_calculated(KB)] = @row_size*2*@top_srch/1024                                                                                                                                      WHERE hash_name = @hash_name AND salt_name = (SELECT dbo.GetNGramHashMode(CAST(@nGramHashMode AS tinyint)))
                
            END
            DELETE FROM #nGramHashModes WHERE rn = @rn; SET @rn = NULL
        END
    END
    EXEC dbo.create_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1,
                                         @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, 
                                         @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, 1     , @top_srch, @DEBUG
END
GO


CREATE TABLE #results
(
    [rn]                  int            IDENTITY(1, 1),
    [hash_name]           nvarchar(100),
    [salt_name]           nvarchar(100),
    [lev_dist]            nvarchar(100),
    [data(KB)]            nvarchar(50),
    [data_calculated(KB)] nvarchar(50),
    [time_create]         nvarchar(MAX),
    [combs]               int,
    [found]               int,
    [time_srch]           nvarchar(MAX),
    [stat_combs]          int,
    [residual_chunks]     int,
    [residual_chunks_%]   float,
    [residual_ham_dist]   int,
    [residual_ham_dist_%] float,
    [residual_lev_dist]   int,
    [residual_lev_dist_%] float,
    [med_fullness]        int,
    [med_fullness_%]      float,
    [avg_fullness]        int,
    [avg_fullness_%]      float
)

DECLARE @schema_name_1           nvarchar(100) = 'dbo',
        @table_name_1            nvarchar(500) = 'customers',
        @table_id_field_name_1   nvarchar(100) = 'id',
        @table_mock_field_name_1 nvarchar(100) = 'Gender',
        @postfix_1               nvarchar(100) = 'full_name',
        @template_1              nvarchar(500) = 'ISNULL(first_name, '''') + ''%'' + ISNULL(patronomyc_name, '''') + ''%'' + ISNULL(last_name, '''')',
        @delimiter_1             nvarchar(50)  = '%',
                                               
        @schema_name_2           nvarchar(100) = 'dbo',
        @table_name_2            nvarchar(500) = 'employees',
        @table_id_field_name_2   nvarchar(100) = 'id',
        @table_mock_field_name_2 nvarchar(100) = 'Gender',
        @postfix_2               nvarchar(100) = 'full_name',
        @template_2              nvarchar(500) = 'ISNULL(first_name, '''') + ''%'' + ISNULL(patronomyc_name, '''') + ''%'' + ISNULL(last_name, '''')',
        @delimiter_2             nvarchar(50)  = '%',
                                               
        @h_schema_name           nvarchar(10)  = 'ix',
        @codepage                nvarchar(10)  = '65001',
        @n                       nvarchar(10)  = '',
        @hashSize                nvarchar(10)  = '',
        @hashChunkSize           nvarchar(10)  = '',
        @kCombCount              nvarchar(10)  = '',
        @nGramHashModes          nvarchar(MAX) = '',
        @salt_filter             nvarchar(MAX) = '',
        @lev_dist                nvarchar(10)  = '',
                                                 
        @is_del                  int           = 0,                             -- change to 1 to clear all
        @top_stat                bigint        = 1000, @top_srch bigint = 5000, -- ...(stat) procedures are expensive 
        @top                     bigint        = 0,
        @DEBUG                   int           = 0,

        @hash_name               nvarchar(100) = ''


SELECT @hash_name = 'H_1gram_x32_C_x8_K_2/4',      @n = '1', @hashSize = '32',   @hashChunkSize = '8',   @kCombCount = '2', @nGramHashModes = '0, 1, 2, 3', @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
SELECT @hash_name = 'H_2gram_x32_C_x8_K_2/4',      @n = '2', @hashSize = '32',   @hashChunkSize = '8',   @kCombCount = '2', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
SELECT @hash_name = 'H_2gram_x32_C_x4_K_4/8',      @n = '2', @hashSize = '32',   @hashChunkSize = '4',   @kCombCount = '4', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name

BEGIN --H_1gram_x32_C_x8_K_2/4 classic_1_2
    DECLARE @found          int,
            @timer          datetime,
            @data           nvarchar(50) = N'',
            @row_size       float
    SELECT  @n              = '1'
           ,@hashSize       = '32'
           ,@hashChunkSize  = '8'
           ,@kCombCount     = '2'
           ,@nGramHashModes = '0'
           ,@salt_filter    = '1, 2'
           ,@lev_dist       = '1'

    IF @is_del = 0
    BEGIN
        EXEC dbo.create_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1,
                                             @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @DEBUG
        EXEC 
        ('
            /*
            -- generated in create_SH_fuzzy_search_join
            
            EXEC [ix].[customers_full_name-employees_full_name_H_1gram_x32_C_x8_K_2/4(stat)] @col_num = 1, @only_fuzzy = 1
            */
            ALTER PROCEDURE [ix].[customers_full_name-employees_full_name_H_1gram_x32_C_x8_K_2/4(stat)]
            (
                @col_num    int = 1,
                @only_fuzzy int = 1
            )
            AS
            BEGIN
                --DECLARE @col_num int = 1, @only_fuzzy int = 1
               ;WITH
                residual_chunks AS
                (
                    SELECT hc1.row_num AS rn1,
                           hc2.row_num AS rn2
                    FROM       [ix].[customers_full_name_H_1gram_x32_C_x8_K_2/4] AS hc1
                    INNER JOIN [ix].[employees_full_name_H_1gram_x32_C_x8_K_2/4] AS hc2 ON hc1.classic_1 = hc2.classic_1
                    INTERSECT
                    SELECT hc1.row_num AS rn1,
                           hc2.row_num AS rn2
                    FROM       [ix].[customers_full_name_H_1gram_x32_C_x8_K_2/4] AS hc1
                    INNER JOIN [ix].[employees_full_name_H_1gram_x32_C_x8_K_2/4] AS hc2 ON hc1.classic_2 = hc2.classic_2
                ),
                residual_ham_dist AS
                (
                    SELECT DISTINCT
                           rn1, rn2
                    FROM             residual_chunks AS residual
                    INNER JOIN [ix].[customers_full_name_H_1gram_x32] AS h1 ON residual.rn1 = h1.row_num
                    INNER JOIN [ix].[employees_full_name_H_1gram_x32] AS h2 ON residual.rn2 = h2.row_num
                    WHERE dbo.HammingDistanceX32(h1.classic_1, h2.classic_1, 2) <= 2
                    INTERSECT
                    SELECT DISTINCT
                           rn1, rn2
                    FROM             residual_chunks AS residual
                    INNER JOIN [ix].[customers_full_name_H_1gram_x32] AS h1 ON residual.rn1 = h1.row_num
                    INNER JOIN [ix].[employees_full_name_H_1gram_x32] AS h2 ON residual.rn2 = h2.row_num
                    WHERE dbo.HammingDistanceX32(h1.classic_2, h2.classic_2, 2) <= 2
                ),
                residual_lev_dist AS -- query optimizer leaves this CTE for last, because it''s necessary to JOIN the templates, which is what we want
                (
                    SELECT residual.rn1, residual.rn2, t1.templ1, t2.templ2
                    FROM       residual_ham_dist AS residual
                    INNER JOIN (SELECT id AS rn1, ISNULL(first_name, '''') + ''%'' + ISNULL(patronomyc_name, '''') + ''%'' + ISNULL(last_name, '''') AS templ1 FROM dbo.customers) AS t1 ON t1.rn1 = residual.rn1
                    INNER JOIN (SELECT id AS rn2, ISNULL(first_name, '''') + ''%'' + ISNULL(patronomyc_name, '''') + ''%'' + ISNULL(last_name, '''') AS templ2 FROM dbo.employees) AS t2 ON t2.rn2 = residual.rn2
                    WHERE (@only_fuzzy = 0 AND dbo.LevenshteinDistanceString(t1.templ1, t2.templ2) <= 1)
                       OR (@only_fuzzy = 1 AND dbo.LevenshteinDistanceString(t1.templ1, t2.templ2)  = 1)
                ),
                residual_stat AS -- in order of ascending selectivity and cost
                (
                    SELECT (SELECT COUNT_BIG(*) FROM [ix].[customers_full_name_H_1gram_x32]) *
                           (SELECT COUNT_BIG(*) FROM [ix].[employees_full_name_H_1gram_x32])   AS ''combs'',
                           (SELECT COUNT_BIG(*) FROM residual_chunks)               AS ''residual_chunks'',
                           (SELECT COUNT_BIG(*) FROM residual_ham_dist)             AS ''residual_ham_dist'',
                           (SELECT COUNT_BIG(*) FROM residual_lev_dist)             AS ''residual_lev_dist''
                ),
                fullness AS
                (
                              SELECT (CAST(dbo.HammingDistanceX32(0, classic_1, 32) AS float) + CAST(dbo.HammingDistanceX32(0, classic_2, 32) AS float))/2 AS fullness FROM [ix].[customers_full_name_H_1gram_x32]
                    UNION ALL SELECT (CAST(dbo.HammingDistanceX32(0, classic_1, 32) AS float) + CAST(dbo.HammingDistanceX32(0, classic_2, 32) AS float))/2            FROM [ix].[employees_full_name_H_1gram_x32]
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
                       ''H_1gram_x32_C_x8_K_2/4'' AS hash_name,
                       ''classic_1_2''        AS salt_name,
                       combs, 
                       residual_chunks,   CAST(residual_chunks   AS float)/CAST(NULLIF(combs,             0) AS float) * 100 AS [residual_chunks_%],
                       residual_ham_dist, CAST(residual_ham_dist AS float)/CAST(NULLIF(residual_chunks,   0) AS float) * 100 AS [residual_ham_dist_%],
                       residual_lev_dist, CAST(residual_lev_dist AS float)/CAST(NULLIF(residual_ham_dist, 0) AS float) * 100 AS [residual_lev_dist_%],
                       med_fullness,      CAST(med_fullness      AS float)/CAST(NULLIF(32,         0) AS float) * 100 AS [med_fullness_%],
                       avg_fullness,      CAST(avg_fullness      AS float)/CAST(NULLIF(32,         0) AS float) * 100 AS [avg_fullness_%]
                FROM       residual_stat
                CROSS JOIN med_fullness
                CROSS JOIN avg_fullness
            
            END
        ')
        
        
        INSERT INTO #results(hash_name, salt_name, stat_combs, residual_chunks, [residual_chunks_%], residual_ham_dist, [residual_ham_dist_%], residual_lev_dist, [residual_lev_dist_%], med_fullness, [med_fullness_%], avg_fullness, [avg_fullness_%]) EXEC [ix].[customers_full_name-employees_full_name_H_1gram_x32_C_x8_K_2/4(stat)] @only_fuzzy = 1
         
        SET @timer = GETDATE()
        EXEC dbo.create_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1,
                                             @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_srch, @DEBUG
        UPDATE #results SET time_create = CAST(CAST(DATEDIFF(MILLISECOND, @timer, GETDATE()) AS float)/1000 AS nvarchar(MAX)) WHERE hash_name = 'H_1gram_x32_C_x8_K_2/4' AND salt_name = 'classic_1_2'
        EXEC 
        ('
            /*
            Generated in create_SH_fuzzy_search_join
            -- usage
            SELECT * FROM [ix].[customers_full_name-employees_full_name_H_1gram_x32_C_x8_K_2/4](1, 1) AS SH_search
            */
            ALTER FUNCTION [ix].[customers_full_name-employees_full_name_H_1gram_x32_C_x8_K_2/4]
            (
                @col_num    int = 1,
                @only_fuzzy int = 1
            )
            RETURNS table AS RETURN
            (
                WITH
                residual_chunks AS
                (
                    SELECT hc1.row_num AS rn1,
                           hc2.row_num AS rn2
                    FROM       [ix].[customers_full_name_H_1gram_x32_C_x8_K_2/4] AS hc1
                    INNER JOIN [ix].[employees_full_name_H_1gram_x32_C_x8_K_2/4] AS hc2 ON hc1.classic_1 = hc2.classic_1
                    INTERSECT
                    SELECT hc1.row_num AS rn1,
                           hc2.row_num AS rn2
                    FROM       [ix].[customers_full_name_H_1gram_x32_C_x8_K_2/4] AS hc1
                    INNER JOIN [ix].[employees_full_name_H_1gram_x32_C_x8_K_2/4] AS hc2 ON hc1.classic_2 = hc2.classic_2
                ),
                residual_ham_dist AS
                (
                    SELECT DISTINCT
                           rn1, rn2
                    FROM             residual_chunks AS residual
                    INNER JOIN [ix].[customers_full_name_H_1gram_x32] AS h1 ON residual.rn1 = h1.row_num
                    INNER JOIN [ix].[employees_full_name_H_1gram_x32] AS h2 ON residual.rn2 = h2.row_num
                    WHERE dbo.HammingDistanceX32(h1.classic_1, h2.classic_1, 2) <= 2
                    INTERSECT
                    SELECT DISTINCT
                           rn1, rn2
                    FROM             residual_chunks AS residual
                    INNER JOIN [ix].[customers_full_name_H_1gram_x32] AS h1 ON residual.rn1 = h1.row_num
                    INNER JOIN [ix].[employees_full_name_H_1gram_x32] AS h2 ON residual.rn2 = h2.row_num
                    WHERE dbo.HammingDistanceX32(h1.classic_2, h2.classic_2, 2) <= 2
                ),
                residual_lev_dist AS -- query optimizer leaves this CTE for last, because it''s necessary to JOIN the templates, which is what we want
                (
                    SELECT residual.rn1, residual.rn2, t1.templ1, t2.templ2
                    FROM       residual_ham_dist AS residual
                    INNER JOIN (SELECT id AS rn1, ISNULL(first_name, '''') + ''%'' + ISNULL(patronomyc_name, '''') + ''%'' + ISNULL(last_name, '''') AS templ1 FROM dbo.customers) AS t1 ON t1.rn1 = residual.rn1
                    INNER JOIN (SELECT id AS rn2, ISNULL(first_name, '''') + ''%'' + ISNULL(patronomyc_name, '''') + ''%'' + ISNULL(last_name, '''') AS templ2 FROM dbo.employees) AS t2 ON t2.rn2 = residual.rn2
                    WHERE (@only_fuzzy = 0 AND dbo.LevenshteinDistanceString(t1.templ1, t2.templ2) <= 1)
                       OR (@only_fuzzy = 1 AND dbo.LevenshteinDistanceString(t1.templ1, t2.templ2)  = 1)
                )
                SELECT * FROM residual_lev_dist
            )
        ')

        SET @timer = GETDATE() SELECT @found = COUNT(*) FROM [ix].[customers_full_name-employees_full_name_H_1gram_x32_C_x8_K_2/4](1, 1) UPDATE #results SET lev_dist = @lev_dist, combs = @top_srch*@top_srch, found = @found, time_srch = CAST(CAST(DATEDIFF(MILLISECOND, @timer, GETDATE()) AS float)/1000 AS nvarchar(MAX)) WHERE hash_name = 'H_1gram_x32_C_x8_K_2/4' AND salt_name = 'classic_1_2'
        --                     SELECT                *  FROM [ix].[customers_full_name-employees_full_name_H_1gram_x32_C_x8_K_2/4](1, 1)

        EXEC [ix].[customers_full_name-employees_full_name_H_1gram_x32_C_x8_K_2/4(index_size)] @data = @data OUT, @row_size = @row_size OUT UPDATE #results SET [data(KB)] = @data, [data_calculated(KB)] = @row_size*2*2/*double hash*/*@top_srch/1024 WHERE hash_name = 'H_1gram_x32_C_x8_K_2/4' AND salt_name = 'classic_1_2'
    END 

    EXEC dbo.create_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1,
                                         @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, 
                                         @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, 1      , @top_srch, @DEBUG
END
                                                     
SELECT @hash_name = 'H_1gram_x32_C_x4_K_4/8',      @n = '1', @hashSize = '32',   @hashChunkSize = '4',    @kCombCount = '4', @nGramHashModes = '0, 1, 2, 3', @salt_filter = '0', @lev_dist = '2' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
                                                                                                          
SELECT @hash_name = 'H_1gram_x16_C_x4_K_2/4',      @n = '1', @hashSize = '16',   @hashChunkSize = '4',    @kCombCount = '2', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
SELECT @hash_name = 'H_1gram_x16_C_x2_K_4/8',      @n = '1', @hashSize = '16',   @hashChunkSize = '2',    @kCombCount = '4', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '2' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
                                                                                                          
SELECT @hash_name = 'H_1gram_x64_C_x16_K_2/4',     @n = '1', @hashSize = '64',   @hashChunkSize = '16',   @kCombCount = '2', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
SELECT @hash_name = 'H_1gram_x64_C_x8_K_4/8',      @n = '1', @hashSize = '64',   @hashChunkSize = '8',    @kCombCount = '4', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '2' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
                                                                                                                                                             
SELECT @hash_name = 'H_1gram_x128_C_x32_K_2/4',    @n = '1', @hashSize = '128',  @hashChunkSize = '32',   @kCombCount = '2', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
SELECT @hash_name = 'H_1gram_x128_C_x16_K_4/8',    @n = '1', @hashSize = '128',  @hashChunkSize = '16',   @kCombCount = '4', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '2' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
                                                                                                                                                             
SELECT @hash_name = 'H_1gram_x256_C_x64_K_2/4',    @n = '1', @hashSize = '256',  @hashChunkSize = '64',   @kCombCount = '2', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
SELECT @hash_name = 'H_1gram_x256_C_x32_K_4/8',    @n = '1', @hashSize = '256',  @hashChunkSize = '32',   @kCombCount = '4', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '2' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
                                                                                                                                                             
SELECT @hash_name = 'H_1gram_x512_C_x128_K_2/4',   @n = '1', @hashSize = '512',  @hashChunkSize = '128',  @kCombCount = '2', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
SELECT @hash_name = 'H_1gram_x512_C_x64_K_4/8',    @n = '1', @hashSize = '512',  @hashChunkSize = '64',   @kCombCount = '4', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '2' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
                                                                                                                                                             
SELECT @hash_name = 'H_1gram_x1024_C_x256_K_2/4',  @n = '1', @hashSize = '1024', @hashChunkSize = '256',  @kCombCount = '2', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
SELECT @hash_name = 'H_1gram_x1024_C_x128_K_4/8',  @n = '1', @hashSize = '1024', @hashChunkSize = '128',  @kCombCount = '4', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '2' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
                                                                                                                                                             
SELECT @hash_name = 'H_1gram_x2048_C_x512_K_2/4',  @n = '1', @hashSize = '2048', @hashChunkSize = '512',  @kCombCount = '2', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
SELECT @hash_name = 'H_1gram_x2048_C_x256_K_4/8',  @n = '1', @hashSize = '2048', @hashChunkSize = '256',  @kCombCount = '4', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '2' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
  
SELECT @hash_name = 'H_1gram_x4096_C_x1024_K_2/4', @n = '1', @hashSize = '4096', @hashChunkSize = '1024', @kCombCount = '2', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '1' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
SELECT @hash_name = 'H_1gram_x4096_C_x512_K_4/8',  @n = '1', @hashSize = '4096', @hashChunkSize = '512',  @kCombCount = '4', @nGramHashModes = '0, 3',       @salt_filter = '0', @lev_dist = '2' EXEC dbo.TEST_SH_fuzzy_search_join @schema_name_1, @table_name_1, @table_id_field_name_1, @table_mock_field_name_1, @postfix_1, @template_1, @delimiter_1, @schema_name_2, @table_name_2, @table_id_field_name_2, @table_mock_field_name_2, @postfix_2, @template_2, @delimiter_2, @h_schema_name, @codepage, @n, @hashSize, @hashChunkSize, @kCombCount, @nGramHashModes, @salt_filter, @lev_dist, @is_del, @top_stat, @top_srch, @DEBUG, @hash_name
  

SELECT [rn],
       [hash_name],
       [salt_name],
       [lev_dist],
       [data(KB)],
       [data_calculated(KB)],
       [time_create],
       [combs],
       [found],
       [time_srch],
       [stat_combs],
       [residual_chunks],
       [residual_chunks_%],
       [residual_ham_dist],
       [residual_ham_dist_%],
       --[residual_lev_dist],
       --[residual_lev_dist_%],
       [med_fullness],
       [med_fullness_%],
       [avg_fullness],
       [avg_fullness_%]
FROM #results



SELECT [rn],
       [hash_name],
       [lev_dist],
       [salt_name],
       [data(KB)],
       ROUND([time_srch],         3) AS [time_srch],
       ROUND([residual_chunks_%], 5) AS [residual_chunks_%],
       ROUND([avg_fullness_%],    5) AS [avg_fullness_%]
FROM #results
WHERE rn<=9
ORDER BY rn,
         lev_dist, 
         salt_name,
         CAST([data(KB)] AS int)


SELECT [rn]
       [hash_name],
       [lev_dist],
       [salt_name],
       [found],
       ROUND([time_srch],           3) AS [time_srch],
       ROUND([residual_chunks_%],   5) AS [residual_chunks_%]
FROM #results
WHERE rn IN(5, 6, 11, 12)
ORDER BY rn,
         lev_dist, 
         salt_name,
         CAST([data(KB)] AS int)

SELECT [hash_name],
       [lev_dist],
       [salt_name],
       [data(KB)],
       ROUND([data_calculated(KB)], 0) AS [data_calculated(KB)],
       ROUND([time_srch],           3) AS [time_srch],
       ROUND([residual_chunks_%],   5) AS [residual_chunks_%],
       ROUND([avg_fullness_%],      5) AS [avg_fullness_%]
FROM #results
WHERE salt_name IN ('classic', 'salt_i_word') and hash_name like '%1gram%'
ORDER BY lev_dist, 
         salt_name,
         CAST([data(KB)] AS int)




