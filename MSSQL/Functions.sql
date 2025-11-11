SET QUOTED_IDENTIFIER, ANSI_NULLS OFF



IF OBJECT_ID('dbo.LevenshteinDistanceString')        IS NOT NULL EXEC('DROP FUNCTION dbo.LevenshteinDistanceString')
IF OBJECT_ID('dbo.DamerauLevenshteinDistanceString') IS NOT NULL EXEC('DROP FUNCTION dbo.DamerauLevenshteinDistanceString')
IF OBJECT_ID('dbo.HammingDistanceString')            IS NOT NULL EXEC('DROP FUNCTION dbo.HammingDistanceString')           
IF OBJECT_ID('dbo.LevenshteinDistanceBytes')         IS NOT NULL EXEC('DROP FUNCTION dbo.LevenshteinDistanceBytes')        
IF OBJECT_ID('dbo.DamerauLevenshteinDistanceBytes')  IS NOT NULL EXEC('DROP FUNCTION dbo.DamerauLevenshteinDistanceBytes') 
IF OBJECT_ID('dbo.HammingDistanceBytes')             IS NOT NULL EXEC('DROP FUNCTION dbo.HammingDistanceBytes')            
IF OBJECT_ID('dbo.HammingDistanceX8')                IS NOT NULL EXEC('DROP FUNCTION dbo.HammingDistanceX8')        
IF OBJECT_ID('dbo.HammingDistanceX16')               IS NOT NULL EXEC('DROP FUNCTION dbo.HammingDistanceX16')       
IF OBJECT_ID('dbo.HammingDistanceX32')               IS NOT NULL EXEC('DROP FUNCTION dbo.HammingDistanceX32')       
IF OBJECT_ID('dbo.HammingDistanceX64')               IS NOT NULL EXEC('DROP FUNCTION dbo.HammingDistanceX64')     
IF OBJECT_ID('dbo.GetSignatureHash')                 IS NOT NULL EXEC('DROP FUNCTION dbo.GetSignatureHash')         
IF OBJECT_ID('dbo.SplitSignatureHash')               IS NOT NULL EXEC('DROP FUNCTION dbo.SplitSignatureHash')       
IF OBJECT_ID('dbo.SplitSignatureHashKComb')          IS NOT NULL EXEC('DROP FUNCTION dbo.SplitSignatureHashKComb')  
IF OBJECT_ID('dbo.GetNGrams')                        IS NOT NULL EXEC('DROP FUNCTION dbo.GetNGrams')                
IF OBJECT_ID('dbo.f_STRING_SPLIT')                   IS NOT NULL EXEC('DROP FUNCTION dbo.f_STRING_SPLIT')
IF OBJECT_ID('dbo.GetNGramHashMode')                 IS NOT NULL EXEC('DROP FUNCTION dbo.GetNGramHashMode')
IF OBJECT_ID('dbo.GetNGramHashModes')                IS NOT NULL EXEC('DROP FUNCTION dbo.GetNGramHashModes')
IF OBJECT_ID('dbo.xor32')                            IS NOT NULL EXEC('DROP FUNCTION dbo.xor32')

IF OBJECT_ID('dbo.f_GENERATE_SERIES')                IS NOT NULL EXEC('DROP FUNCTION dbo.f_GENERATE_SERIES')
IF OBJECT_ID('dbo.parse_xml_illegal_ASCII_char')     IS NOT NULL EXEC('DROP FUNCTION dbo.parse_xml_illegal_ASCII_char')

--IF EXISTS (SELECT * FROM sys.assemblies WHERE name = 'SHFuzzySearch') EXEC('DROP ASSEMBLY [SHFuzzySearch]')
GO

CREATE FUNCTION                 dbo.LevenshteinDistanceString       (@str1   nvarchar (MAX), @str2   nvarchar (MAX))             RETURNS int WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.DistanceAlgorithmsSql.LevenshteinDistanceString                                                                                
GO                                                                                                                                         
CREATE FUNCTION                 dbo.DamerauLevenshteinDistanceString(@str1   nvarchar (MAX), @str2   nvarchar (MAX))             RETURNS int WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.DistanceAlgorithmsSql.DamerauLevenshteinDistanceString                                                             
GO                                                                                                                             
CREATE FUNCTION                 dbo.HammingDistanceString           (@str1   nvarchar (MAX), @str2   nvarchar (MAX), @limit int) RETURNS int WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.DistanceAlgorithmsSql.HammingDistanceString                                                                        
GO                                                                                                                             
CREATE FUNCTION                 dbo.LevenshteinDistanceBytes        (@bytes1 varbinary(MAX), @bytes2 varbinary(MAX))             RETURNS int WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.DistanceAlgorithmsSql.LevenshteinDistanceBytes                                                                                 
GO                                                                                                                                         
CREATE FUNCTION                 dbo.DamerauLevenshteinDistanceBytes (@bytes1 varbinary(MAX), @bytes2 varbinary(MAX))             RETURNS int WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.DistanceAlgorithmsSql.DamerauLevenshteinDistanceBytes                                                                      
GO                                                                                                                             
CREATE FUNCTION                 dbo.HammingDistanceBytes            (@bytes1 varbinary(MAX), @bytes2 varbinary(MAX), @limit int) RETURNS int WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.DistanceAlgorithmsSql.HammingDistanceBytes                                                                                                                                                        
GO  
--
CREATE FUNCTION                 dbo.HammingDistanceX8  (@x tinyint,        @y tinyint,       @limit tinyint) RETURNS tinyint WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.DistanceAlgorithmsSql.HammingDistanceX8                                                                                          
GO                                                                                                         
CREATE FUNCTION                 dbo.HammingDistanceX16 (@x smallint,       @y smallint,      @limit tinyint) RETURNS tinyint WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.DistanceAlgorithmsSql.HammingDistanceX16                                                                                           
GO                                                                                                         
CREATE FUNCTION                 dbo.HammingDistanceX32 (@x int,            @y int,           @limit tinyint) RETURNS tinyint WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.DistanceAlgorithmsSql.HammingDistanceX32                                                                                           
GO                                                                                                         
CREATE FUNCTION                 dbo.HammingDistanceX64 (@x bigint,         @y bigint,        @limit tinyint) RETURNS tinyint WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.DistanceAlgorithmsSql.HammingDistanceX64                                                                                                                                                                           
GO
--
CREATE FUNCTION            dbo.GetSignatureHash(@str nvarchar (MAX), @n tinyint, @delimiter nvarchar(MAX), @codepage int, @nGramHashMode tinyint, @saltFilter tinyint, @hashSize int) RETURNS varbinary(MAX) WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.SignatureHashSql.GetSignatureHash                                                                            
GO
CREATE FUNCTION            dbo.SplitSignatureHash     (@hash varbinary(MAX), @chunkSize int)                     RETURNS table (i int, iChunk      varbinary(MAX)) WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.SignatureHashSql.SplitSignatureHash                                                                   
GO                                                              
CREATE FUNCTION            dbo.SplitSignatureHashKComb(@hash varbinary(MAX), @chunkSize int, @kCombSize tinyint) RETURNS table (i int, iChunkKComb varbinary(MAX)) WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.SignatureHashSql.SplitSignatureHashKComb                                                                                                                                                                               
GO       
--
CREATE FUNCTION       dbo.GetNGrams       (@str nvarchar (MAX), @n tinyint, @delimiter nvarchar(MAX))                              RETURNS table (i int, i_nGram nvarchar(10))  WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.SplitterSql.GetNGrams                                                                                                                                                                                       
GO                                                                                                                                                                                                                                 
CREATE FUNCTION       dbo.f_STRING_SPLIT  (@str nvarchar (MAX),             @delimiter nvarchar(MAX), @StringSplitOptions tinyint) RETURNS table (i int, [str]   nvarchar(MAX)) WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.SplitterSql.f_STRING_SPLIT                                                                                                                                                                                    
GO
CREATE FUNCTION       dbo.GetNGramHashMode(@nGramHashMode tinyint)                                                                 RETURNS nvarchar(MAX)                        WITH EXECUTE AS CALLER EXTERNAL NAME
SHFuzzySearch.SplitterSql.GetNGramHashMode                                                                                                                                                                                  
GO

--
CREATE FUNCTION     dbo.xor32(@x int, @y int) RETURNS int WITH EXECUTE AS CALLER EXTERNAL NAME
[SHFuzzySearch].[Debug].xor32         
GO

/*
returns names of hashes from string with their IDs

SELECT * FROM dbo.GetNGramHashModes(N'0, 1, 2, 100', ', ', 1)
*/
CREATE FUNCTION dbo.GetNGramHashModes
(
  @str                nvarchar(MAX),
  @delimiter          nvarchar(MAX),
  @StringSplitOptions tinyint
)
RETURNS table
AS 
RETURN
    /*
    DECLARE @str                nvarchar(MAX) = '3, 2, 1, 0',
            @delimiter          nvarchar(MAX) = ' ,',
            @StringSplitOptions tinyint       = 1
    */
    SELECT rn, i, str AS mode
    FROM
    (
        SELECT i AS rn, str AS i, dbo.GetNGramHashMode(CAST(str AS tinyint)) AS str
        FROM dbo.f_STRING_SPLIT(@str, @delimiter, @StringSplitOptions)
    ) t
    WHERE str IS NOT NULL
GO

/*
int:          −2 147 483 648 - 2 147 483 647
unsigned int: 0              - 4 294 967 295

SELECT * FROM dbo.f_GENERATE_SERIES(0, 5, 1)
*/
CREATE FUNCTION dbo.f_GENERATE_SERIES (@start int, @end int, @step int)
RETURNS table 
AS
RETURN
(
    --DECLARE @start int = -500
    --DECLARE @end   int = 16
    --DECLARE @step  int = 1
    --;

    WITH x AS 
    (
	    SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) v(n)
    )
    SELECT TOP (ROUND((@end- @start) / @step, 0) + 1)
	       @start - @step + ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) * @step AS n
    FROM
	    x AS x1, --1           - 10
	    x AS x2, --11          - 100
	    x AS x3, --101         - 1000
	    x AS x4, --1001        - 10 000
	    x AS x5, --10 001      - 100 000
	    x AS x6, --100 001     - 1 000 000
	    x AS x7, --1 000 001   - 10 000 000
	    x AS x8, --10 000 001  - 100 000 000
	    x AS x9  --100 000 001 - 1 000 000 000
)
GO

/*
recovers illegal characters from XML string

SELECT dbo.parse_xml_illegal_ASCII_char(N'aasdsa')
*/
CREATE FUNCTION dbo.parse_xml_illegal_ASCII_char
(
    @str nvarchar(MAX)
)
RETURNS nvarchar(MAX)
AS
BEGIN
    --DECLARE @str nvarchar(MAX) = N''
    --;

    WITH core AS
    (
        SELECT n                                 AS n, 
               CAST(n AS binary(1))              AS hex, 
               CHAR(n)                           AS [char],
               (SELECT CHAR(n) FOR XML PATH('')) AS hex_str_xml
        FROM dbo.f_GENERATE_SERIES(1, 255, 1) AS ASCII_codes
    )
    --SELECT * FROM core --WHERE hex_str_xml <> [char]

    SELECT @str = REPLACE(@str, hex_str_xml, [char]) FROM core WHERE hex_str_xml <> [char]
    RETURN @str
END
GO

