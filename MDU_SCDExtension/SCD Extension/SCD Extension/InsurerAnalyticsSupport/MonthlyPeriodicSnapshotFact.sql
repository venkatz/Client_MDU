CREATE PROC   [InsurerAnalyticsSupport].[MonthlyPeriodicSnapshotFact] 
AS
BEGIN
     DECLARE @TransactionStartTime datetime = CURRENT_TIMESTAMP;
    BEGIN TRY
        DECLARE @ClaimNumber varchar( 100 ),
                @ReportingDate varchar ( 20 ),
                @YearMonth int,
                @CountRows int,
                @ReportingMonth varchar( 20 ),
                @ExtractRunID varchar( 10 )
        SET @CountRows = ( SELECT [Parameter].[value]
                             FROM InsurerAnalyticsSupport.[Parameter]
                             WHERE [Parameter].Name = 'Reporting Month' )
        
	   SET @ReportingDate =  EOMONTH(( SELECT CONVERT( date,DATEADD( mm,-@CountRows,GETDATE( )))))
        
	   SET @ReportingMonth = LEFT(( SELECT CONVERT( varchar( 25 ),DATEADD( dd,-(DAY( DATEADD( mm,1,@ReportingDate ))-1),DATEADD( mm,1,@ReportingDate )),112 )),6 )
        
	   SET @ExtractRunID = ( SELECT [Parameter].[Value] FROM InsurerAnalyticsSupport.[Parameter]
                                WHERE 
						       [Parameter].JobType = 'Claims'
                                 AND [Parameter].Name = 'ExtractRunID' )

       
	   IF @CountRows > 0
            BEGIN
                IF OBJECT_ID( 'tempdb..#ClientWithOpenClaims' ) IS  NOT NULL
                    BEGIN
                        DROP TABLE insurerAnalyticsSupport.#ClientWithOpenClaims
                    END
                CREATE TABLE insurerAnalyticsSupport.#ClientWithOpenClaims( claimnumber varchar( 100 ),
                                                           YearMonth int,
                                                           CountofClaims int )
                ALTER TABLE insurerAnalyticsSupport.#ClientWithOpenClaims ALTER COLUMN claimnumber varchar( 100 ) COLLATE SQL_Latin1_General_CP1_CI_AS
                
/****** To populate Reporting month into the Temp table yearmonth (YYYYMM) **********/
			
			 DECLARE @i int = 1
                IF OBJECT_ID( 'tempdb..#yearmonth' ) IS NOT NULL
                    BEGIN
                        DROP TABLE insurerAnalyticsSupport.#yearmonth
                    END
                CREATE TABLE #yearmonth( YearMonth varchar( 10 ))
                WHILE @i <= @CountRows
                    BEGIN
                        INSERT INTO  #yearmonth ( YearMonth )
                        VALUES ( @ReportingMonth )
                        SET @ReportingMonth = LEFT( REPLACE( DATEADD( mm,1,CONVERT( date,CONCAT( @ReportingMonth,'01' ))),'-','' ),6 )
                        SET @i=@i+1
                    END
				
/********** Cursor to loop through number of months ********/

                DECLARE @YearAndMonth varchar( 10 )
                DECLARE YearMonthCursor CURSOR
                    FOR
                        SELECT #yearmonth.YearMonth
                          FROM InsurerAnalyticsSupport.#yearmonth
               
			 OPEN YearMonthCursor
                FETCH NEXT FROM YearMonthCursor INTO @YearAndMonth
               
			 WHILE @@FETCH_STATUS = 0
                    BEGIN
                        SET @ReportingDate = REPLACE( @ReportingDate,'-','' )
                        INSERT INTO #ClientWithOpenClaims ( claimnumber,
                                           YearMonth,
                                           CountofClaims )
                        SELECT cs.ClaimNumber,
                               @YearAndMonth,
                               COUNT( * )
                          FROM
                               InsurerAnalyticsClaimsFact.ClaimSnapshot cs INNER JOIN InsurerAnalyticsCommonDimension.Dates d
                               ON cs.NotificationDate = d.DateId
                          WHERE
                               cs.NotificationDate <= @ReportingDate
                           AND cs.SettledDate IS NULL
                            OR (cs.NotificationDate <= @ReportingDate
						  AND cs.SettledDate > @ReportingDate)
                          GROUP BY cs.ClaimNumber
                        SET @ReportingDate = REPLACE( eomonth( DATEADD( mm,1,CONVERT( date,@reportingDate ))),'-','' )
                        FETCH NEXT FROM YearMonthCursor INTO @YearAndMonth
                    END
                CLOSE YearMonthCursor
                DEALLOCATE yearmonthcursor

                     
/*********** Inserting into the Table MonthlyClaimsFactPeriodicSnapshot ************/

	   TRUNCATE TABLE InsurerAnalyticsClaimsFact.MonthlyClaimsFactPeriodicSnapshot
	   INSERT INTO   InsurerAnalyticsClaimsFact.MonthlyClaimsFactPeriodicSnapshot ( ClientID,
																	   YearMonth,
																	   NoOfClaims )
	   SELECT v.clientid ,
			 t.yearmonth,
			 COUNT( t.countofclaims )
		  FROM
			 InsurerAnalyticsCommonDimension.VW__Client v 
			 INNER JOIN InsurerAnalyticsClaimsDimension.BridgeClaimantClient b
			 ON v.ClientID = b.ClientID
			 INNER JOIN InsurerAnalyticsSupport.#ClientWithOpenClaims t
			 ON B.ClaimNumber = t.claimnumber
		  WHERE v.EndDate IS NULL  ----Active Client
		  GROUP BY   v.clientId,
				    t.yearmonth

 /*********** Audit **********************/

        DECLARE @TransactionEndTime datetime = CURRENT_TIMESTAMP
	   DECLARE @AuditCount int
        SET @AuditCount = ( SELECT COUNT( * )
                                FROM InsurerAnalyticsClaimsFact.MonthlyClaimsFactPeriodicSnapshot )
			               
	   EXEC InsurerAnalyticsSupport.CreateAuditRecord
	   @JobType = N'Claims',
	   @ExtractRunID = @ExtractRunID,
	   @AuditItem = N'Monthly Periodic Snapshot Fact',
	   @AuditSubItem = N'Monthly Periodic Snapshot Fact',
	   @AuditStatus = N'SUCCESS',
	   @AuditCount = @AuditCount,
	   @AuditComment = N'Records loaded to Monthly periodic Snapshot',
	   @AuditStartDate = @TransactionStartTime,
	   @AuditCompletionDate = @TransactionEndTime,
	   @AuditType = 'Audit';
			
			
END
     
ELSE
  BEGIN
	   SET @TransactionEndTime = CURRENT_TIMESTAMP
	   EXEC InsurerAnalyticsSupport.CreateAuditRecord
	   @JobType = N'Claims',
	   @ExtractRunID = @ExtractRunID,
	   @AuditItem = N'Monthly Periodic Snapshot Fact',
	   @AuditSubItem = N'Monthly Periodic Snapshot Fact',
	   @AuditStatus = N'FAILURE',
	   @AuditCount = @AuditCount,
	   @AuditComment = N'The Reporting month cannot be Null or Zero',
	   @AuditStartDate = @TransactionStartTime,
	   @AuditCompletionDate = @TransactionEndTime,
	   @AuditType = 'Audit',
	   @ErrorDetail = N'The Reporting month cannot be Null or Zero'
			
END
    END TRY
   
    BEGIN CATCH
        DECLARE @ErrorMessage varchar( 2000 ) = NULL;
        SET @TransactionEndTime = CURRENT_TIMESTAMP
	   SET @ErrorMessage = '(error: ' + CONVERT( varchar, ERROR_NUMBER( )) + ', line: ' + CONVERT( varchar, ERROR_LINE( )) + ') ' + ERROR_MESSAGE( ) 
       
	   EXEC InsurerAnalyticsSupport.CreateAuditRecord
	   @JobType = N'Claims',
	   @ExtractRunID = @ExtractRunID,
	   @AuditItem = N'Monthly Periodic Snapshot Fact',
	   @AuditSubItem = N'Monthly Periodic Snapshot Fact',
	   @AuditStatus = N'FAILURE',
	   @AuditCount = @AuditCount,
	   @AuditComment = N'Error in the procedure',
	   @AuditStartDate = @TransactionStartTime,
	   @AuditCompletionDate = @TransactionEndTime,
	   @AuditType = 'Error',
	   @ErrorDetail = @ErrorMessage;
       
END CATCH;
END
