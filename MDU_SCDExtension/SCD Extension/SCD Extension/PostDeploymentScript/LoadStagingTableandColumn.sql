insert into [InsurerAnalyticsSupport].[LoadStagingTable] ([Schema],[Table],[FileName],HeaderRecords,FieldDelimiter,TextQualifier,JobType,[Enabled],PostLoadSQL)

values ('InsurerAnalyticsClaimsStaging','vw__ClaimantStagingTable','ClaimantExtractFile.dat',1,'|','"','Claims',1,
  ' DECLARE @ExtractRunID NVARCHAR(10) = ?;
    DECLARE @AuditItem NVARCHAR(50) = ?;
    DECLARE @AuditSubItem NVARCHAR(50) = ?;
    DECLARE @JobType NVARCHAR(10) = ?;
    DECLARE @param5 NVARCHAR(100) = ?;
    DECLARE @param6 NVARCHAR(100) = ?;
        
    EXEC [InsurerAnalyticsSupport].[ArchiveDuplicates] 
    @ExtractRunID, 
    @JobType, 
    @AuditItem, 
    @AuditSubItem, 
    N''InsurerAnalyticsClaimsStaging.vw__ClaimantStagingTable'',
    N''OriginalClaimantID'',
    N''First''
    SELECT @@ERROR AS Result')

	/* To load metadata into the load staging column table */

	Exec [InsurerAnalyticsSupport].[GenerateLoadStagingMetadata] 'InsurerAnalyticsClaimsStaging','vw__ClaimantStagingTable','Claims'
