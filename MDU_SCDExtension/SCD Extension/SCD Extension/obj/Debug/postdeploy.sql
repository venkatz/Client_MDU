
Exec [InsurerAnalyticsSupport].[BuildExtensionTriggersAndViews]
Update [InsurerAnalyticsSupport].[ExtractDefinition]
set [enable] = 0 
where Name = 'Claimant'
and JobType = 'Claims'


INSERT [InsurerAnalyticsSupport].[ExtractDefinition] ([ExtractDefinitionID], [Name], [SQLStatement], [Enable], [JobType], [DefinitionType], [Description], [VersionNumber], [VersionStartDate]) VALUES (22, N'Claimant', N'SELECT 
	OriginalClaimantID,
	PolicyNumber,
	PolicyHolderID,
	ClaimNumber,
	ClaimType,
	PartyTypeCode,
	PartyType,
	ClaimantFullName,
	OriginalHomeID,
	HomeAddressLine1,
	HomeAddressLine2,
	HomeCityCode,
	HomeCityName,
	HomeStateCode,
	HomeStateName,
	HomePostCode,
	HomeCountryCode,
	HomeCountryName,
	HomeCountyCode,
	HomeCountyName,
	HomeLatitude,
	HomeLongitude,
	OriginalBusinessID,
	BusinessAddressLine1,
	BusinessAddressLine2,
	BusinessCityCode,
	BusinessCityName,
	BusinessStateCode,
	BusinessStateName,
	BusinessPostCode,
	BusinessCountryCode,
	BusinessCountryName,
	BusinessCountyCode,
	BusinessCountyName,
	BusinessLatitude,
	BusinessLongitude,
	OriginalOtherID,
	OtherAddressLine1,
	OtherAddressLine2,
	OtherCityCode,
	OtherCityName,
	OtherStateCode,
	OtherStateName,
	OtherCountyCode,
	OtherCountyName,
	OtherPostCode,
	OtherCountryCode,
	OtherCountryName,
	OtherLatitude,
	OtherLongitude,
	MaxTransID
 FROM OPENQUERY(SQLDB,''
SELECT 
	CAST(CLAIMANT.ROLEID AS NVARCHAR(100)) AS "OriginalClaimantID",
	CAST(RTRIM(REPLICATE(''''0'''',4 - LEN(P.POLICYCOMPANY)) + CAST(P.POLICYCOMPANY AS NCHAR (4)))
								+ RTRIM(REPLICATE(''''0'''',4 - LEN(P.POLICYSUBCOMPANY)) + CAST(P.POLICYSUBCOMPANY AS NCHAR (4))) 
								+ RTRIM(P.PRODUCTCODE) 
								+ RTRIM(REPLICATE(''''0'''',9 - LEN(P.SHORTPOLICYNUM )) + CAST(P.SHORTPOLICYNUM  AS NCHAR (9)))	AS NVARCHAR (100))  AS "PolicyNumber",
	CAST(Client.Client AS NVARCHAR(100)) AS "PolicyHolderID",
	CAST(Claim.ClaimNum AS NVARCHAR(100)) "ClaimNumber",
	CAST(Claim.ClaimType as NVARCHAR(100)) "ClaimType", 
	CAST(Claimant.TPINDICATOR AS NVARCHAR(2)) "PartyTypeCode"    ,
	CAST(CASE WHEN Claimant.TPINDICATOR=0 THEN ''''First'''' WHEN Claimant.TPIndicator=1 THEN ''''Third'''' ELSE ''''Unknown'''' END AS NVARCHAR(100)) "PartyType",
	CAST(RTRIM(ClientName.Title) + '''' '''' + RTRIM(ClientName.FirstName) + '''' '''' + RTRIM(ClientName.Surname) AS NVARCHAR(300))      AS "ClaimantFullName",
	CAST(NULL AS NVARCHAR(100)) AS "OriginalHomeID",
	CAST(HomeAddress.AddrLine1 AS NVARCHAR(300)) AS     "HomeAddressLine1",   
	CAST(HomeAddress.AddrLine2 AS NVARCHAR(300)) AS "HomeAddressLine2",   
	CAST(HomeAddress.City AS NVARCHAR(300)) AS "HomeCityCode",
	CAST(HomeAddress.City AS NVARCHAR(300)) AS "HomeCityName",   
	CAST(HomeAddress.State AS NVARCHAR(100)) AS "HomeStateCode",   
	CAST(HomeStateLookup.Name AS NVARCHAR(300)) AS     "HomeStateName",   
	CAST(HomeAddress.PostCode AS NVARCHAR(100)) AS "HomePostCode",   
	CAST(HomeAddress.Country AS NVARCHAR(300)) AS "HomeCountryCode",   
	CAST(HomeCountryLookup.Name AS NVARCHAR(300)) AS "HomeCountryName",   
	CAST(HomeAddress.CountyNme AS NVARCHAR(300)) AS     "HomeCountyName",   
	CAST(LEFT(UPPER(REPLACE(HomeAddress.COUNTYNME,'''' '''','''''''')),100) AS NVARCHAR(100)) AS "HomeCountyCode",
	CAST(HomeAddress.LATCOORD AS NVARCHAR(100)) AS "HomeLatitude",
	CAST(HomeAddress.LONGCOORD AS NVARCHAR(100)) AS "HomeLongitude",
	CAST(NULL AS NVARCHAR(100)) AS "OriginalBusinessID",
	CAST(BusinessAddress.AddrLine1 AS NVARCHAR(300)) AS     "BusinessAddressLine1",   
	CAST(BusinessAddress.AddrLine2 AS NVARCHAR(300)) AS "BusinessAddressLine2",   
	CAST(BusinessAddress.City AS NVARCHAR(300)) AS "BusinessCityCode",   
	CAST(BusinessAddress.City AS NVARCHAR(300)) AS "BusinessCityName",   
	CAST(BusinessAddress.State AS NVARCHAR(100)) AS "BusinessStateCode",   
	CAST(BusinessStateLookup.Name AS NVARCHAR(300)) AS     "BusinessStateName",   
	CAST(BusinessAddress.PostCode AS NVARCHAR(100)) AS "BusinessPostCode",   
	CAST(BusinessAddress.Country AS NVARCHAR(100)) AS "BusinessCountryCode",   
	CAST(BusinessCountryLookup.Name AS NVARCHAR(300)) AS "BusinessCountryName",   
	CAST(BusinessAddress.CountyNme AS NVARCHAR(300)) AS     "BusinessCountyName",   
	CAST(LEFT(UPPER(REPLACE(BusinessAddress.COUNTYNME,'''' '''','''''''')),100) AS NVARCHAR(100)) AS "BusinessCountyCode",
	CAST(BusinessAddress.LATCOORD AS NVARCHAR(100)) AS "BusinessLatitude",
	CAST(BusinessAddress.LONGCOORD AS NVARCHAR(100)) AS "BusinessLongitude",
	CAST(NULL AS NVARCHAR(100)) AS "OriginalOtherID",
	CAST(OtherAddress.AddrLine1 AS NVARCHAR(300)) AS     "OtherAddressLine1",   
	CAST(OtherAddress.AddrLine2 AS NVARCHAR(300)) AS "OtherAddressLine2",   
	CAST(OtherAddress.City AS NVARCHAR(300)) AS "OtherCityCode",   
	CAST(OtherAddress.City AS NVARCHAR(300)) AS "OtherCityName",   
	CAST(OtherAddress.State AS NVARCHAR(100)) AS "OtherStateCode",   
	CAST(OtherStateLookup.Name AS NVARCHAR(300)) AS     "OtherStateName",   
	CAST(OtherAddress.PostCode AS NVARCHAR(100)) AS "OtherPostCode",   
	CAST(OtherAddress.Country AS NVARCHAR(100)) AS "OtherCountryCode",   
	CAST(OtherCountryLookup.Name AS NVARCHAR(300)) AS "OtherCountryName",   
	CAST(OtherAddress.CountyNme AS NVARCHAR(300)) AS     "OtherCountyName",   
	CAST(LEFT(UPPER(REPLACE(OtherAddress.COUNTYNME,'''' '''','''''''')),100) AS NVARCHAR(100)) AS "OtherCountyCode",
	CAST(OtherAddress.LATCOORD AS NVARCHAR(100)) AS "OtherLatitude",
	CAST(OtherAddress.LONGCOORD AS NVARCHAR(100)) AS "OtherLongitude",
	(SELECT Max(Maxlist) 
	FROM (VALUES 
	(Claim.TransID), (Coalesce(Claimant.TransID,0))) AS value(Maxlist)) AS MaxTransID
FROM CLAIM.CLAIM AS Claim  
LEFT JOIN CLAIM.CLAIMANT AS Claimant  ON Claim.CLAIMNUM = Claimant.CLAIMNUM
LEFT JOIN CLAIM.ROLE AS ClaimantRole  ON ClaimantRole.ID=Claimant.ROLEID
LEFT JOIN CLAIM.POLICYSNAPSHOT AS P ON  Claim.CLAIMNUM = P.CLAIMNUM
LEFT JOIN Client.Client AS Client ON CAST (ClaimantRole.PartyRef AS integer) = CAST (Client.seqcltno AS integer) 
AND CAST(ClaimantRole.PartyDB AS Integer) = CAST (Client.Company AS integer)
LEFT JOIN (Select * from client.name where offsetno = 999999999 and activto = 99999999 and sortseq = 1) AS ClientName
ON CAST (ClientName.client AS integer) = CAST (Client.client AS integer) AND CAST(ClientName.Company AS Integer) = CAST (Client.Company AS integer)
LEFT JOIN (Select * from client.ADDRREF where offsetno = 999999999 and activto = 99999999 and sortseq = 1) AS HomeParty  
ON HomeParty.COMPANY=ClientName.company  AND HomeParty.CLIENT=ClientName.client  
AND HomeParty.ADDRTYPE = ''''HA''''
LEFT JOIN (Select * from client.ADDRREF where offsetno = 999999999 and activto = 99999999 and sortseq = 1) AS BusinessParty  
ON BusinessParty.COMPANY=ClientName.company  AND BusinessParty.CLIENT=ClientName.client  
AND BusinessParty.ADDRTYPE = ''''SA''''
LEFT JOIN (Select * from client.ADDRREF where offsetno = 999999999 and activto = 99999999 and sortseq = 1) AS OtherParty  
ON OtherParty.COMPANY=ClientName.company  AND OtherParty.CLIENT=ClientName.client  
AND OtherParty.ADDRTYPE = ''''OA''''
LEFT JOIN client.ADDRESS AS HomeAddress  ON HomeAddress.addrref=HomeParty.addrref  
LEFT JOIN lookup.XLOOKUPTRANSVIEW AS HomeStateLookup ON HomeStateLookup.ID=HomeAddress.state AND HomeStateLookup.LookupName=''''LKState'''' AND HomeStateLookup.language=''''en''''   
LEFT JOIN lookup.XLOOKUPTRANSVIEW AS HomeCountryLookup ON HomeCountryLookup.ID=HomeAddress.Country AND HomeCountryLookup.LookupName=''''LKCountry'''' AND HomeCountryLookup.language=''''en''''   
LEFT JOIN client.ADDRESS AS BusinessAddress  ON BusinessAddress.addrref=BusinessParty.addrref  
LEFT JOIN lookup.XLOOKUPTRANSVIEW AS BusinessStateLookup ON BusinessStateLookup.ID=BusinessAddress.state AND BusinessStateLookup.LookupName=''''LKState'''' AND BusinessStateLookup.language=''''en''''   
LEFT JOIN lookup.XLOOKUPTRANSVIEW AS BusinessCountryLookup ON BusinessCountryLookup.ID=BusinessAddress.Country AND BusinessCountryLookup.LookupName=''''LKCountry'''' AND BusinessCountryLookup.language=''''en''''   
LEFT JOIN client.ADDRESS AS OtherAddress  ON OtherAddress.addrref=OtherParty.addrref  
LEFT JOIN lookup.XLOOKUPTRANSVIEW AS OtherStateLookup ON OtherStateLookup.ID=OtherAddress.state AND OtherStateLookup.LookupName=''''LKState'''' AND OtherStateLookup.language=''''en''''   
LEFT JOIN lookup.XLOOKUPTRANSVIEW AS OtherCountryLookup ON OtherCountryLookup.ID=OtherAddress.Country AND OtherCountryLookup.LookupName=''''LKCountry'''' AND OtherCountryLookup.language=''''en''''   
WHERE Claim.TransID > @MaxTransId  OR Claimant.TransID > @MaxTransId OR P.TransID > @MaxTransId  OR ClaimantRole.TransID > @MaxTransId '')', 1, N'Claims', NULL, NULL, NULL, NULL)
SET IDENTITY_INSERT [InsurerAnalyticsSupport].[ExtractDefinition] OFF

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


/* Updating the Enable column for older version to 0  */

Update [InsurerAnalyticsSupport].[FactAndDimensionDefinition]
set Enable = 0
where  Name = 'Client Dimension'
AND JobType = 'Claims'

/* Inserting the new metadata for the Extension Procedure */

insert into [InsurerAnalyticsSupport].[FactAndDimensionDefinition] ([Order],name,SQLStatement,[Enable],JobType,DefinitionType,[Description])
values (40,
	   'Client Dimension',
	   'DECLARE	
		@NewRecords int,
		@ChangedRecords int,
		@ExpiredSCD2Records int,
		@RecordsRead int

EXEC	[InsurerAnalyticsSupport].[ProcessClaimsClientDimension__Extension]
		@ExtractRunID = ?,
		@NewRecords = @NewRecords OUTPUT,
		@ChangedRecords = @ChangedRecords OUTPUT,
		@ExpiredSCD2Records = @ExpiredSCD2Records OUTPUT,
		@RecordsRead = @RecordsRead OUTPUT

SELECT	@NewRecords AS N''NewRecords'',
		@ChangedRecords AS N''ChangedRecords'',
		@ExpiredSCD2Records AS N''ExpiredSCD2Records'',
		@RecordsRead AS N''RecordsRead''',

		'Claims',
		'Dimension',
		'Definition to process Claimants into the Client Dimension')
Insert into  [InsurerAnalyticsSupport].[Parameter] (Name,Value,JobType,[Description],[Enabled])
values ('Reporting Month',0,'Claims','Number of months reporting should be done on Fact Periodic snapshot table',1)



GO
