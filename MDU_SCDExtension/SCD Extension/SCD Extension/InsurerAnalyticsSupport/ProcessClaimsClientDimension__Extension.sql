

CREATE PROCEDURE [InsurerAnalyticsSupport].[ProcessClaimsClientDimension__Extension]
@ExtractRunID nvarchar( 10 ),
@NewRecords int = 0 OUTPUT,
@ChangedRecords int = 0 OUTPUT,
@ExpiredSCD2Records int = 0 OUTPUT,
@RecordsRead int = 0 OUTPUT
AS
SET NOCOUNT ON
SET XACT_ABORT ON
SET CONCAT_NULL_YIELDS_NULL OFF;

BEGIN TRY
    
    BEGIN TRAN ALL_OR_NOTHING
    
    DECLARE @BridgeRecordsRead int = 0;
    
    UPDATE InsurerAnalyticsCommonDimension.VW__Client
    
	   SET VW__Client.CounterFlag = NULL;
    
    UPDATE InsurerAnalyticsClaimsDimension.BridgeClaimantClient
	   
	   SET BridgeClaimantClient.CounterFlag = NULL;

    --*
    --* If there no records in the Staging table for this extract run id then quit. Else  do Type 2 SCD update of the Client table.
    --*

    IF EXISTS ( SELECT 1 FROM InsurerAnalyticsClaimsStaging.VW__ClaimantStagingTable
                  WHERE VW__ClaimantStagingTable.ExtractRunID = @ExtractRunID )
        BEGIN

            --*
            --* Populate a temporary table with the new/updated Client data
            --*

            SELECT * INTO #ClaimsClientDimension
              FROM (
				SELECT * FROM (
							 SELECT ROW_NUMBER( ) OVER ( PARTITION BY ClaimantStaging.OriginalClaimantID ORDER BY ClaimantStaging.ClaimNumber ) AS Priority,
							 ClaimantStaging.OriginalClaimantID,
							 ClaimantStaging.PolicyNumber,
							 ClaimantStaging.PolicyHolderID,
							 ClaimantStaging.ClaimNumber,
							 ClaimantStaging.ClaimType,
							 ClaimantStaging.PartyTypeCode,
							 ClaimantStaging.PartyType,
							 ClaimantStaging.ClaimantFullName,
							 ClaimantStaging.OriginalHomeID,
							 ClaimantStaging.HomeAddressLine1,
							 ClaimantStaging.HomeAddressLine2,
							 ClaimantStaging.HomeCityCode,
							 ClaimantStaging.HomeCityName,
							 ClaimantStaging.HomeStateCode,
							 ClaimantStaging.HomeStateName,
							 ClaimantStaging.HomePostCode AS HomePostalCode,
							 ClaimantStaging.HomeCountryCode,
							 ClaimantStaging.HomeCountryName,
							 ClaimantStaging.HomeCountyCode,
							 ClaimantStaging.HomeCountyName,
							 ClaimantStaging.HomeLatitude,
							 ClaimantStaging.HomeLongitude,
							 ClaimantStaging.OriginalBusinessID,
							 ClaimantStaging.BusinessAddressLine1,
							 ClaimantStaging.BusinessAddressLine2,
							 ClaimantStaging.BusinessCityCode,
							 ClaimantStaging.BusinessCityName,
							 ClaimantStaging.BusinessStateCode,
							 ClaimantStaging.BusinessStateName,
							 ClaimantStaging.BusinessPostCode AS BusinessPostalcode,
							 ClaimantStaging.BusinessCountryCode,
							 ClaimantStaging.BusinessCountryName,
							 ClaimantStaging.BusinessCountyCode,
							 ClaimantStaging.BusinessCountyName,
							 ClaimantStaging.BusinessLatitude,
							 ClaimantStaging.BusinessLongitude,
							 ClaimantStaging.OriginalOtherID,
							 ClaimantStaging.OtherAddressLine1,
							 ClaimantStaging.OtherAddressLine2,
							 ClaimantStaging.OtherCityCode,
							 ClaimantStaging.OtherCityName,
							 ClaimantStaging.OtherStateCode,
							 ClaimantStaging.OtherStateName,
							 ClaimantStaging.OtherCountyCode,
							 ClaimantStaging.OtherCountyName,
							 ClaimantStaging.OtherPostCode AS OtherPostalCode,
							 ClaimantStaging.OtherCountryCode,
							 ClaimantStaging.OtherCountryName,
							 ClaimantStaging.OtherLatitude,
							 ClaimantStaging.OtherLongitude,
							 CAST( NULL AS int ) AS ClientID,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 1,ClaimantStaging.HomeCountryCode,ClaimantStaging.HomePostCode,ClaimantStaging.HomeStateName ) AS HomeGeoMapCode1,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 2,ClaimantStaging.HomeCountryCode,ClaimantStaging.HomePostCode,ClaimantStaging.HomeStateName ) AS HomeGeoMapCode2,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 3,ClaimantStaging.HomeCountryCode,ClaimantStaging.HomePostCode,ClaimantStaging.HomeStateName ) AS HomeGeoMapCode3,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 4,ClaimantStaging.HomeCountryCode,ClaimantStaging.HomePostCode,ClaimantStaging.HomeStateName ) AS HomeGeoMapCode4,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 1,ClaimantStaging.BusinessCountryCode,ClaimantStaging.BusinessPostCode,ClaimantStaging.BusinessStateName ) AS BusinessGeoMapCode1,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 2,ClaimantStaging.BusinessCountryCode,ClaimantStaging.BusinessPostCode,ClaimantStaging.BusinessStateName ) AS BusinessGeoMapCode2,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 3,ClaimantStaging.BusinessCountryCode,ClaimantStaging.BusinessPostCode,ClaimantStaging.BusinessStateName ) AS BusinessGeoMapCode3,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 4,ClaimantStaging.BusinessCountryCode,ClaimantStaging.BusinessPostCode,ClaimantStaging.BusinessStateName ) AS BusinessGeoMapCode4,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 1,ClaimantStaging.OtherCountryCode,ClaimantStaging.OtherPostCode,ClaimantStaging.OtherStateName ) AS OtherGeoMapCode1,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 2,ClaimantStaging.OtherCountryCode,ClaimantStaging.OtherPostCode,ClaimantStaging.OtherStateName ) AS OtherGeoMapCode2,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 3,ClaimantStaging.OtherCountryCode,ClaimantStaging.OtherPostCode,ClaimantStaging.OtherStateName ) AS OtherGeoMapCode3,
							 InsurerAnalyticsSupport.DeriveGeoMapCode( 4,ClaimantStaging.OtherCountryCode,ClaimantStaging.OtherPostCode,ClaimantStaging.OtherStateName ) AS OtherGeoMapCode4
							 
							 FROM InsurerAnalyticsClaimsStaging.VW__ClaimantStagingTable AS ClaimantStaging
							 WHERE ClaimantStaging.ExtractRunID = @ExtractRunID 
							 ) AS Claimants
								  
							  WHERE Claimants.Priority = 1 ) AS TempTable;

            --*
            --* Get Count of Records before load
            --*

            SET @RecordsRead = ( SELECT COUNT( * )
                                   FROM ( SELECT ROW_NUMBER( ) OVER ( PARTITION BY #ClaimsClientDimension.PolicyHolderID ORDER BY #ClaimsClientDimension.OriginalClaimantID ) AS RowNum
                                            FROM InsurerAnalyticsSupport.#ClaimsClientDimension
                                            WHERE #ClaimsClientDimension.PolicyholderID IS NOT NULL ) AS Client
                                   WHERE Client.Rownum = 1 );

            --*
            --* merge policyholder
            --*

            /****** Creating temp table for Distinct client based on policy holder id *******/

            SELECT * INTO #DistinctClientPolicyNotNull
              FROM ( SELECT * FROM
						  
						  (SELECT *,ROW_NUMBER( ) OVER ( PARTITION BY #ClaimsClientDimension.PolicyHolderID ORDER BY #ClaimsClientDimension.OriginalClaimantID ) AS RowNum
						    FROM InsurerAnalyticsSupport.#ClaimsClientDimension
						    WHERE #ClaimsClientDimension.PolicyholderID IS NOT NULL ) AS TblA 
						    )
						    
						    AS B
						    WHERE B.Rownum = 1

            /******  Performing Merge *******/

            MERGE
            InsurerAnalyticsCommonDimension.VW__Client AS Target
            USING
            InsurerAnalyticsSupport.#DistinctClientPolicyNotNull AS Source
            ON
            Target.ClientCode = Source.PolicyHolderID
            WHEN NOT MATCHED
                  THEN INSERT
                  (
                  ClientCode,
                  ClientName,
                  ClaimantID,
                  claimtype,
                  PolicyHolderID,
                  HomeAddressOriginalID,
                  HomeAddressLine1,
                  HomeAddressLine2,
                  HomeCity,
                  HomeCountyName,
                  HomeState,
                  HomeCountry,
                  HomePostalCode,
                  BusinessAddressOriginalID,
                  BusinessAddressLine1,
                  BusinessAddressLine2,
                  BusinessCityName,
                  BusinessCountyName,
                  BusinessStateName,
                  BusinessCountryName,
                  BusinessPostalCode,
                  OtherAddressOriginalID,
                  OtherAddressLine1,
                  OtherAddressLine2,
                  OtherCityName,
                  OtherCountyName,
                  OtherStateName,
                  OtherCountryName,
                  OtherPostalCode,
                  HomeGeoMapCode1,
                  HomeGeoMapCode2,
                  HomeGeoMapCode3,
                  HomeGeoMapCode4,
                  BusinessGeoMapCode1,
                  BusinessGeoMapCode2,
                  BusinessGeoMapCode3,
                  BusinessGeoMapCode4,
                  OtherGeoMapCode1,
                  OtherGeoMapCode2,
                  OtherGeoMapCode3,
                  OtherGeoMapCode4,
                  HomeLatitude,
                  HomeLongitude,
                  BusinessLatitude,
                  BusinessLongitude,
                  OtherLatitude,
                  OtherLongitude,
                  CounterFlag,
                  StartDate
                  )
                  VALUES
                  (
                  Source.PolicyHolderID,
                  Source.ClaimantFullName,
                  Source.OriginalClaimantID,
                  source.claimtype,
                  Source.PolicyHolderID,
                  Source.OriginalHomeID,
                  Source.HomeAddressLine1,
                  Source.HomeAddressLine2,
                  Source.HomeCityName,
                  Source.HomeCountyName,
                  Source.HomeStateName,
                  Source.HomeCountryName,
                  Source.HomePostalCode,
                  Source.OriginalBusinessID,
                  Source.BusinessAddressLine1,
                  Source.BusinessAddressLine2,
                  Source.BusinessCityName,
                  Source.BusinessCountyName,
                  Source.BusinessStateName,
                  Source.BusinessCountryName,
                  Source.BusinessPostalCode,
                  Source.OriginalOtherID,
                  Source.OtherAddressLine1,
                  Source.OtherAddressLine2,
                  Source.OtherCityName,
                  Source.OtherCountyName,
                  Source.OtherStateName,
                  Source.OtherCountryName,
                  Source.OtherPostalCode,
                  Source.HomeGeoMapCode1,
                  Source.HomeGeoMapCode2,
                  Source.HomeGeoMapCode3,
                  Source.HomeGeoMapCode4,
                  Source.BusinessGeoMapCode1,
                  Source.BusinessGeoMapCode2,
                  Source.BusinessGeoMapCode3,
                  Source.BusinessGeoMapCode4,
                  Source.OtherGeoMapCode1,
                  Source.OtherGeoMapCode2,
                  Source.OtherGeoMapCode3,
                  Source.OtherGeoMapCode4,
                  Source.HomeLatitude,
                  Source.HomeLongitude,
                  Source.BusinessLatitude,
                  Source.BusinessLongitude,
                  Source.OtherLatitude,
                  Source.OtherLongitude,
                  'N',
                  CONVERT( char( 10 ), CURRENT_TIMESTAMP - 1, 101 )
                  )
            WHEN MATCHED AND
                   (
                   ISNULL( Target.ClientName, '' ) <> ISNULL( Source.ClaimantFullName, '' )
                OR

                   ISNull( Target.HomeAddressLine1, '' ) <> ISNULL( Source.HomeAddressLine1, '' )
                OR ISNULL( Target.HomeAddressLine2, '' ) <> ISNULL( Source.HomeAddressLine2, '' )
                OR ISNULL( Target.HomeCity, '' ) <> ISNULL( Source.HomeCityName, '' )
                OR ISNULL( Target.HomeCountyName, '' ) <> ISNULL( Source.HomeCountyName, '' )
                OR ISNULL( Target.HomeState, '' ) <> ISNULL( Source.HomeStateName, '' )
                OR ISNULL( Target.HomeCountry, '' ) <> ISNULL( Source.HomeCountryName, '' )
                OR ISNULL( Target.HomePostalCode, '' ) <> ISNULL( Source.HomePostalCode, '' )
                   )
               AND Target.EndDate IS NULL
                  THEN UPDATE
                  SET Target.EndDate = CONVERT( char( 10 ),CURRENT_TIMESTAMP - 1, 101 ),
                      Target.CounterFlag = 'D';

            --Insert expired records into view

            INSERT INTO InsurerAnalyticsCommonDimension.VW__Client
            (
            ClientCode,
            ClientName,
            ClaimantID,
            claimtype,
            PolicyHolderID,
            HomeAddressOriginalID,
            HomeAddressLine1,
            HomeAddressLine2,
            HomeCity,
            HomeCountyName,
            HomeState,
            HomeCountry,
            HomePostalCode,
            BusinessAddressOriginalID,
            BusinessAddressLine1,
            BusinessAddressLine2,
            BusinessCityName,
            BusinessCountyName,
            BusinessStateName,
            BusinessCountryName,
            BusinessPostalCode,
            OtherAddressOriginalID,
            OtherAddressLine1,
            OtherAddressLine2,
            OtherCityName,
            OtherCountyName,
            OtherStateName,
            OtherCountryName,
            OtherPostalCode,
            HomeGeoMapCode1,
            HomeGeoMapCode2,
            HomeGeoMapCode3,
            HomeGeoMapCode4,
            BusinessGeoMapCode1,
            BusinessGeoMapCode2,
            BusinessGeoMapCode3,
            BusinessGeoMapCode4,
            OtherGeoMapCode1,
            OtherGeoMapCode2,
            OtherGeoMapCode3,
            OtherGeoMapCode4,
            HomeLatitude,
            HomeLongitude,
            BusinessLatitude,
            BusinessLongitude,
            OtherLatitude,
            OtherLongitude,
            CounterFlag,
            StartDate
            )
            SELECT
            Source.PolicyHolderID,
            Source.ClaimantFullName,
            Source.OriginalClaimantID,
            Source.ClaimType,
            Source.PolicyHolderID,
            Source.OriginalHomeID,
            Source.HomeAddressLine1,
            Source.HomeAddressLine2,
            Source.HomeCityName,
            Source.HomeCountyName,
            Source.HomeStateName,
            Source.HomeCountryName,
            Source.HomePostalCode,
            Source.OriginalBusinessID,
            Source.BusinessAddressLine1,
            Source.BusinessAddressLine2,
            Source.BusinessCityName,
            Source.BusinessCountyName,
            Source.BusinessStateName,
            Source.BusinessCountryName,
            Source.BusinessPostalCode,
            Source.OriginalOtherID,
            Source.OtherAddressLine1,
            Source.OtherAddressLine2,
            Source.OtherCityName,
            Source.OtherCountyName,
            Source.OtherStateName,
            Source.OtherCountryName,
            Source.OtherPostalCode,
            Source.HomeGeoMapCode1,
            Source.HomeGeoMapCode2,
            Source.HomeGeoMapCode3,
            Source.HomeGeoMapCode4,
            Source.BusinessGeoMapCode1,
            Source.BusinessGeoMapCode2,
            Source.BusinessGeoMapCode3,
            Source.BusinessGeoMapCode4,
            Source.OtherGeoMapCode1,
            Source.OtherGeoMapCode2,
            Source.OtherGeoMapCode3,
            Source.OtherGeoMapCode4,
            Source.HomeLatitude,
            Source.HomeLongitude,
            Source.BusinessLatitude,
            Source.BusinessLongitude,
            Source.OtherLatitude,
            Source.OtherLongitude,
            'C',
            CONVERT( char( 10 ), CURRENT_TIMESTAMP - 1, 101 )
              FROM
                   InsurerAnalyticsSupport.#DistinctClientPolicyNotNull Source INNER JOIN
                   InsurerAnalyticsCommonDimension.VW__Client Target
                   ON
                   Target.ClientCode = Source.PolicyHolderID
              WHERE
              Target.CounterFlag = 'D';
            SET @RecordsRead = @recordsRead + ( SELECT COUNT( * )
                                                  FROM ( SELECT ROW_NUMBER( ) OVER ( PARTITION BY #ClaimsClientDimension.OriginalClaimantID ORDER BY #ClaimsClientDimension.OriginalClaimantID ) AS RowNum
                                                           FROM InsurerAnalyticsSupport.#ClaimsClientDimension
                                                           WHERE #ClaimsClientDimension.PolicyholderID IS NULL ) AS Client
                                                  WHERE Client.RowNum = 1 );

            --*
            --* merge claimants
            --*
/************Creating Temp table for Policy holder ID is Null **************/

            SELECT * INTO #DistinctClientPolicyNull
              FROM 
              (SELECT * FROM
				    
				    ( SELECT *,ROW_NUMBER( ) OVER ( PARTITION BY #ClaimsClientDimension.PolicyHolderID ORDER BY #ClaimsClientDimension.OriginalClaimantID ) AS RowNum
				      FROM InsurerAnalyticsSupport.#ClaimsClientDimension
					 WHERE #ClaimsClientDimension.PolicyholderID IS NULL ) AS TblA 
					 
					 )AS B
					 
					 WHERE B.Rownum = 1

/************* Performing Merge ************************/

            MERGE
            InsurerAnalyticsCommonDimension.VW__Client AS Target
            USING
            InsurerAnalyticsSupport.#DistinctClientPolicyNull  AS Source
            ON Target.ClaimantID = Source.OriginalClaimantID
            WHEN NOT MATCHED
                  THEN INSERT
                  (
                  ClientCode,
                  ClientName,
                  ClaimantID,
                  claimtype,
                  PolicyHolderID,
                  HomeAddressOriginalID,
                  HomeAddressLine1,
                  HomeAddressLine2,
                  HomeCity,
                  HomeCountyName,
                  HomeState,
                  HomeCountry,
                  HomePostalCode,
                  BusinessAddressOriginalID,
                  BusinessAddressLine1,
                  BusinessAddressLine2,
                  BusinessCityName,
                  BusinessCountyName,
                  BusinessStateName,
                  BusinessCountryName,
                  BusinessPostalCode,
                  OtherAddressOriginalID,
                  OtherAddressLine1,
                  OtherAddressLine2,
                  OtherCityName,
                  OtherCountyName,
                  OtherStateName,
                  OtherCountryName,
                  OtherPostalCode,
                  HomeGeoMapCode1,
                  HomeGeoMapCode2,
                  HomeGeoMapCode3,
                  HomeGeoMapCode4,
                  BusinessGeoMapCode1,
                  BusinessGeoMapCode2,
                  BusinessGeoMapCode3,
                  BusinessGeoMapCode4,
                  OtherGeoMapCode1,
                  OtherGeoMapCode2,
                  OtherGeoMapCode3,
                  OtherGeoMapCode4,
                  HomeLatitude,
                  HomeLongitude,
                  BusinessLatitude,
                  BusinessLongitude,
                  OtherLatitude,
                  OtherLongitude,
                  CounterFlag,
                  StartDate
                  )
                  VALUES
                  (
                  Source.PolicyHolderID,
                  Source.ClaimantFullName,
                  Source.OriginalClaimantID,
                  source.claimtype,
                  Source.PolicyHolderID,
                  Source.OriginalHomeID,
                  Source.HomeAddressLine1,
                  Source.HomeAddressLine2,
                  Source.HomeCityName,
                  Source.HomeCountyName,
                  Source.HomeStateName,
                  Source.HomeCountryName,
                  Source.HomePostalCode,
                  Source.OriginalBusinessID,
                  Source.BusinessAddressLine1,
                  Source.BusinessAddressLine2,
                  Source.BusinessCityName,
                  Source.BusinessCountyName,
                  Source.BusinessStateName,
                  Source.BusinessCountryName,
                  Source.BusinessPostalCode,
                  Source.OriginalOtherID,
                  Source.OtherAddressLine1,
                  Source.OtherAddressLine2,
                  Source.OtherCityName,
                  Source.OtherCountyName,
                  Source.OtherStateName,
                  Source.OtherCountryName,
                  Source.OtherPostalCode,
                  Source.HomeGeoMapCode1,
                  Source.HomeGeoMapCode2,
                  Source.HomeGeoMapCode3,
                  Source.HomeGeoMapCode4,
                  Source.BusinessGeoMapCode1,
                  Source.BusinessGeoMapCode2,
                  Source.BusinessGeoMapCode3,
                  Source.BusinessGeoMapCode4,
                  Source.OtherGeoMapCode1,
                  Source.OtherGeoMapCode2,
                  Source.OtherGeoMapCode3,
                  Source.OtherGeoMapCode4,
                  Source.HomeLatitude,
                  Source.HomeLongitude,
                  Source.BusinessLatitude,
                  Source.BusinessLongitude,
                  Source.OtherLatitude,
                  Source.OtherLongitude,
                  'N',
                  CONVERT( char( 10 ), CURRENT_TIMESTAMP - 1, 101 ))
            WHEN MATCHED AND
                   ISNULL( Target.ClientName, '' ) <> ISNULL( Source.ClaimantFullName, '' )
                OR
			    ISNull( Target.HomeAddressLine1, '' ) <> ISNULL( Source.HomeAddressLine1, '' )
                OR ISNULL( Target.HomeAddressLine2, '' ) <> ISNULL( Source.HomeAddressLine2, '' )
                OR ISNULL( Target.HomeCity, '' ) <> ISNULL( Source.HomeCityName, '' )
                OR ISNULL( Target.HomeCountyName, '' ) <> ISNULL( Source.HomeCountyName, '' )
                OR ISNULL( Target.HomeState, '' ) <> ISNULL( Source.HomeStateName, '' )
                OR ISNULL( Target.HomeCountry, '' ) <> ISNULL( Source.HomeCountryName, '' )
                OR ISNULL( Target.HomePostalCode, '' ) <> ISNULL( Source.HomePostalCode, '' )
                  THEN UPDATE
                  SET Target.EndDate = CONVERT( char( 10 ),CURRENT_TIMESTAMP - 1, 101 ),
                      Target.CounterFlag = 'E';

            ---------Insert Expired Record

            INSERT INTO InsurerAnalyticsCommonDimension.VW__Client
            (
            ClientCode,
            ClientName,
            ClaimantID,
            claimtype,
            PolicyHolderID,
            HomeAddressOriginalID,
            HomeAddressLine1,
            HomeAddressLine2,
            HomeCity,
            HomeCountyName,
            HomeState,
            HomeCountry,
            HomePostalCode,
            BusinessAddressOriginalID,
            BusinessAddressLine1,
            BusinessAddressLine2,
            BusinessCityName,
            BusinessCountyName,
            BusinessStateName,
            BusinessCountryName,
            BusinessPostalCode,
            OtherAddressOriginalID,
            OtherAddressLine1,
            OtherAddressLine2,
            OtherCityName,
            OtherCountyName,
            OtherStateName,
            OtherCountryName,
            OtherPostalCode,
            HomeGeoMapCode1,
            HomeGeoMapCode2,
            HomeGeoMapCode3,
            HomeGeoMapCode4,
            BusinessGeoMapCode1,
            BusinessGeoMapCode2,
            BusinessGeoMapCode3,
            BusinessGeoMapCode4,
            OtherGeoMapCode1,
            OtherGeoMapCode2,
            OtherGeoMapCode3,
            OtherGeoMapCode4,
            HomeLatitude,
            HomeLongitude,
            BusinessLatitude,
            BusinessLongitude,
            OtherLatitude,
            OtherLongitude,
            CounterFlag,
            StartDate
            )
            SELECT
            Source.PolicyHolderID,
            Source.ClaimantFullName,
            Source.OriginalClaimantID,
            Source.ClaimType,
            Source.PolicyHolderID,
            Source.OriginalHomeID,
            Source.HomeAddressLine1,
            Source.HomeAddressLine2,
            Source.HomeCityName,
            Source.HomeCountyName,
            Source.HomeStateName,
            Source.HomeCountryName,
            Source.HomePostalCode,
            Source.OriginalBusinessID,
            Source.BusinessAddressLine1,
            Source.BusinessAddressLine2,
            Source.BusinessCityName,
            Source.BusinessCountyName,
            Source.BusinessStateName,
            Source.BusinessCountryName,
            Source.BusinessPostalCode,
            Source.OriginalOtherID,
            Source.OtherAddressLine1,
            Source.OtherAddressLine2,
            Source.OtherCityName,
            Source.OtherCountyName,
            Source.OtherStateName,
            Source.OtherCountryName,
            Source.OtherPostalCode,
            Source.HomeGeoMapCode1,
            Source.HomeGeoMapCode2,
            Source.HomeGeoMapCode3,
            Source.HomeGeoMapCode4,
            Source.BusinessGeoMapCode1,
            Source.BusinessGeoMapCode2,
            Source.BusinessGeoMapCode3,
            Source.BusinessGeoMapCode4,
            Source.OtherGeoMapCode1,
            Source.OtherGeoMapCode2,
            Source.OtherGeoMapCode3,
            Source.OtherGeoMapCode4,
            Source.HomeLatitude,
            Source.HomeLongitude,
            Source.BusinessLatitude,
            Source.BusinessLongitude,
            Source.OtherLatitude,
            Source.OtherLongitude,
            'N',
            CONVERT( char( 10 ), CURRENT_TIMESTAMP - 1, 101 )
              FROM
                   #DistinctClientPolicyNull Source INNER JOIN
                   InsurerAnalyticsCommonDimension.VW__Client Target
                   ON
                   Target.ClaimantID = Source.OriginalClaimantID
              WHERE
              Target.CounterFlag = 'E';
            DECLARE @TransactionStartTime datetime = CURRENT_TIMESTAMP;

            --*
            --* update bridge table
            --*

            MERGE
            InsurerAnalyticsClaimsDimension.BridgeClaimantClient AS Target
            USING
            (
            SELECT DISTINCT
            ClientStaging.ClaimNumber,
            ClientStaging.PolicyholderID,
            ClientStaging.OriginalClaimantID AS ClaimantID,
            Client.clientID,
            PartyType,
            PartyTypeCode
              FROM
                   #ClaimsClientDimension AS ClientStaging LEFT JOIN
                   InsurerAnalyticsCommonDimension.VW__Client AS Client
                   ON
                   Client.policyholderid = ClientStaging.policyholderid
              WHERE
              ClientStaging.PolicyHolderID IS NOT NULL
            UNION
            SELECT DISTINCT
            ClientStaging.ClaimNumber,
            ClientStaging.PolicyholderID,
            ClientStaging.OriginalClaimantID AS ClaimantID,
            Client.ClientID,
            PartyType,
            PartyTypeCode
              FROM
                   #ClaimsClientDimension AS ClientStaging LEFT JOIN
                   InsurerAnalyticsCommonDimension.Vw__Client AS Client
                   ON
                   Client.ClaimantID = ClientStaging.OriginalClaimantID
              WHERE
              ClientStaging.PolicyHolderID IS NULL
            ) AS Source
            ON
                   Source.ClaimantID = Target.OriginalClaimantID
               AND Target.ClaimNumber = Source.ClaimNumber
               AND Target.partytypecode = Source.partytypecode

            -- Business key

            WHEN NOT MATCHED
                  THEN INSERT
                  (
                  ClientID,
                  ClaimNumber,
                  OriginalClaimantID,
                  OriginalClientID,
                  partytypecode,
                  partytype,
                  CounterFlag
                  )
                  VALUES
                  (
                  Source.ClientID,
                  Source.ClaimNumber,
                  Source.ClaimantID,
                  Source.PolicyholderID,
                  Source.PartyTypeCode,
                  Source.PartyType,
                  'N'
                  );
            SET @BridgeRecordsRead = ( SELECT COUNT( * )
                                         FROM #ClaimsClientDimension );
        END
    DECLARE @TransactionEndTime datetime = CURRENT_TIMESTAMP;

    --*
    --* Get Audit Counts
    --*
    --* bridge
    --*

    SELECT
    @NewRecords = ISNULL( SUM( IIF( CounterFlag = 'N', 1, 0 )),0 ),
    @ChangedRecords = ISNULL( SUM( IIF( CounterFlag = 'C', 1, 0 )),0 ),
    @RecordsRead = ISNULL( @RecordsRead,0 )
      FROM InsurerAnalyticsClaimsDimension.BridgeClaimantClient;

    --DECLARE @BridgeRecordsRead int = (SELECT count(*) FROM #ClaimsClientDimension );

    EXEC InsurerAnalyticsSupport.CreateAuditRecord
    @JobType = N'Claims',
    @ExtractRunID = @ExtractRunID,
    @AuditItem = N'BaseLoadFactsAndDimensions',
    @AuditSubItem = N'Bridge Claimant Client',
    @AuditStatus = N'SUCCESS',
    @AuditCount = @BridgeRecordsRead,
    @AuditComment = N'Records Read',
    @AuditStartDate = @TransactionStartTime,
    @AuditCompletionDate = @TransactionEndTime,
    @AuditType = 'Audit';
    EXEC InsurerAnalyticsSupport.CreateAuditRecord
    @JobType = N'Claims',
    @ExtractRunID = @ExtractRunID,
    @AuditItem = N'BaseLoadFactsAndDimensions',
    @AuditSubItem = N'Bridge Claimant Client',
    @AuditStatus = N'SUCCESS',
    @AuditCount = @NewRecords,
    @AuditComment = N'Records Added',
    @AuditStartDate = @TransactionStartTime,
    @AuditCompletionDate = @TransactionEndTime,
    @AuditType = 'Audit';
    EXEC InsurerAnalyticsSupport.CreateAuditRecord
    @JobType = N'Claims',
    @ExtractRunID = @ExtractRunID,
    @AuditItem = N'BaseLoadFactsAndDimensions',
    @AuditSubItem = N'Bridge Claimant Client',
    @AuditStatus = N'SUCCESS',
    @AuditCount = @ChangedRecords,
    @AuditComment = N'Changed Records',
    @AuditStartDate = @TransactionStartTime,
    @AuditCompletionDate = @TransactionEndTime,
    @AuditType = 'Audit';

    --*
    --* client
    --*

    SELECT
    ISNULL( SUM( IIF( CounterFlag = 'N', 1, 0 )),0 ) AS NewRecords,
    ISNULL( SUM( IIF( CounterFlag = 'C', 1, 0 )),0 ) AS ChangedRecords,
    ISNULL( SUM( IIF( CounterFlag = 'D', 1, 0 )),0 )+ ISNULL( SUM( IIF( CounterFlag = 'E', 1, 0 )),0 ) AS ExpiredSCD2Records,
    ISNULL( @RecordsRead,0 ) AS RecordsRead
      FROM InsurerAnalyticsCommonDimension.Vw__Client
    DECLARE @ErrorMessage varchar( 2000 ) = NULL;

/******* Updating the ClientLowestKey in Client__EXtension Table ******************/

    SELECT * INTO #Clk
      FROM( SELECT c.ClientID,
                   c.clientcode,
                   DENSE_RANK( )OVER( ORDER BY clientcode )AS ClientLowestKey
              FROM InsurerAnalyticsCommonDimension.client c )a
    
    UPDATE ce
    SET ce.ClientLowestKey = #clk.ClientLowestKey
      FROM InsurerAnalyticsCommonDimension.Client__Extension ce INNER JOIN
      #Clk
           ON ce.ClientID = #Clk.ClientID
    
    DROP TABLE #Clk

   ------------=================================================================================---------------

    COMMIT TRAN ALL_OR_NOTHING
END TRY
BEGIN CATCH
    IF XACT_STATE( ) = -1
        BEGIN
            ROLLBACK TRAN ALL_OR_NOTHING

            -- Set Audit Counts

            SELECT
            0 NewRecords,
            0 ChangedRecords,
            0 ExpiredSCD2Records,
            ISNULL( @RecordsRead,0 ) AS RecordsRead
        END
    ELSE
        BEGIN IF XACT_STATE( ) = 1
                  BEGIN
                      COMMIT TRAN ALL_OR_NOTHING
                  END
        END
    SET @ErrorMessage = '(error: '
    +
    CONVERT( varchar, ERROR_NUMBER( ))
    +
    ', line: '
    +
    CONVERT( varchar, ERROR_LINE( ))
    +
    ') '
    + ERROR_MESSAGE( )
END CATCH
BEGIN
    IF @ErrorMessage IS NOT NULL
        BEGIN
            THROW  100000, @ErrorMessage, 16
        END
    ELSE
        BEGIN
            RETURN 0
        END
END


GO


