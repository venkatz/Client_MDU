
CREATE TABLE [InsurerAnalyticsClaimsStaging].[ClaimantStagingTable__Extension](
	[ClaimantStaging__ExtensionId] [int] IDENTITY(1,1) NOT NULL,
	[ClaimantStagingTableId] [int] NULL,
	[ClaimType] [nvarchar](100) NULL,
PRIMARY KEY CLUSTERED 
(
	[ClaimantStaging__ExtensionId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

ALTER TABLE [InsurerAnalyticsClaimsStaging].[ClaimantStagingTable__Extension]  WITH NOCHECK ADD  CONSTRAINT [FK_ClaimantStagingTableID__ExtensionID] FOREIGN KEY([ClaimantStagingTableId])
REFERENCES [InsurerAnalyticsClaimsStaging].[ClaimantStagingTable] ([ClaimantStagingTableID])
GO

ALTER TABLE [InsurerAnalyticsClaimsStaging].[ClaimantStagingTable__Extension] NOCHECK CONSTRAINT [FK_ClaimantStagingTableID__ExtensionID]
GO


