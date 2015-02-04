CREATE TABLE [InsurerAnalyticsCommonDimension].[Client__Extension](
	[Client__ExtensionId] [int] IDENTITY(1,1) NOT NULL,
	[ClientLowestKey] [int] NULL,
	[ClientID] [int] NULL,
	[claimtype] [nvarchar](100) NULL,
	[StartDate] [datetime] NULL,
	[EndDate] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[Client__ExtensionId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

ALTER TABLE [InsurerAnalyticsCommonDimension].[Client__Extension]  WITH NOCHECK ADD  CONSTRAINT [FK_Client_Extension_ClientID] FOREIGN KEY([ClientID])
REFERENCES [InsurerAnalyticsCommonDimension].[Client] ([ClientID])
GO

ALTER TABLE [InsurerAnalyticsCommonDimension].[Client__Extension] NOCHECK CONSTRAINT [FK_Client_Extension_ClientID]
GO

