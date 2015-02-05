USE [master]
GO

--=================================================================================================
--Before use, ensure that all placeholders have been updated
--
--1) ServerName - The server hosting the remote database
--2) DatabaseName - The name of the remote contained Insurer Claims/Policy database
--3) UserName - Remote UserName
--4) Password - remote User Password
--==================================================================================================

EXEC master.dbo.sp_addlinkedserver @server = N'SQLDB', @srvproduct=N'', @provider=N'SQLNCLI', @provstr=N'Provider=SQLNCLI;Server=<ServerName>;Database=<DatabaseName>;'

EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=N'SQLDB',@useself=N'False',@locallogin=NULL,@rmtuser=N'<UserName>',@rmtpassword='<Password>'

GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'collation compatible', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'data access', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'dist', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'pub', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'rpc', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'rpc out', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'sub', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'connect timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'collation name', @optvalue=null
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'lazy schema validation', @optvalue=N'false'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'query timeout', @optvalue=N'0'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'use remote collation', @optvalue=N'true'
GO

EXEC master.dbo.sp_serveroption @server=N'SQLDB', @optname=N'remote proc transaction promotion', @optvalue=N'true'
GO

