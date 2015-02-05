

/************************************************************************ 
Create Linked Server POLICYSYSTEM
Purpose:
This script creates the POLICYSYSTEMLinked Server, if it does not already exist. 
IMPORTANT:
BEFORE RUNNING THIS SCRIPT, ENSURE THAT THE USERID AND PASSWORD HAS BEEN CHANGED!
*************************************************************************/

IF  EXISTS (SELECT srv.name FROM sys.servers srv WHERE srv.server_id != 0 AND srv.name = N'POLICYSYSTEM') EXEC master.dbo.sp_dropserver @server=N'POLICYSYSTEM', @droplogins='droplogins'
GO

EXEC master.dbo.sp_addlinkedserver @server = N'POLICYSYSTEM', @srvproduct=N'DB2', @provider=N'IBMDADB2.DB2COPY1', @datasrc=N'POLICYRO'
GO
EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=N'POLICYSYSTEM',@useself=N'False',@locallogin=NULL,@rmtuser=N'<UserID>',@rmtpassword='<Password>'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'collation compatible', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'data access', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'dist', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'pub', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'rpc', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'rpc out', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'sub', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'connect timeout', @optvalue=N'0'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'collation name', @optvalue=null
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'lazy schema validation', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'query timeout', @optvalue=N'0'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'use remote collation', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'POLICYSYSTEM', @optname=N'remote proc transaction promotion', @optvalue=N'true'
GO
/************************************************************************ 
Create Linked Server CLAIMSYSTEM
Purpose:
This script creates the CLAIMSYSTEMLinked Server, if it does not already exist. 
IMPORTANT:
BEFORE RUNNING THIS SCRIPT, ENSURE THAT THE USERID AND PASSWORD HAS BEEN CHANGED!
*************************************************************************/
IF  EXISTS (SELECT srv.name FROM sys.servers srv WHERE srv.server_id != 0 AND srv.name = N'CLAIMSYSTEM') EXEC master.dbo.sp_dropserver @server=N'CLAIMSYSTEM', @droplogins='droplogins'
GO
EXEC master.dbo.sp_addlinkedserver @server = N'CLAIMSYSTEM', @srvproduct=N'DB2', @provider=N'IBMDADB2.DB2COPY1', @datasrc=N'CLAIMSRO'
GO
EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname=N'CLAIMSYSTEM',@useself=N'False',@locallogin=NULL,@rmtuser=N'<UserID>',@rmtpassword='<Password>'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'collation compatible', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'data access', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'dist', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'pub', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'rpc', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'rpc out', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'sub', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'connect timeout', @optvalue=N'0'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'collation name', @optvalue=null
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'lazy schema validation', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'query timeout', @optvalue=N'0'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'use remote collation', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'CLAIMSYSTEM', @optname=N'remote proc transaction promotion', @optvalue=N'true'
GO


USE [master]
GO
EXEC master.dbo.sp_MSset_oledb_prop N'IBMDADB2.DB2COPY1', N'AllowInProcess', 1
