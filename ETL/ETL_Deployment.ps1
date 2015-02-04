#==================================================================================================
# ETL_Deployment.ps1
#===================================================================================================

#Clear-Host
Write-Output "-----------------------------------------------------------------------------------------"
Write-Output "Executing Insurer Analytics ETL. Please wait while modules are loaded."
Write-Output "-----------------------------------------------------------------------------------------"

Push-Location
Import-Module "SQLPS" -DisableNameChecking
Pop-Location
#Clear-Host

#configuration
$Version=$env:CONFIG_Version
$SoftwareFolder=$env:CONFIG_SoftwareFolder
$RootWorkingFolder=$env:CONFIG_RootWorkingFolder
$SourceSystemType=$env:CONFIG_SourceSystemType
$EnableDeltas=$env:CONFIG_EnableDeltas

$DataWarehouseServer=$env:CONFIG_DataWarehouseServer
$DataWarehouseServerPort=$env:CONFIG_DataWarehouseServerPort
$DataWarehouseDatabase=$env:CONFIG_DataWarehouseDatabase
$SSISCatalogFolder=$env:CONFIG_SSISCatalogFolder

$AnalysisServer=$env:CONFIG_AnalysisServer
$AnalysisDatabase=$env:CONFIG_AnalysisDatabase
$AnalysisAuditDatabase=$env:CONFIG_AnalysisAuditDatabase

$ApplicationServer=$env:CONFIG_ApplicationServer
$ApplicationDatabase=$env:CONFIG_ApplicationDatabase

$FinancialPeriodDayStart=$env:CONFIG_FinancialPeriodDayStart
$FinancialPeriodMonthStart=$env:CONFIG_FinancialPeriodMonthStart

$SQLBcpExecutable=$env:CONFIG_SQLBcpExecutable
$SQLPackageUtilityFolder=$env:CONFIG_SQLPackageUtilityFolder

$CreateSSISDBProjectSQL="DECLARE @CatalogName [NVARCHAR](100)='" + $SSISCatalogFolder + "'; Declare @folder_id bigint; EXEC [SSISDB].[catalog].[create_folder] @folder_name=@CatalogName, @folder_id=@folder_id OUTPUT"
$CreateSSISDBFolderSQL="DECLARE @CatalogName [NVARCHAR](100)='" + $SSISCatalogFolder + "'; IF NOT EXISTS(SELECT 1 FROM SSISDB.internal.folders WHERE name = @CatalogName)  BEGIN EXEC [SSISDB].[catalog].[create_folder] @folder_name=@CatalogName END"

Write-Output "Check Configuration Parameters"
Write-Output "-----------------------------------------------------------------------------------------"
Write-Output "Version:			$Version"
Write-Output "SoftwareFolder:			$SoftwareFolder"
Write-Output "RootWorkingFolder:		$RootWorkingFolder"
Write-Output "SourceSystemType:		$SourceSystemType"
Write-Output "EnableDeltas:			$EnableDeltas"

Write-Output "DataWarehouseServer:		$DataWarehouseServer"
Write-Output "DataWarehouseServerPort:	$DataWarehouseServerPort"
Write-Output "DataWarehouseDatabase:		$DataWarehouseDatabase"
Write-Output "SSISCatalogFolder:		$SSISCatalogFolder"

Write-Output "AnalysisServer:			$AnalysisServer"
Write-Output "AnalysisDatabase:		$AnalysisDatabase"
Write-Output "AnalysisAuditDatabase:		$AnalysisAuditDatabase"

Write-Output "ApplicationServer:		$ApplicationServer"
Write-Output "ApplicationDatabase:		$ApplicationDatabase"

Write-Output "FinancialPeriodDayStart:	$FinancialPeriodDayStart"
Write-Output "FinancialPeriodMonthStart:	$FinancialPeriodMonthStart"

Write-Output "SQLBcpExecutable:		$SQLBcpExecutable"
Write-Output "SQLPackageUtilityFolder:	$SQLPackageUtilityFolder"
Write-Output ""

$Continue = Read-Host "Press Enter to continue: Q to quit"  
if ($Continue -eq 'Q' -or $Continue -eq 'q')
     {
		Write-Output ""
		Write-Output "-----------------------------------------------------------------------------------------"
		Write-Output "User aborted installation."
		Write-Output "-----------------------------------------------------------------------------------------"
        exit
     }

#====================================================================================
function Create-FolderStructure()
#create initial installation folder structure
#====================================================================================
{
    param ([string]$FileRoot)
    
    Write-Output "Insurer Analytics ETL, Version $Version : Create Folder Structure."
    Write-Out -LogMessage "Insurer Analytics ETL, Version $Version : Create Folder Structure."
    
    $IncomingFiles=$FileRoot + "\IncomingFiles"
    $ClaimsIncomingFiles=$IncomingFiles + "\Claims"
    $PolicyIncomingFiles=$IncomingFiles + "\Policy"
    $ExternalIncomingFiles=$IncomingFiles + "\External"
    $ArchivedFiles=$FileRoot + "\ArchivedFiles"
    $ClaimsArchivedFiles=$ArchivedFiles + "\Claims"
    $PolicyArchivedFiles=$ArchivedFiles + "\Policy"
    $ExternalArchivedFiles=$ArchivedFiles + "\External"
    $LogFiles=$FileRoot + "\Log"
    $TempFiles=$FileRoot + "\Temp"
    $ClaimsScopeFiles=$ScopeFiles + "\Claims"
    $PolicyScopeFiles=$ScopeFiles + "\Policy"
    
    if (-not(Test-Path $FileRoot))
    {
        New-Item $FileRoot -type directory
	}
	if (-not(Test-Path $IncomingFiles))
    {
        New-Item $IncomingFiles -type directory
	}
	if (-not(Test-Path $ClaimsIncomingFiles))
    {
		New-Item $ClaimsIncomingFiles -type directory
	}
	if (-not(Test-Path $PolicyIncomingFiles))
    {
		New-Item $PolicyIncomingFiles -type directory
	}
	if (-not(Test-Path $ExternalIncomingFiles))
    {
		New-Item $ExternalIncomingFiles -type directory
	}
	if (-not(Test-Path $ArchivedFiles))
    {
        New-Item $ArchivedFiles -type directory
    }
	if (-not(Test-Path $ClaimsArchivedFiles))
    {
		New-Item $ClaimsArchivedFiles -type directory
	}
	if (-not(Test-Path $PolicyArchivedFiles))
    {
		New-Item $PolicyArchivedFiles -type directory
	}
	if (-not(Test-Path $ExternalArchivedFiles))
    {
		New-Item $ExternalArchivedFiles -type directory
	}
	if (-not(Test-Path $TempFiles))
    {
		New-Item $TempFiles -type directory
	}	
	if (-not(Test-Path $LogFiles))
    {
		New-Item $LogFiles -type directory
	}
	if (-not(Test-Path $ClaimsScopeFiles))
    {
		New-Item $ClaimsScopeFiles -type directory
	}
	if (-not(Test-Path $PolicyScopeFiles))
    {
		New-Item $PolicyScopeFiles -type directory
    }
}#END (Create-FolderStructure)
#====================================================================================

#====================================================================================
function Update-FileConfig()
#replace the placeholders in the fileswith the values read from the config xml
#====================================================================================
{
    param ([string]$FileName, [string]$Placeholder, [String]$ConfigValue)
        (Get-Content $FileName) -replace $Placeholder, $ConfigValue |Set-Content $FileName
}#END (Update-FileConfig)
#====================================================================================

#====================================================================================
function Execute-SQL-Statement()
#====================================================================================
    #--------------------------------------------------------------------------------
    # Description
    # Uses SQLPS cmdlet Invoke-Sqlcmd
    # Parameters: $Sqlcommand -String containing SQL statement
    #             $DataWarehouseServer -Instance to connect to
    #             $Database -Database to connect to
    #             $OutFile (not used by Invoke-Sqlcmd as it needs to be appended to) -logfile
    # This function will execute an SQl script given as a parameter above, using the other parameters to define where 
    # the script is run. The returnCode is returned to the calling module.
    #--------------------------------------------------------------------------------
{
    param ([string]$SQLStatement, [string]$DataWarehouseServer, [string]$DatabaseName)

    $SQLExit = Invoke-Sqlcmd -Query $SqlStatement -ServerInstance $DataWarehouseServer -Database $DatabaseName
    return $SQLExit
}#End function (Execute-SQL-Statement)
#====================================================================================

#====================================================================================
function Test-ItemProperty()
# test if the registry item for the alias exists already
#====================================================================================
{
    param ([string]$RegistryPath,[string]$AliasName)
        $RC = $null
        $RC = Get-ItemProperty -path $RegistryPath -name $AliasName -errorAction SilentlyContinue
    return $RC -ne $null
}#End (Test-itemProperty)
#====================================================================================

#====================================================================================
function Write-Out()
# 
#====================================================================================
{
    param ([string]$LogMessage)
        Write-Host "================================================================================" -foregroundcolor Blue -backgroundcolor White
        Write-Host "Hostname:$env:computername on $(Get-Date)" -foregroundcolor Blue -backgroundcolor White
        Write-Host "$LogMessage" -foregroundcolor Black -backgroundcolor White
        Write-Host "================================================================================" -foregroundcolor Blue -backgroundcolor White
}#End (Write-Out)
#====================================================================================

#====================================================================================
function Create-SQLAlias()
# Check to see if the required SQL Aliases exist. If not then create them
#====================================================================================
{
    param ([string]$Alias, [string]$Server, [string]$Port)
    
    Write-Out -LogMessage "Insurer Analytics ETL, Version $Version : Create SQL Alias $Alias"
    Write-Output "Insurer Analytics ETL, Version $Version : Create SQL Alias $Alias"
    
    if ($Port -ne "0") #port specified
    {
        $AliasPort="," + $Port
    }
    else #use dynamic port allocation
    {
        $AliasPort=""
    }

    $x86 = "HKLM:\Software\Microsoft\MSSQLServer\Client\ConnectTo"
    $x64 = "HKLM:\Software\Wow6432Node\Microsoft\MSSQLServer\Client\ConnectTo"

    #We're going to see if the ConnectTo key already exists, and create it if it doesn't.
    if ((test-path -path $x86) -ne $True)
    {
        Write-Out -LogMessage "$x86 doesn't exist - creating"
        New-Item $x86
    }
    if ((test-path -path $x64) -ne $True)
    {
        Write-Out -LogMessage "$x64 doesn't exist - creating"
        New-Item $x64
    }
	
    $DataWarehouseServerAliasExists = Test-ItemProperty -RegistryPath $x86 -AliasName $Alias
    $DataWarehouseServerAlias6432Exists = Test-ItemProperty -RegistryPath $x64 -AliasName $Alias
    
    if ($DataWarehouseServerAliasExists -eq $false)
    {
        #----------------------------------------------------------------------------
        # Create SQL Alias "Data Warehouse Server" and "SSIS Package Store"
        #----------------------------------------------------------------------------
            
        New-ItemProperty $x86 -name $Alias -propertytype String -value "DBMSSOCN,$Server $AliasPort"
    }
    else {
        set-itemproperty  $x86 -name $Alias -value "DBMSSOCN,$Server $AliasPort"
    }

    if ($DataWarehouseServerAlias6432Exists -eq $false)
    {
        #----------------------------------------------------------------------------
        # Create SQL Alias "Data Warehouse Server" and "SSIS Package Store"
        #----------------------------------------------------------------------------
            
        New-ItemProperty $x64 -name $Alias -propertytype String -value "DBMSSOCN,$Server $AliasPort"
    }else {
        set-itemproperty $x64 -name $Alias -value "DBMSSOCN,$Server $AliasPort"
    }
}#End (CreateSQLAliases)
#==================================================================================

#====================================================================================
function Publish-Dacpac()
# Publish dacpac
#====================================================================================
{
    param ([string]$FileRoot, [string]$DataWarehouseDatabase, [string]$Version, [string]$DataWarehouseServer, [string]$AnalysisServer, [string]$AnalysisDatabase, [string]$AnalysisAuditDatabase, [string]$SourceSystemType, [string]$RootWorkingFolder, [string]$ApplicationServer, [string]$ApplicationDatabase, [string]$SSISCatalogFolder, [string]$FinancialPeriodDayStart, [string]$FinancialPeriodMonthStart, [string]$SQLPackageUtilityFolder, [string]$EnableDeltas, [string]$SQLBcpExecutable)
    
    Write-Out -LogMessage "Insurer Analytics ETL, Version $Version : Publish Dacpac"
    Write-Output "Insurer Analytics ETL, Version $Version : Publish Dacpac"
    $SqlPackagePath="C:\Program Files (x86)\Microsoft SQL Server\110\DAC\bin"
    $SourceFile=$FileRoot + "\dacpac\InsurerAnalyticsDataWarehouse.dacpac"
    
    Push-Location $SqlPackagePath
    
    try {
        .\SqlPackage.exe /Action:Publish /Sourcefile:$Sourcefile /TargetServerName:$DataWarehouseServer /TargetDatabaseName:$DataWarehouseDatabase /p:CreateNewDatabase=True /Variables:Version=$Version /Variables:RootWorkingFolder=$RootWorkingFolder /Variables:SourceSystemType=$SourceSystemType /Variables:EnableDeltas=$EnableDeltas /Variables:AnalysisServer=$AnalysisServer /Variables:AnalysisDatabase=$AnalysisDatabase /Variables:AnalysisAuditDatabase=$AnalysisAuditDatabase /Variables:ApplicationServer=$ApplicationServer /Variables:ApplicationDatabase=$ApplicationDatabase /Variables:FinancialPeriodDayStart=$FinancialPeriodDayStart /Variables:FinancialPeriodMonthStart=$FinancialPeriodMonthStart /Variables:SSISCatalogFolder=$SSISCatalogFolder /Variables:DataWarehouseServer=$DataWarehouseServer /Variables:DataWarehouseDatabase=$DataWarehouseDatabase /Variables:SQLBcpExecutable=$SQLBcpExecutable
    
        if ($LASTEXITCODE -eq 1)
        {
            Write-Out $LASTEXITCODE
            # SqlPackage will write an error to STDOTU for us if required
            exit 1
        }
    }
    finally {
        # Return to previous folder
        Pop-Location
    }
}#End (PublishDacpac)
#==================================================================================

#====================================================================================
function Publish-Dacpac-Extension()
# Publish dacpac extension
#====================================================================================
{
    param ([string]$FileRoot, [string]$DataWarehouseDatabase, [string]$Version, [string]$DataWarehouseServer, [string]$RootWorkingFolder, [string]$SourceFile, [string]$SQLPackageUtilityFolder)
    
    Write-Out -LogMessage "Insurer Analytics ETL, Version $Version : Publish Regional or Client Extension Dacpac"
    Write-Output "Insurer Analytics ETL, Version $Version : Publish Regional or Client Extension Dacpac"
    $SQLPackagePath="C:\Program Files (x86)\Microsoft SQL Server\110\DAC\bin"

    Push-Location $SQLPackagePath
    
    try {
        .\SqlPackage.exe /Action:Publish /Sourcefile:$Sourcefile /TargetServerName:$DataWarehouseServer /TargetDatabaseName:$DataWarehouseDatabase  /p:IncludeCompositeObjects=false /Variables:Version=$Version  

        if ($LASTEXITCODE -eq 1)
        {
            Write-Out $LASTEXITCODE
            # SqlPackage will write an error to STDOTU for us if required
            exit 1
        }
    }
    finally {
        # Return to previous folder
        Pop-Location
    }
}#End (Publish-Dacpac-Extension)
#==================================================================================

#====================================================================================
function Publish-Ispac()
# Deploy SSIS project
#====================================================================================
{
    param ([string]$FileRoot, [string]$DataWarehouseDatabase, [string]$DataWarehouseServer, [string]$SSISCatalogFolder, [string]$Version, [string]$SQLPackageUtilityFolder)
    
    Write-Out -LogMessage "Insurer Analytics ETL, Version $Version : Deploy SSIS Project."
    Write-Output "Insurer Analytics ETL, Version $Version : Deploy SSIS Project."
    
	$SourcePath=$FileRoot + "\SSIS\BaseETL.ispac"
    $DestinationPath="/SSISDB/" + $SSISCatalogFolder + "/" + $SSISCatalogFolder + "ETL"
	Push-Location $SQLPackageUtilityFolder
		
	.\IsDeploymentWizard.exe /Silent /SourcePath:$SourcePath /DestinationServer:$DataWarehouseServer /DestinationPath:$DestinationPath
	Pop-location 
}#End (Deploy-SSIS-Project)
#==================================================================================

#====================================================================================
function Deploy-SSAS()
#execute xmla to create analysis services database
#====================================================================================
{
    param ([string]$AnalysisServer, [string]$FileRoot, [string]$DataWarehouseDatabase, [string]$AnalysisDatabase, [string]$CubeXMLA, [string]$AssemblyXMLA )
    
    Write-Out -LogMessage "Insurer Analytics ETL, Version $Version : Create SSAS Database - $AnalysisDatabase"
    Write-Output "Insurer Analytics ETL, Version $Version : Create SSAS Database - $AnalysisDatabase"
    Import-Module "sqlascmdlets"
	
    $XMLAFileName=$FileRoot + "\XMLA\" + $CubeXMLA
	$EditedXMLAFileName=$FileRoot + "\XMLA\" + $AnalysisDatabase + ".xmla"
	
    $XMLACreateAssemblyFileName=$FileRoot + "\XMLA\" + $AssemblyXMLA
	$EditedXMLACreateAssemblyFileName=$FileRoot + "\XMLA\" + $AnalysisDatabase + "Assembly.xmla"
	
    $DropXMLA='<Delete xmlns="http://schemas.microsoft.com/analysisservices/2003/engine"><Object><DatabaseID>' + $AnalysisDatabase + '</DatabaseID></Object></Delete>'
    
	if ( ($CubeXMLA -eq "InsurerAnalytics.xmla") -and ($AnalysisDatabase -ne "InsurerAnalytics") )
	{
		$EncasedAnalysisDatabase=">" + $AnalysisDatabase + "<"
	
		# Change the name of the cube in the XMLA files
		cat $XMLAFileName | %{$_ -replace ">InsurerAnalytics<", $EncasedAnalysisDatabase} > $EditedXMLAFileName
		cat $XMLACreateAssemblyFileName | %{$_ -replace ">InsurerAnalytics<", $EncasedAnalysisDatabase} > $EditedXMLACreateAssemblyFileName
	}
	else 
	{
		if ( ($CubeXMLA -eq "InsurerAnalyticsAudit.xmla") -and ($AnalysisDatabase -ne "InsurerAnalyticsAudit") )
		{
			$EncasedAnalysisDatabase=">" + $AnalysisDatabase + "<"
		
			# Change the name of the cube in the XMLA files
			cat $XMLAFileName | %{$_ -replace ">InsurerAnalyticsAudit<", $EncasedAnalysisDatabase} > $EditedXMLAFileName
			cat $XMLACreateAssemblyFileName | %{$_ -replace ">InsurerAnalyticsAudit<", $EncasedAnalysisDatabase} > $EditedXMLACreateAssemblyFileName
		}
	}
 
	# Update the data source based on the name of the data warehouse database
	$XMLAContent = Get-Content $EditedXMLAFileName
	$NewXMLAContent = $XMLAContent -replace "InsurerAnalyticsDataWarehouse", $DataWarehouseDatabase
    $NewXMLAContent | Set-Content $EditedXMLAFileName 
		
    Write-Output "Dropping existing SSAS database - if no SSAS database exists then errors will be received, which can be safely ignored"
    Invoke-ASCmd -Server:$AnalysisServer -Query:$DropXMLA
    Invoke-ASCmd -Server:$AnalysisServer -InputFile:$EditedXMLAFileName
    Invoke-ASCmd -Server:$AnalysisServer -InputFile:$EditedXMLACreateAssemblyFileName
}#End (Deploy-SSAS)
#====================================================================================

#====================================================================================
#Body
#====================================================================================
Write-Output ""
Write-Out -LogMessage "Insurer Analytics ETL, Version $Version"
Write-Output ""

try
{
    $prompt = "Create ETL Folder Structure"
    $message = "Do you want to create the ETL folder structure?"
    $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes", "Creates folder structure."
    $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No", "Does not create folder structure."

    $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)

    $result = $host.ui.PromptForChoice($title, $message, $options, 0) 

    if($result -eq 0)
    {
        Create-FolderStructure -FileRoot $RootWorkingFolder
    }
	
	Write-Output ""

    $prompt = "Create SQL Server Alias"
    $message = "Do you want to create the SQL Server alias?"
    $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes", "Creates SQL Server alias."
    $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No", "Does not create SQL Server alias."

    $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)

    $result = $host.ui.PromptForChoice($title, $message, $options, 0) 

    if($result -eq 0)
    {
        Create-SQLAlias -Alias "Data Warehouse Server" -Server $DataWarehouseServer -Port $DataWarehouseServerPort
    }
	
	Write-Output ""

    $prompt = "Deploy data warehouse"
    $message = "Do you want to deploy the data warehouse?"
    $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes", "Deploys the data warehouse"
    $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No", "Does not deploy the data warehouse."

    $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)

    $result = $host.ui.PromptForChoice($title, $message, $options, 0) 

    if($result -eq 0)
    {
        Publish-Dacpac -FileRoot $SoftwareFolder -DataWarehouseDatabase $DataWarehouseDatabase -Version $Version -DataWarehouseServer $DataWarehouseServer -AnalysisServer $AnalysisServer -AnalysisDatabase $AnalysisDatabase -AnalysisAuditDatabase $AnalysisAuditDatabase -SourceSystemType $SourceSystemType -RootWorkingFolder $RootWorkingFolder -ApplicationServer $ApplicationServer -ApplicationDatabase $ApplicationDatabase -SSISCatalogFolder $SSISCatalogFolder -FinancialPeriodDayStart $FinancialPeriodDayStart -FinancialPeriodMonthStart $FinancialPeriodMonthStart -SQLPackageUtilityFolder $SQLPackageUtilityFolder -EnableDeltas $EnableDeltas -SQLBcpExecutable $SQLBcpExecutable
    }
	
	Write-Output ""

#
    # if an regional dacpac exists then prompt user whether they want to publish it. 
    #
    $ExtensionDacpac=$SoftwareFolder + "\dacpac\InsurerAnalyticsDataWarehouse__Region.dacpac"
    if ((Test-Path $ExtensionDacpac))
    {
        $prompt = "Publish dacpac Regional Extensions"
        $message = "Do you want to deploy the data warehouse regional extensions?"
        $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes", "Publishes the regional dacpac"
        $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No", "Does not publish the regional dacpac."

        $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)

        $result = $host.ui.PromptForChoice($title, $message, $options, 0) 

        if($result -eq 0)
        {
            Publish-Dacpac-Extension -FileRoot $SoftwareFolder -DataWarehouseDatabase $DataWarehouseDatabase -Version $Version -DataWarehouseServer $DataWarehouseServer -RootWorkingFolder $RootWorkingFolder -Sourcefile $ExtensionDacpac -SQLPackageUtilityFolder $SQLPackageUtilityFolder
        }
	
	    Write-Output ""
    }



    #
    # if an extension dacpac exists then prompt user whether they want to publish it. 
    #
    $ExtensionDacpac=$SoftwareFolder + "\dacpac\InsurerAnalyticsDataWarehouse__Extension.dacpac"
    if ((Test-Path $ExtensionDacpac))
    {
        $prompt = "Publish dacpac Client Extensions"
        $message = "Do you want to deploy the data warehouse client extensions?"
        $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes", "Publishes the extended dacpac"
        $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No", "Does not publish the extended dacpac."

        $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)

        $result = $host.ui.PromptForChoice($title, $message, $options, 0) 

        if($result -eq 0)
        {
            Publish-Dacpac-Extension -FileRoot $SoftwareFolder -DataWarehouseDatabase $DataWarehouseDatabase -Version $Version -DataWarehouseServer $DataWarehouseServer -RootWorkingFolder $RootWorkingFolder -Sourcefile $ExtensionDacpac -SQLPackageUtilityFolder $SQLPackageUtilityFolder
        }
	
	    Write-Output ""
    }

    $prompt = "Deploy SSIS packages"
    $message = "Do you want to deploy the SSIS packages?"
    $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes", "Deploys the SSIS packages"
    $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No", "Does not deploy the SSIS packages."

    $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)

    $result = $host.ui.PromptForChoice($title, $message, $options, 0) 

    if($result -eq 0)
    {
		#Create SSISDB Folder
		Execute-SQL-Statement -SQLStatement $CreateSSISDBFolderSQL -DataWarehouseServer $DataWarehouseServer -DatabaseName "SSISDB"		
     
		#deploy the ispac
		Publish-Ispac -FileRoot $SoftwareFolder -DataWarehouseDatabase $DataWarehouseDatabase -DataWarehouseServer $DataWarehouseServer -SSISCatalogFolder $SSISCatalogFolder -SQLPackageUtilityFolder $SQLPackageUtilityFolder -Version $Version
    }
	
	Write-Output ""

    $prompt = "Deploy SSAS cube"
    $message = "Do you want to deploy the SSAS cube?"
    $yes = New-Object System.Management.Automation.Host.ChoiceDescription "&Yes", "Deploys the SSAS cube"
    $no = New-Object System.Management.Automation.Host.ChoiceDescription "&No", "Does not deploy the SSAS cube."

    $options = [System.Management.Automation.Host.ChoiceDescription[]]($yes, $no)

    $result = $host.ui.PromptForChoice($title, $message, $options, 0) 

    if($result -eq 0)
    {
        Deploy-SSAS -FileRoot $SoftwareFolder -AnalysisServer $AnalysisServer -DataWarehouseDatabase $DataWarehouseDatabase -AnalysisDatabase $AnalysisDatabase -CubeXMLA "InsurerAnalytics.xmla" -AssemblyXMLA "InsurerAnalyticsAssembly.xmla"
        Deploy-SSAS -FileRoot $SoftwareFolder -AnalysisServer $AnalysisServer -DataWarehouseDatabase $DataWarehouseDatabase -AnalysisDatabase $AnalysisAuditDatabase -CubeXMLA "InsurerAnalyticsAudit.xmla" -AssemblyXMLA "InsurerAnalyticsAuditAssembly.xmla"
    }
	
	Write-Output ""

}
catch
    {
        Write-Out "$_ Error ?????" 
        Write-Output "$_ Error ?????" 
        #Write-Error ?
    }
#====================================================================================
#END (Body)
