
#------------------------------------------------------------------------------------
#--Name:ValidateConfiguration.ps1
#--Created:08/09/2014
#--Description: validate the configuration ETL configuration file. Only permit deployment
#--if rudimentary validation is passed.
#--
#--1. Files and folders are checked for existence
#--2. Servers are tested for connectivity
#--3. dates are validated (leap year is an exception)
#--3. boolean values are checked
#--4. lists are checked
#--5. test connections are made against theDatabase and Analysis servers and databases
#------------------------------------------------------------------------------------

#====================================================================================
function Is-Numeric($Value) 
#-- check for integer values
{
    return $Value -match "^[\d\.]+$"
}
#====================================================================================

#====================================================================================
function Validate-Variable()
#====================================================================================
{
    param ([string]$VariableName, [string]$VariableType)

    $Status=$true
    if ( ($VariableType -eq "Enter your value here") -or ($VariableType -eq "") )
    {
        $Message="Value: ""$VariableType"" is invalid for $VariableName"
        $Status=$false
        $global:ConfigurationIsValid=$false
        Report-ValidationStatus -Message $Message -Status $Status
    }

}#End function (Validate-Variable)
#====================================================================================

#====================================================================================
function Ping-server()
#====================================================================================
{
    param ([string]$ServerName, [string]$ItemName)

    $Status=$true
    if (!(Test-Connection -Cn $ServerName -BufferSize 16 -Count 1 -ea 0 -quiet))

    {
        $global:ConfigurationIsValid=$false
        $Message="Server ""$ServerName"" for $ItemName is not reachable. Ensure server name is correct"
        $Status=$false
    }
    else
    {
        $Message="Server $ServerName for ""$ItemName"" exists"
    }
    Report-ValidationStatus -Message $Message -Status $Status
}#End (Ping-Server)
#====================================================================================

#====================================================================================
function Check-SourceSystemType()
#====================================================================================
{
    param ([string]$SourceSystemType, [string]$VariableName, [string]$ItemName)

    $Status=$true
    if ($SourceSystemType -eq "SQL" -or $SourceSystemType -eq "DB2")

    {
        $Message="Source System Type ""$SourceSystemType"" is valid"     
    }
    else
    {
        $global:ConfigurationIsValid=$false
        $Message="Source System Type ""$SourceSystemType"" is not valid. Valid values are ""SQL"" or ""DB2"" "
        $Status=$false
    }
    Report-ValidationStatus -Message $Message -Status $Status 
}#End (Ping-Server)
#====================================================================================

#====================================================================================
function Check-Bool()
#====================================================================================
{
    param ([string]$BoolValue, [string]$ItemName)

    $Status=$true
    if ($BoolValue -eq 1 -or $BoolValue -eq 0 )
    {
        $Message="Value: ""$BoolValue"" for $ItemName is valid"
    }
    else
    {
        $Message="Value: ""$BoolValue"" for $ItemName is not valid. Valid values are 1 and 0"
        $Status=$false
    }
    Report-ValidationStatus -Message $Message -Status $Status
}#End (Check-Bool)
#====================================================================================

#====================================================================================
function Check-Folder()
#====================================================================================
{
    param ([string]$FolderName, [string]$ItemName)

    $Status=$true
    if ( Test-Path -Path $FolderName )
    {
       $Message="Folder: ""$FolderName"" for $ItemName exists"
    }
    else
    {
        if ( $ItemName -eq "SQLPackageUtilityFolder" )
        {
            $global:ConfigurationIsValid=$false
            $Message="Folder ""$FolderName"" for $ItemName must exist for installation to continue"
        }

        $Status=$false
    }
    Report-ValidationStatus -Message $Message -Status $Status
}#End (Check-Folder)
#====================================================================================

#====================================================================================
function Check-Exe()
#====================================================================================
{
    param ([string]$ExeName, [string]$ItemName)

    $Status=$true
    if ( Test-Path -Path $ExeName )
    {
       $Message="Executable: ""$ExeName"" for $ItemName exists"
    }
    else
    {
       $Message="Executable ""$ExeName"" for $ItemName does not exist"
       $Status=$false
    }
    Report-ValidationStatus -Message $Message -Status $Status
}#End (Check-Folder)
#====================================================================================

#====================================================================================
function Check-DayMonth()
#====================================================================================
#
# validate month and day (ignore leap years as financial period won't start on Feb 29
#====================================================================================
{
    param ([string]$DayNumber, [string]$MonthNumber, [string]$ItemName)

    $Status=$true

    if ($DayNumber.length -ne 2 -or $MonthNumber.length -ne 2 ) # must be 2 characters
    {
        $Message="Day or Month: ""$DayNumber/$MonthNumber"" for $ItemName is incorrect length (must be two characters)"
        $Status=$false
        Report-ValidationStatus -Message $Message -Status $Status
    }
    else
    {
        if ( (Is-Numeric $DayNumber) -and (Is-Numeric $MonthNumber) ) # must be numeric
        {
            if ( $MonthNumber -le 12 ) # must be a month
            {
                $MaxDays=switch($MonthNumber) # thirty days have september etc.
                    {
                        02 {"28"} # assume that financial year won't start on a leap day
                        04 {"30"}
                        05 {"30"}
                        09 {"30"}
                        11 {"30"}
                        default {"31"}
                    }

                if ( $DayNumber -gt 0 -and $DayNumber -le $MaxDays )
                {
                    $Message="Day: $DayNumber for $ItemName is valid"
                    Report-ValidationStatus -Message $Message -Status $Status
                }
                else
                {
                    $Message="Day and Month combination: ""$DayNumber/$MonthNumber"" for $ItemName is invalid"
                    $Status=$false
                    Report-ValidationStatus -Message $Message -Status $Status
                }
                #$Message="Month: $MonthNumber for $ItemName is valid"
                #Report-ValidationStatus -Message $Message -Status $Status
            }
            else
            {
                $Message="Day and Month combination: ""$DayNumber/$MonthNumber"" for $ItemName is invalid"
                $Status=$false
                Report-ValidationStatus -Message $Message -Status $Status
            }
        }
        else
        {
            $Message="Day or Month is not numeric: ""$DayNumber/$MonthNumber"" for $ItemName is invalid"
            $Status=$false
            Report-ValidationStatus -Message $Message -Status $Status
        }
    }
}#End (Check-DayMonth)
#====================================================================================

#====================================================================================
function Report-ValidationStatus()
#====================================================================================
{
    param ([string]$Message, [string]$Status)
    
    $FailForeColour="Red"
    $FailBackColour="Black"
    $ForeColour="White"
    $BackColour="Blue"
    if (( $Status -eq $false ) -or ( $Status -eq 2 )) #2=warn
    {
        Write-Host "$Message"  -foregroundcolor $FailForeColour -backgroundcolor $FailBackColour
    }

}
#End (Report-ValidationStatus)
#====================================================================================

#====================================================================================
function Ping-Database()
#====================================================================================
{
    param ([string]$ServerName,[string]$InitialCatalog,[string]$ItemName)

    $Status=$true
    try
    {
        $DBConnection = New-Object System.Data.SqlClient.SqlConnection
        $DBConnection.ConnectionString = "Data Source=$ServerName;Integrated Security=SSPI;Initial Catalog=$InitialCatalog"
        $DBConnection.Open() 
        
        $Warning=2
        $Status=$false
        $DBConnection.close() 
        if ( $InitialCatalog -eq "master" )
        {
            $MessageWarning=""
            $Status=$true
        }
        else
        {
            $Status=$Warning
            $MessageWarning="WARNING: Database will be recreated!"
            $Message="Connected succesfully to $InitialCatalog on $ServerName for ""$ItemName"" $MessageWarning"
            Report-ValidationStatus -Message $Message -Status $Status 
        }
    }

    catch
    {
        if ( $InitialCatalog -eq "master" )
        { 
			$global:ConfigurationIsValid=$false
            $Message="Failed to connect to SQL Instance $ServerName for ""$ItemName"" "
            Report-ValidationStatus -Message $Message -Status $false
        }
        else
        {
            $Message="Failed to connect to $InitialCatalog on $ServerName. Database will be created"
            Report-ValidationStatus -Message $Message -Status $true
        }
        
    }
}#End (Ping-Database)
#====================================================================================

#====================================================================================
function Ping-SSAS()
#====================================================================================
{
    param ([string]$ServerName,[string]$InitialCatalog,[string]$ItemName)

    $Status=$true
    $Warning=2
    [System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices") >$null
    ("Microsoft.AnalysisServices") >$null
    $Server = New-Object Microsoft.AnalysisServices.Server

    try #ping the server
    {
        $Server.connect($ServerName)
        $ASServer=$Server.Name
    }
    catch #cannot connect to server
    {
        $Status=$false
        $Message="Cannot connect to Analysis Server $ServerName"
        Report-ValidationStatus -Message $Message -Status $Status
    }
    if ( $ASServer -eq $ServerName ) #ping database on server
    {
        try #ping database
        {
            $DBID = $InitialCatalog
            $Database = $Server.Databases.Item($DBID)
            $DatabaseName=$Database.name
            
            $Status=$Warning
            $Message="Connected successfully to $InitialCatalog on $ServerName for ""$ItemName"" WARNING: Database will be recreated!"
            Report-ValidationStatus -Message $Message -Status $Status
        }
        catch #cannot connect to database
        {
            $Message="Cannot connect to $InitialCatalog on $ServerName for ""$ItemName"". Database will be created" 
            Report-ValidationStatus -Message $Message -Status $true
        }
    }
}
#End (Ping-SSAS)
#====================================================================================

#====================================================================================
function Prompt-Continue()
#====================================================================================
{
    if ( $global:ConfigurationIsValid -ne $true )
    {
        exit 1
    }
    else
    {
		Write-Host "Successfully validated configuration parameters."
		Write-Host "-----------------------------------------------------------------------------------------"
		Write-Host ""
		
        $Continue = Read-Host "Press Enter to continue: Q to quit"  
		
        if ($Continue -eq 'Q' -or $Continue -eq 'q')
        {
            exit 1 #notice to quit (user has option to ignore warnings)
        }
        else
        {
            exit 0 #continue
        }
    }
}
#END (Prompt-Continue)
#====================================================================================   


#====================================================================================
# main body
#====================================================================================
    #--
    #-- set global status
    #--
    $global:ConfigurationIsValid=$true
    $global:ConfigurationWarnings=$false

    #--
    #-- read configuration from configuration file
    #--
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
    $ApplicationServer=$env:CONFIG_ApplicationServer
    $ApplicationDatabase=$env:CONFIG_ApplicationDatabase
    $FinancialPeriodDayStart=$env:CONFIG_FinancialPeriodDayStart
    $FinancialPeriodMonthStart=$env:CONFIG_FinancialPeriodMonthStart
    $SQLBcpExecutable=$env:CONFIG_SQLBcpExecutable
    $SQLPackageUtilityFolder=$env:CONFIG_SQLPackageUtilityFolder   
   
    #--
    #-- validate the values 
    #--
    Validate-Variable -VariableName "Version" -VariableType $Version 
    Validate-Variable -VariableName "SoftwareFolder" -VariableType $SoftwareFolder 
    Validate-Variable -VariableName "RootWorkingFolder" -VariableType $RootWorkingFolder 
    Validate-Variable -VariableName "SourceSystemType" -VariableType $SourceSystemType  
    Validate-Variable -VariableName "EnableDeltas" -VariableType $EnableDeltas  
    Validate-Variable -VariableName "DataWarehouseServer" -VariableType $DataWarehouseServer 
    Validate-Variable -VariableName "DataWarehouseServerPort" -VariableType $DataWarehouseServerPort  
    Validate-Variable -VariableName "DataWarehouseDatabase" -VariableType $DataWarehouseDatabase  
    Validate-Variable -VariableName "SSISCatalogFolder" -VariableType $SSISCatalogFolder  
    Validate-Variable -VariableName "AnalysisServer" -VariableType $AnalysisServer 
    Validate-Variable -VariableName "AnalysisDatabase" -VariableType $AnalysisDatabase 
    Validate-Variable -VariableName "ApplicationServer" -VariableType $ApplicationServer  
    Validate-Variable -VariableName "ApplicationDatabase" -VariableType $ApplicationDatabase 
    Validate-Variable -VariableName "FinancialPeriodDayStart" -VariableType $FinancialPeriodDayStart 
    Validate-Variable -VariableName "FinancialPeriodMonthStart" -VariableType $FinancialPeriodMonthStart  
    Validate-Variable -VariableName "SQLBcpExecutable" -VariableType $SQLBcpExecutable   
    Validate-Variable -VariableName "SQLPackageUtilityFolder" -VariableType $SQLPackageUtilityFolder 

    #--
    #-- verify servers exist
    #--
    #Ping-Server -ServerName $DataWarehouseServer -ItemName "DataWarehouseServer"
    #Ping-Server -ServerName $AnalysisServer -ItemName "AnalysisServer"
    #Ping-Server -ServerName $ApplicationServer -ItemName "ApplicationServer"

    #--
    #-- ping the SQL Server instance. First to test connectivity. Secondly to determine in the data warehouse
    #-- database already  exists or not
    #--
    Ping-Database -ServerName $DataWarehouseServer -InitialCatalog "master" -ItemName "DataWarehouseServer"
    Ping-Database -ServerName $DataWarehouseServer -InitialCatalog $DatawarehouseDatabase -ItemName "DatawarehouseDatabase"
    Ping-Database -ServerName $ApplicationServer -InitialCatalog "master" -ItemName "ApplicationServer"

    #--
    #-- ping Analysis Services
    #-- 
    Ping-SSAS -ServerName $AnalysisServer -InitialCatalog $AnalysisDatabase -ItemName "AnalysisDatabase"

    #--
    #-- verify Source System Type
    #--
    Check-SourceSystemType -SourcesystemType $SourceSystemType -ItemName "Source System Type"
    
    #--
    #-- validate folder names
    #--
    Check-Folder -FolderName $RootWorkingFolder -ItemName "RootWorkingFolder"
    Check-Folder -FolderName $SoftwareFolder -ItemName "Software Folder"
     
    #--
    #-- check executables exists
    #--
    Check-Exe -ExeName $SQLBcpExecutable -ItemName "SQLBcpExecutable"
    Check-Folder -FolderName $SQLPackageUtilityFolder -ItemName "SQLPackageUtilityFolder"
 
    #--
    #-- check booleans
    #--
    Check-Bool -BoolValue $EnableDeltas -ItemName "EnableDeltas"
 
    #--
    #-- check day/month
    #--
    Check-DayMonth -DayNumber $FinancialPeriodDayStart -MonthNumber $FinancialPeriodMonthStart -ItemName "FinancialPeriod(Day/Month)Start"

    #--
    #-- ask user if they wish to ignore warnigns and continue
    #--
    Prompt-Continue
#==================================================================================== 
#END