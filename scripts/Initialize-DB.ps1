<#
.SYNOPSIS
Set up, initialize, or reinitialize a database or subset of tables for Tesla.
.DESCRIPTION
This script reads Tesla config files for a master and slave agent and initializes tables.
Generally, this script should be run on a server that has the Tesla config files, which should be 
in the same datacenter as either the master or slave. If the amount of data you're initializing is 
small or you don't mind waiting, you can certainly run it from your own workstation. 
For Netezza slaves you'll need to already have things like the ssh keys and nzload scripts set up.
Make sure you have reviewed your configuration files fully before running this script, as one of the
things it does when setting up a new database is dropping all tables in the CT databases (but
not the master or slave databases).

This script requires powershell V2, as well as the .NET SMO tools installed, both of which are 
available from Microsoft. For initializing Netezza slaves, it also requires the NZOLEDB driver
which is available from IBM only if you own a Netezza server.
.PARAMETER masterconfigfile
Full path to the relevant Tesla Master agent configuration xml file.
.PARAMETER slaveconfigfile
Full path to the relevant Tesla Master agent configuration xml file
.PARAMETER tablelist
Optional comma separated list of tables to (re)initialize. If left out, all 
tables in the slave config file wll be (re)initialized. Use this when adding new tables to 
an existing setup.
.PARAMETER mappingsfile
Use for heterogeneous replication (i.e. Netezza slaves). This must be the full path to a file
for mapping data types from the master database type to slave database type. You should use the
same file you're going to use when running the slave agent. The mappings files that come with
Tesla should be sufficient but you can change them if you need to for your environment.
.PARAMETER newdatabase
Specify this if you are setting up a database for the first time. Don't specify this if you are
just adding new tables to an existing setup or reinitializing tables, adding a new slave
or a new shard to an existing setup. This switch will cause all required databases to be created on the master,
slave and relay, and it will drop all tables in the CT databases as well.
It won't drop tables in the slave or master database other than tblDDLEvent on the master.
.PARAMETER newslave
Use this when adding a new slave to an existing setup. This will make changes only to the slave side,
it won't modify the relay or master database at all (other than tblCTInitialize/tblCTSlaveVersion).
.PARAMETER newshard
Use this when adding a new shard to an existing sharded setup. This will not mess with the consolidated CT
database on the relay side or the slave side, it will only set up the master portion and 
initialize any data. This flag implies -notfirstshard as well, you don't need to specify both.
.PARAMETER notlastslave
Specify this switch when you are initializing multiple slaves, you must include this for all
but the last slave. Doing so will make sure the first batch is able to start correctly by
setting records in tblCTInitialize and tblCTVersion appropriately.
.PARAMETER notfirstshard
If you are doing a sharded setup, pass this flag for all but the first shard so that
the tables on the slave side don't get truncated or dropped in between each shard.
.PARAMETER reinitialize
Pass this to avoid dropping and recreating slave tables, instead just truncate and then 
reinitialize the data. The purpose of this is to maintain any custom indexes, table distributions
etc. you may have made on the slave side. Note that the schema for the slave MUST be correct already
for this to work.
.PARAMETER yes
Skips prompts for confirmation that occur before dropping all tables on a CT database. Use this
flag only if you're sure the database names in your config file are correct. Also skips prompt
that confirms the intentions of this script at the beginning.
.PARAMETER maxthreads
When initializing more than one table, table initialization happens in parallel. This parameter configures
the maximum number of threads to use. The default is 2.
.INPUTS
You cannot pipe objects to Initialize-DB.
.OUTPUTS
   Writes console messages about its progress to the screen, but doesn't output any objects.
.EXAMPLE
Initialize a new MSSQL -> MSSQL tesla setup with no sharding:
.\Initialize-DB -masterconfigfile "D:\tesla\master.xml" -slaveconfigfile "D:\tesla\slave.xml" -newdatabase
.EXAMPLE
Initialize a new tesla setup with two MSSQL slaves and one Netezza slave. Note the use of -notlast and separate config files:
.\Initialize-DB -masterconfigfile "D:\tesla\master.xml" -slaveconfigfile "D:\tesla\MSSQLslave1.xml" -newdatabase -notlast
.\Initialize-DB -masterconfigfile "D:\tesla\master.xml" -slaveconfigfile "D:\tesla\NZslave1.xml" -mappingsfile "D:\tesla\data_mappings" -newdatabase -notlast 
.\Initialize-DB -masterconfigfile "D:\tesla\master.xml" -slaveconfigfile "D:\tesla\MSSQLslave2.xml" -newdatabase
.EXAMPLE
Initialize a new sharded setup with two MSSQL slaves. Carefully note the use of the notlast, notfirstshard flags:
.\Initialize-DB -masterconfigfile "D:\tesla\master_shard1.xml" -slaveconfigfile "D:\tesla\MSSQLslave1.xml" -newdatabase -notlast 
.\Initialize-DB -masterconfigfile "D:\tesla\master_shard2.xml" -slaveconfigfile "D:\tesla\MSSQLslave1.xml" -newdatabase -notlast -notfirstshard
.\Initialize-DB -masterconfigfile "D:\tesla\master_shard1.xml" -slaveconfigfile "D:\tesla\MSSQLslave2.xml" -newdatabase
.\Initialize-DB -masterconfigfile "D:\tesla\master_shard2.xml" -slaveconfigfile "D:\tesla\MSSQLslave2.xml" -newdatabase -notfirstshard
.EXAMPLE
Add two new tables to an existing Tesla setup on two slaves:
.\Initialize-DB -masterconfigfile "D:\tesla\master.xml" -slaveconfigfile "D:\tesla\MSSQLslave1.xml" -tablelist "table1,table2" -notlast 
.\Initialize-DB -masterconfigfile "D:\tesla\master.xml" -slaveconfigfile "D:\tesla\MSSQLslave2.xml" -tablelist "table1,table2" 
.EXAMPLE
Add a new slave to an existing setup
.\Initialize-DB -masterconfigfile "D:\tesla\master.xml" -slaveconfigfile "D:\tesla\MSSQLslave3.xml" -newslave
.EXAMPLE
Add a new shard to an existing sharded setup
.\Initialize-DB -masterconfigfile "D:\tesla\master_shard3.xml" -slaveconfigfile "D:\tesla\MSSQLslave1.xml" -newshard -notlastslave
.\Initialize-DB -masterconfigfile "D:\tesla\master_shard3.xml" -slaveconfigfile "D:\tesla\MSSQLslave2.xml" -newshard
.NOTES
Version History
v1.0   - Scott Sandler - Initial release
#>
Param(
 [Parameter(Mandatory=$true,Position=1)][string]$masterconfigfile,
 [Parameter(Mandatory=$true,Position=2)][string]$slaveconfigfile,
 [Parameter(Mandatory=$false,Position=3)][string]$tablelist,
 [Parameter(Mandatory=$false,Position=5)][string]$mappingsfile,
 [switch]$newdatabase,
 [switch]$newslave,
 [switch]$newshard,
 [switch]$notlastslave,
 [switch]$notfirstshard,
 [switch]$reinitialize,
 [switch]$yes,
 [Parameter(Mandatory=$false)][int]$maxthreads=2
)
Push-Location (Split-Path -Path $MyInvocation.MyCommand.Definition -Parent)
Import-Module .\Modules\DB
$erroractionpreference = "Stop"

#########################
# This script is somewhat dangerous if the wrong parameters are passed, so 
# unless they explicitly skipped the prompt, we're going to tell them exactly 
# what this script is about to do.
#########################
if (!$yes) {
    Write-Host -ForegroundColor green "Okay, so here's the plan:"
    Write-Host "* We'll load master settings from $masterconfigfile"
    Write-Host "* We'll load slave settings from $slaveconfigfile"
    
    if ($tablelist.length -gt 1) {
        Write-Host "* We're initializing tables only in this list: $tablelist"
    } else {
        Write-Host "* We're initializing all configured tables since -tablelist isn't specified."
    }
    
    if ($mappingsfile.length -gt 1) {
        Write-Host "* We'll use data type mappings from this file: $mappingsfile"
    } else {
        Write-Host "* We won't use a data type mappings file because -mappingsfile isn't specified."
    }
    
    if ($newdatabase) {
        Write-Host "* This is a brand new database setup, which means no existing agents related 
        to this database are running anywhere. No masters, no other shards, no other slaves, 
        nothing else for this database is happening yet. All necessary CT and
        slave databases will be created, and all tables will be dropped on the CT databases."
    } else {
        Write-Host "* This is not a new database, which means there are master and slave agents
        related to this database that are already set up."
    }
    
    if ($newslave) {
        Write-Host "* This is a new slave running on an existing setup for the first time, which means the master
        is already publishing batches and other slaves are already subscribing to those,
        so we will only create objects on the slave side."
    } else {
        Write-Host "* This is not a new slave being added to an existing setup."
    }
    
    if ($newshard) {
        Write-Host "* This is a new shard that will be added to an existing sharded setup,
        which means you already have one or more shards publishing batches, slaves
        already running, and shardcoordinator already set up. All we are going to do
        is set up change tracking on the new shard master side and copy any initial data on
        to the slave (without truncating the slave tables)."
    } else {
        Write-Host "* This is not a new shard being added to an existing sharded setup."
    }
    
    if ($notlastslave) {
        Write-Host "* This is not the last slave that will be initialized for this database
        from this master, which means we will not mark the tables as complete on
        tblCTInitialize yet, and we will not mark the initial batch as published in 
        tblCTVersion."
    } else {
        Write-Host "* This is the last slave that will be initialized for this database from 
        this master, which means we will mark the tables as complete on tblCTInitialize and
        the first batch as published on tblCTVersion."
    }
    
    if ($notfirstshard) {
        Write-Host "* This is a sharded setup, and this is not the first shard being initialized,
        so we will not create or modify the consolidated shard relay DB."
    } elseif (!$newshard) {
        Write-Host "* This is either not a sharded setup, or this is the first shard being initialized
        in a new sharded setup."
    }
    
    if ($reinitialize) {
        Write-Host "* We will be reinitializing tables, which means the appropriate schema is already
        in place on the slave and we do not want to drop and recreate that table. We'll just 
        truncate it and load the data in instead."
    } else {
        Write-Host "* We're initializing the slave tables from scratch, which means they will be
        dropped if they exist. Any custom indexes or table distribution on the slave for these
        tables will be lost."
    }
    
    Write-Host "* We will use at most $maxthreads thread(s) for initializing tables in parallel."
    
    Write-Host -foregroundcolor green "`r`nDo you believe, to the best of your knowledge, 
    that the above information is correct and we're about to do what you need done? "
    
    $result = read-host "[y/n]"
    if ($result -ne "y") {
        throw ("Confirmation to continue not given, exiting with failure.")
    }        
}

#########################
# Function definitions
#########################
Function Drop-AllTables {
[CmdletBinding()]
    param(
    [Parameter(Position=0, Mandatory=$true)] [string]$ServerInstance,
    [Parameter(Position=1, Mandatory=$true)] [string]$Database,
    [Parameter(Position=2, Mandatory=$true)] [bool]$yes
    )
    if ($yes) {
        $prompt = "y"   
    } else {
        $prompt = read-host "Dropping all tables on database $Database on server $ServerInstance. Are you sure? [y/n]"
    }
    if ($prompt -ne "y") {
        throw "Confirmation not provided to drop tables. Exiting with failure!"
    }
    $tables = invoke-sqlcmd2 -serverinstance $serverinstance -database $database `
    -query "select '[' + table_schema + '].[' + table_name + ']' as t from information_schema.tables where table_type = 'BASE TABLE'"
    $cmd = ""
    foreach ($table in $tables) {
        if ($table) {    
            $cmd += "DROP TABLE " + $table.t + "`r`n"
        }
    }
    if ($cmd -ne "") {    
        invoke-sqlcmd2 -serverinstance $serverinstance -database $database -query $cmd
    }
}

Function Drop-AllNetezzaTables {
[CmdletBinding()]
    param(
    [Parameter(Position=0, Mandatory=$true)] [string]$ServerInstance,
    [Parameter(Position=1, Mandatory=$true)] [string]$Database,
    [Parameter(Position=2, Mandatory=$true)] [string]$User,
    [Parameter(Position=3, Mandatory=$true)] [string]$Password,
    [Parameter(Position=4, Mandatory=$true)] [bool]$yes
    )
    if ($yes) {
        $prompt = "y"        
    } else {
        $prompt = read-host "Dropping all tables on database $Database on server $ServerInstance. Are you sure? [y/n]"
    }
    if ($prompt -ne "y") {
        throw "Confirmation not provided to drop tables. Exiting with failure!"
    }
    $result = invoke-netezzaquery -s $serverinstance -database $database -u $user -p $password `
        -query "SELECT TABLENAME FROM _V_TABLE WHERE OBJTYPE = 'TABLE';"
    foreach ($row in $result) {
        if (!$row) { continue }
        $table = $row.TABLENAME
        invoke-netezzaquery -s $serverinstance -database $database -u $user -p $password `
            -query "DROP TABLE $table"         
    }      
}

Function Grant-Permissions($server, $db, $user, $password) {
    $query = "if not exists (select 1 from sys.syslogins where name = '$user') 
    	CREATE LOGIN [$user] WITH PASSWORD='$password', DEFAULT_DATABASE=[$db], DEFAULT_LANGUAGE=[us_english], CHECK_EXPIRATION=OFF, CHECK_POLICY=OFF
    USE $db
    if not exists (SELECT 1 from sys.sysusers where name = '$user')
    	CREATE USER [$user] FOR LOGIN [$user]
    EXEC sp_addrolemember N'db_owner', N'$user'"
    invoke-sqlcmd2 -serverinstance $server -database $db -query $query        
}

Function Create-DB ($server, $db, $type, $user, $password) {
    if ($type -eq "MSSQL") {
        $query = "if not exists (select 1 from sys.databases where name = '$db')
        CREATE DATABASE $db"
        invoke-sqlcmd2 -serverinstance $server -query $query
        $query = "ALTER DATABASE $db SET RECOVERY SIMPLE"
        invoke-sqlcmd2 -serverinstance $server -query $query
        Grant-Permissions $server $db $user $password
    } elseif ($type -eq "Netezza") {
        $query = "select 1 from _v_database where database = '" + $db.ToUpper() + "'"
        $exists = invoke-netezzaquery -s $server -database $db -u $user -p $password -query $query
        if ($exists -eq $null) {
            $query = "CREATE DATABASE $db"
            invoke-netezzaquery -s $server -database $db -u $user -p $password -query $query
        }
    }
}

Function Get-Modifiers([System.Xml.XmlNodeList]$modifiers) {
    $columnmodifiers = $null 
    foreach ($modifier in $modifiers) {
        if (!$modifier) {
            continue
        }
        if ($columnmodifiers -eq $null) {
            $columnmodifiers = "<root>"
        }
        $columnmodifiers += [string]$modifier.OuterXML        
    }
    return $columnmodifiers
}

Function Get-Columns([System.Xml.XmlNode]$columns) {    
    $columnlist = $null
    if ($columns -ne $null) {
        $columnlist = [string]$columns.OuterXML
    }
    return $columnlist
}


####################
# Initialize variables based on arguments
####################

$tablestoinclude = @()
if ($tablelist.length -gt 0) {
    $tablestoinclude = $tablelist.Split(",")
}

Write-Host "Loading slave XML settings"
[xml]$xml = Get-Content $slaveconfigfile

$slave = $xml.SelectSingleNode("/conf/slave").InnerText
$slavetype = $xml.SelectSingleNode("/conf/slaveType").InnerText
$slavedb = $xml.SelectSingleNode("/conf/slaveDB").InnerText
$slavectdb = $xml.SelectSingleNode("/conf/slaveCTDB").InnerText
$slaveuser = $xml.SelectSingleNode("/conf/slaveUser").InnerText
$slavepassword = $xml.SelectSingleNode("/conf/slavePassword").InnerText
$nzloadscriptpath = $xml.SelectSingleNode("/conf/nzLoadScriptPath").InnerText
$netezzastringlength = $xml.SelectSingleNode("/conf/netezzaStringLength").InnerText
$bcppath = $xml.SelectSingleNode("/conf/bcpPath").InnerText
$plinkpath = $xml.SelectSingleNode("/conf/plinkPath").InnerText
$netezzauser = $xml.SelectSingleNode("/conf/netezzaUser").InnerText
$netezzaprivatekeypath = $xml.SelectSingleNode("/conf/netezzaPrivateKeyPath").InnerText

$relay = $xml.SelectSingleNode("/conf/relayServer").InnerText
$relaytype = $xml.SelectSingleNode("/conf/relayType").InnerText
$relaydb = $xml.SelectSingleNode("/conf/relayDB").InnerText
$relayuser = $xml.SelectSingleNode("/conf/relayUser").InnerText
$relaypassword = $xml.SelectSingleNode("/conf/relayPassword").InnerText
$tables = $xml.SelectSingleNode("/conf/tables")
$errorlogdb = $xml.SelectSingleNode("/conf/errorLogDB").InnerText

Write-Host "Loading master XML settings"
[xml]$xml = Get-Content $masterconfigfile

$master = $xml.SelectSingleNode("/conf/master").InnerText
$mastertype = $xml.SelectSingleNode("/conf/masterType").InnerText
$masterdb = $xml.SelectSingleNode("/conf/masterDB").InnerText
$masterctdb = $xml.SelectSingleNode("/conf/masterCTDB").InnerText
$masteruser = $xml.SelectSingleNode("/conf/masterUser").InnerText
$masterpassword = $xml.SelectSingleNode("/conf/masterPassword").InnerText
$sharding = $xml.SelectSingleNode("/conf/sharding").InnerText
$mastertableconf = $xml.SelectSingleNode("/conf/tables")

$mastertables = @()
foreach ($tableconf in $mastertableconf.SelectNodes("table")) {
    $mastertables += ($tableconf.schemaname + "." + $tableconf.name)
}
if ($sharding -eq "true") {
    $sharding = $true
    #slave's relay is the consolidated relay, master's relay is a different database for sharding.
    $consolidatedctdb = $relaydb
    $relaydb = $xml.SelectSingleNode("/conf/relayDB").InnerText
} else {
    $sharding = $false
}

###########################
# This is where we start actually doing stuff
###########################
Write-Host "initializing ctripledes decrypter"
$ctripledes = new-object ctripledes

if ($newdatabase -or $newshard) {
    Write-Host "enabling change tracking on master database"
    $query = "IF NOT EXISTS (select 1 from sys.change_tracking_databases WHERE database_id = DB_ID('$masterdb'))
	ALTER DATABASE [$masterdb] SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 4 DAYS)"
    invoke-sqlcmd2 -serverinstance $master -database $masterdb -query $query
    
    Write-Host "tblDDLEvent on master database"
    $query = "IF OBJECT_ID('dbo.tblDDLEvent') IS NOT NULL
	DROP TABLE tblDDLEVent

    CREATE TABLE [dbo].[tblDDLevent](
    	[DdeID] [int] IDENTITY(1,1) NOT NULL,
    	[DdeTime] [datetime] NOT NULL DEFAULT GETDATE(),
    	[DdeEvent] [nvarchar](max) NULL,
    	[DdeTable] [varchar](255) NULL,
    	[DdeEventData] [xml] NULL,
    PRIMARY KEY CLUSTERED 
    (
    	[DdeID] ASC
    )WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
    ) ON [PRIMARY]"
    invoke-sqlcmd2 -serverinstance $master -database $masterdb -query $query
    $query = "IF EXISTS (select 1 from sys.triggers WHERE name = 'ddl_trig')
	DROP TRIGGER ddl_trig ON DATABASE
    --this one is important!
    SET ANSI_PADDING ON"
    invoke-sqlcmd2 -serverinstance $master -database $masterdb -query $query
    $query ="
    CREATE TRIGGER [ddl_trig]
    ON DATABASE 
    FOR ALTER_TABLE, RENAME
    AS
    SET NOCOUNT ON

    DECLARE @data XML, @EventType nvarchar(max), @TargetobjectType nvarchar(max),@objectType nvarchar(max) ;
    DECLARE @event nvarchar(max), @tablename nvarchar(max)
    SET @data = EVENTDATA();
    SELECT @event = EVENTDATA().value('(/EVENT_INSTANCE/TSQLCommand/CommandText)[1]', 'nvarchar(MAX)'); 
    SELECT @EventType = EVENTDATA().value('(/EVENT_INSTANCE/EventType)[1]', 'nvarchar(MAX)')

    IF @EventType = 'RENAME'
    BEGIN
       SELECT @TargetobjectType = EVENTDATA().value('(/EVENT_INSTANCE/TargetObjectType)[1]', 'nvarchar(MAX)'), 
       @objectType = EVENTDATA().value('(/EVENT_INSTANCE/ObjectType)[1]', 'nvarchar(MAX)')
       
       IF @TargetobjectType = 'TABLE' AND @objectType = 'COLUMN'  
    		SELECT @tablename = EVENTDATA().value('(/EVENT_INSTANCE/TargetObjectName)[1]', 'varchar(256)');      
    END
    ELSE
    		SELECT @tablename = EVENTDATA().value('(/EVENT_INSTANCE/ObjectName)[1]', 'varchar(256)');		


    IF @tablename IS NOT NULL AND EXISTS (select 1 from sys.change_tracking_tables  where object_id = object_id(@tablename))	
    INSERT tblDDLevent (DdeEvent, DdeTable, DdeEventData)
    		SELECT @event,
    		@tablename, 
    		@data	
    ;"
    invoke-sqlcmd2 -serverinstance $master -database $masterdb -query $query
    
    #granting tesla login permissions on master database
    Grant-Permissions $master $masterdb $masteruser $ctripledes.Decrypt($masterpassword)
    
    Write-Host "creating $masterctdb on server $master"
    Create-DB $master $masterctdb $mastertype $masteruser $ctripledes.Decrypt($masterpassword)

    Write-Host "dropping all tables on $masterctdb on server $master"
    Drop-AllTables $master $masterctdb $yes

    #create tblCTInitialize
    $query = "CREATE TABLE dbo.tblCTInitialize (
    tableName varchar(100) NOT NULL PRIMARY KEY,
    iniStartTime datetime NOT NULL,
    inProgress bit NOT NULL,
    iniFinishTime datetime NULL,
    nextSynchVersion bigint NOT NULL
    )"

    Write-Host "creating tblCTInitialize on $masterctdb on server $master"
    invoke-sqlcmd2 -serverinstance $master -database $masterctdb -query $query

    Write-Host "creating $relaydb on server $relay"
    Create-DB $relay $relaydb $relaytype $relayuser $ctripledes.Decrypt($relaypassword)

    if ($relay -ne $master) {
        Write-Host "dropping all tables on $relaydb on server $relay"
        Drop-AllTables $relay $relaydb $yes
    }

    #tblCTversion has no identity on sharded CT dbs
    if ($sharding) {
        $query = "CREATE TABLE [dbo].[tblCTVersion](
    	[CTID] [bigint] NOT NULL PRIMARY KEY,
    	[syncStartVersion] [bigint] NULL,
    	[syncStopVersion] [bigint] NULL,
    	[syncStartTime] [datetime] NULL,
    	[syncStopTime] [datetime] NULL,
    	[syncBitWise] [int] NOT NULL DEFAULT (0)
        )"
    } else {
        $query = "
        CREATE TABLE [dbo].[tblCTVersion](
        	[CTID] [bigint] IDENTITY(1,1) NOT NULL PRIMARY KEY,
        	[syncStartVersion] [bigint] NULL,
        	[syncStopVersion] [bigint] NULL,
        	[syncStartTime] [datetime] NULL,
        	[syncStopTime] [datetime] NULL,
        	[syncBitWise] [int] NOT NULL DEFAULT (0)
        ) 

        CREATE TABLE [dbo].[tblCTSlaveVersion](
        	[CTID] [bigint] NOT NULL,
        	[slaveIdentifier] [varchar](100) NOT NULL,
        	[syncStartVersion] [bigint] NULL,
        	[syncStopVersion] [bigint] NULL,
        	[syncStartTime] [datetime] NULL,
        	[syncStopTime] [datetime] NULL,
        	[syncBitWise] [int] NOT NULL DEFAULT (0),
        	PRIMARY KEY (CTID, slaveIdentifier)
        )"
    }
    Write-Host "creating tblCTVersion/tblCTSlaveVersion on $relaydb on server $relay"

    invoke-sqlcmd2 -serverinstance $relay -database $relaydb -query $query

    Write-Host "getting CHANGE_TRACKING_CURRENT_VERSION() from master"
    $result = invoke-sqlcmd2 -serverinstance $master -database $masterdb -query "SELECT CHANGE_TRACKING_CURRENT_VERSION() as v"
    $version = $result.v
    
    #if this is the first shard in a sharded setup we also need to create the consolidated CT database    
    #otherwise, if it's not the first shard, or if it's a new shard in an existing setup, we skip this portion
    #since the consolidated DB will already have been created.
    if ($sharding -and !$notfirstshard -and !$newshard) {
        Write-Host "creating $consolidatedctdb on server $relay"
        Create-DB $relay $consolidatedctdb $relaytype $relayuser $ctripledes.Decrypt($relaypassword)
        
        Write-Host "dropping all tables on $consolidatedctdb on server $relay"
        Drop-AllTables $relay $consolidatedctdb $yes
$query = @"
CREATE TABLE [dbo].[tblCTVersion](
	[CTID] [bigint] IDENTITY(1,1) NOT NULL PRIMARY KEY,
	[syncStartVersion] [bigint] NULL,
	[syncStopVersion] [bigint] NULL,
	[syncStartTime] [datetime] NULL,
	[syncStopTime] [datetime] NULL,
	[syncBitWise] [int] NOT NULL DEFAULT (0)
) 

CREATE TABLE [dbo].[tblCTSlaveVersion](
	[CTID] [bigint] NOT NULL,
	[slaveIdentifier] [varchar](100) NOT NULL,
	[syncStartVersion] [bigint] NULL,
	[syncStopVersion] [bigint] NULL,
	[syncStartTime] [datetime] NULL,
	[syncStopTime] [datetime] NULL,
	[syncBitWise] [int] NOT NULL DEFAULT (0),
	PRIMARY KEY (CTID, slaveIdentifier)
) 
"@
        Write-Host "creating tblCTVersion/tblCTSlaveVersion on $consolidatedctdb on server $relay"
        invoke-sqlcmd2 -serverinstance $relay -database $consolidatedctdb -query $query
        
        Write-Host "writing version number $version to consolidated tblCTVersion"
        $query = "INSERT INTO tblCTVersion (syncStartVersion, syncStartTime, syncStopVersion, syncBitWise) 
        VALUES (0, '1/1/1990', 0, 0);"
        $result = invoke-sqlcmd2 -serverinstance $relay -database $consolidatedctdb -query $query
    }

    if ($sharding) {
        Write-Host "Getting lastest CTID from consolidated tblCTVersion"
        $result = invoke-sqlcmd2 -serverinstance $relay -database $consolidatedctdb -query "SELECT MAX(CTID) as maxctid from tblCTVersion"
        $ctid = $result.maxctid
        Write-Host "writing version number $version to sharded tblCTVersion for ctid $ctid"
        $query = "INSERT INTO tblCTVersion (CTID, syncStartVersion, syncStartTime, syncStopVersion, syncBitWise) 
        VALUES ($ctid, $version, '1/1/1990', $version, 0);"
        $result = invoke-sqlcmd2 -serverinstance $relay -database $relaydb -query $query
    } else {
        Write-Host "writing version number $version to tblCTVersion"
        $query = "INSERT INTO tblCTVersion (syncStartVersion, syncStartTime, syncStopVersion, syncBitWise) 
        VALUES ($version, '1/1/1990', $version, 0);"
        $result = invoke-sqlcmd2 -serverinstance $relay -database $relaydb -query $query
    }
}

if ($newdatabase -or $newslave) {
    Write-Host "creating $slavectdb on server $slave"
    Create-DB $slave $slavectdb $slavetype $slaveuser $ctripledes.Decrypt($slavepassword)

    Write-Host "creating $errorlogdb on server $relay"
    Create-DB $relay $errorlogdb $relaytype $relayuser $ctripledes.Decrypt($relaypassword)
    
    Write-Host "creating tblCTError in $errorlogdb"
    $query = "
    IF OBJECT_ID('tblCTError') IS NULL
    CREATE TABLE [dbo].[tblCTError](
    	[CelId] [int] IDENTITY(1,1) NOT NULL,
        [CelHeaders] [nvarchar](max) NULL,
    	[CelError] [nvarchar](max) NULL,
    	[CelLogDate] [datetime] NOT NULL DEFAULT GETDATE(),
    	[CelSent] [bit] NOT NULL DEFAULT 0,
    	[CelAcknowledged] [bit] NOT NULL DEFAULT 0
    )"
    invoke-sqlcmd2 -serverinstance $relay -database $errorlogdb -query $query
    
    if ($relay -ne $slave ) {
        Write-Host "dropping all tables on $slavectdb on server $slave"
        if ($slavetype -eq "MSSQL") {        
            Drop-AllTables $slave $slavectdb $yes
        } elseif ($slavetype -eq "Netezza") {
            Drop-AllNetezzaTables $slave $slavectdb $slaveuser $slavepassword $yes
        }
    }
    Write-Host "creating $slavedb on server $slave"
    Create-DB $slave $slavedb $slavetype $slaveuser $ctripledes.Decrypt($slavepassword)
}

#capture slave CTID so that if batches happen on master while we are initializing tables we don't miss out on those changes
if ($sharding) {
    $result = invoke-sqlcmd2 -serverinstance $relay -database $consolidatedctdb -query "SELECT MAX(CTID) as maxctid from tblCTVersion"
} else {
    $result = invoke-sqlcmd2 -serverinstance $relay -database $relaydb -query "SELECT MAX(CTID) as maxctid from tblCTVersion"
}
$slavectid = $result.maxctid


########################
# Begin initialization of actual tables
########################

#this array wll hold a hashtable listing all the arguments that need to be passed for each table to be initialized
$tablestoinitialize = @()

#loop through tables and determine the appropriate arguments to pass to AddTable-ToCT
foreach ($tableconf in $tables.SelectNodes("table")) {
    if ($tablestoinclude.length -gt 0 -and $tablestoinclude -notcontains $tableconf.name) {
        continue
    }
    if ($mastertables -notcontains ($tableconf.schemaname + "." + $tableconf.name)) {
        write-host ("This master doesn't publish " + $tableconf.name + ", skipping")
        continue
    }
    #get XML for master config so that we can get any custom column modifiers/column lists
    [xml]$masterconf = ($mastertableconf.SelectNodes("table") | ? {$_.name -eq $tableconf.name -and $_.schemaname -eq $tableconf.schemaname}).OuterXML
    #parse column modifiers and column lists for master and slave version of this table
    $mastermodifiers = Get-Modifiers $masterconf.SelectNodes("columnModifier") 
    $slavemodifiers = Get-Modifiers $tableconf.SelectNodes("columnModifier") 
    $mastercolumnlist = Get-Columns $masterconf.SelectSingleNode("columnList")
    $slavecolumnlist = Get-Columns $tableconf.SelectSingleNode("columnList")
    
    #hashtable of the arguments we will use when calling AddTable-ToCT
    $arguments = @{"master" = $master; 
        "masterdb" = $masterdb; 
        "slave" = $slave; 
        "slavedb" = $slavedb;
        "slavetype" = $slavetype; 
        "table" = $tableconf.name;
        "schema" = $tableconf.schemaname;
        "user" = $slaveuser;
        "password" = $slavepassword; 
        "slavecolumnlist" = $slavecolumnlist;
        "mastercolumnlist" = $mastercolumnlist;
        "slavecolumnmodifiers" = $slavecolumnmodifiers;
        "mastercolumnmodifiers" = $mastercolumnmodifiers; 
        "netezzastringlength" = $netezzastringlength;
        "mappingsfile" = $mappingsfile;
        "sshuser" = $sshuser;
        "pkpath" = $pkpath;
        "plinkpath" = $plinkpath;
        "nzloadscript" = $nzloadscript;
        "bcppath" = $bcppath;
        "reinitialize" = $reinitialize;
        "notlast" = $notlastslave;
        "notfirstshard" = $notfirstshard;
        "directory" = $pwd #$pwd is a magic variable for the current working directory
    }
    $tablestoinitialize += $arguments
}

#initialize tables in parallel with a configurable throttle
$tablestoinitialize | Invoke-Parallel -Throttle $maxthreads {
    Write-Host ("Calling .\AddTable-ToCT for table " + $_.table)    
    #switch to directory containing the script. required because this is inside a runspace which
    #doesn't inherit the environment of the parent scope.
    cd $_.directory
    $starttime = Get-Date
    #many of these params (i.e. the netezza ones) may be null or empty but that's fine
    #note, switches can be specfied using a bool with the : syntax, i.e. -switch:$true
    .\AddTable-ToCT -master $_.master -masterdb $_.masterdb -slave $_.slave -slavedb $_.slavedb -slavetype $_.slavetype `
        -table $_.table -schema $_.schema -user $_.user -password $_.password -slavecolumnlist $_.slavecolumnlist `
        -mastercolumnlist $_.mastercolumnlist -slavecolumnmodifiers $_.slavecolumnmodifiers -mastercolumnmodifiers $_.mastercolumnmodifiers `
        -netezzastringlength $_.netezzastringlength -mappingsfile $_.mappingsfile -sshuser $_.sshuser -pkpath $_.pkpath -plinkpath $_.plinkpath `
        -nzloadscript $_.nzloadscript -bcppath $_.bcppath -reinitialize:$_.reinitialize -notlast:$_.notlast -notfirstshard:$_.notfirstshard
   
   $duration = [math]::Round((Get-Date).Subtract($starttime).TotalMinutes, 2)
   Write-Host ("Initialization complete for table " + $_.table + " in " + $duration + " minutes")
}

#update row of tblCTVersion, setting syncbitwise to 7
if (($newdatabase -or $newslave) -and !$notfirstshard) {
    write-host "inserting first row in tblCTSlaveVersion"
    $query = "insert into tblCTSlaveVersion (CTID, slaveidentifier, syncstarttime, syncstoptime, syncbitwise)
    VALUES ($slavectid, '$slave', getdate(), getdate(), 255)"
    if ($sharding) {
        invoke-sqlcmd2 -serverinstance $relay -database $consolidatedctdb -query $query
    } else {
        invoke-sqlcmd2 -serverinstance $relay -database $relaydb -query $query
    }
}

if ($newdatabase -or $newshard) {
    if (!$notlastslave) {
        write-host "marking initial batch as done on relay"
        invoke-sqlcmd2 -serverinstance $relay -database $relaydb -query "update tblCTVersion set syncbitwise = 7, syncstoptime = getdate()"
        write-host "marking any in progress rows in tblCTInitialize as complete"
        invoke-sqlcmd2 -serverinstance $master -database $masterctdb -query "update tblCTInitialize set inprogress = 0, inifinishtime = GETDATE() where inprogress = 1"
        if ($sharding -and !$newshard) {
            write-host "marking initial batch as done on relay consolidated table"
            invoke-sqlcmd2 -serverinstance $relay -database $consolidatedctdb -query "update tblCTVersion set syncbitwise = 7, syncstoptime = getdate()"
        }
    }
}