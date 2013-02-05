# Tesla Replicator

Tesla is a multi threaded, incremental, batch based data replication tool which is built with heterogeneous sources and destinations in mind.

Currently it supports Microsoft SQL Server 2008 or greater as a master (publisher) and both SQL Server and Netezza as slaves. Support for Hive slaves is in the works. It would be possible to add MySQL and PostgreSQL as masters and/or slaves as well in the future, though we are not currently working on this.

Tesla is designed by DBAs, with DBAs in mind. It's meant to be easy to configure, initialize, monitor and troubleshoot. Everything is based on configuration files, which we highly recommend storing in version control and deploying like any other application. It integrates with statsD/graphite for performance monitoring and graylog for centralized logging, but if you don't wish to use those tools you certainly don't need to.

## Why use Tesla?

We developed Tesla because we needed a way to incrementally replicate data from MSSQL to Netezza. We had too much data to do nightly full dumps of the data, and we had too many updates and deletes to rely on simple timestamp-based ETL scripts. Since data warehouses like Netezza and Hive are bad at small transactions but good at big batches, we wanted a batch based replication system where we could get a consistent view of a databases's tables in one batch. We created Tesla to leverage MSSQL's change tracking feature, finding out which rows had been inserted, updated or deleted since the last batch and replicating those changes to Netezza.

Once we started using it though, we realized it could replace SQL Server's Transactional Replication for some use cases we had. While SQL's replication is great when extremely low latency is required, it's also single threaded and row-based, so it does not perform well for high rates of change or across high latency WAN links (i.e. cross country or continent). SQL's replication often falls behind, and with a high enough rate of change it can take hours to catch up. Tesla, on the other hand, being batch based and multi threaded, is able to replicate these large change sets across countries without falling behind. If it does fall behind or if it's turned off for a while, it has built in mechanisms to consolidate and dedupe change sets, ensuring it will always catch back up.

The first iteration of Tesla, created in 2009, was based on t-sql stored procedures, functions and SSIS packages. In 2012 we decided we wanted to redesign it to be more decoupled and extensible to different types of masters and slaves.

## Requirements

The .NET Framework v4.0 is required to build and run Tesla. For Netezza slaves, you must install the NZOLEDB driver, which can be downloaded from IBM if you have a Netezza server. 

## Architecture

Tesla is made up of several decoupled agents. Each agent is its own separate Tesla process which has its own configuration file and must be scheduled to run on its own schedule. This allows you to decide when and where each individal agent will run.

- **Master**: publishes change sets (delta tables) from a Master server to a Relay server. You should decide how often to run this based on your latency requirements and rate of change.
- **Slave**: downloads change sets from a Relay server and applies them to a Slave server. You need one slave agent per slave server, as they are entirely independent of each other. 
- **ShardCoordinator**: use this only if you have sharded masters and wish to consolidate the sharded data into a single database on slaves. It consolidates the change sets from each shard down into one batch, and makes sure master agents on each shard start at roughly the same time.
- **Notifier**: this sends e-mail alerts that are generated by **non-critical** errors in Tesla, such as errors for tables that are configured with stopOnError = false. It sends all alerts in one e-mail to avoid spamminess. A good starting point for frequency is once per hour. Note, critical alerts do not go through this process at all, they cause Tesla agents to immediately exit with failure, and alerting should be done by your job scheduler (i.e. SQL Agent operators). No matter how many databases you publish in Tesla you only need one notifier per relay server.
- **MasterMainteneance**: drops old tables on the master based on configured retention settings. You can reuse the master's config file for this agent.
- **RelayMaintenance**: drops old tables on the relay server. You can use the master or slave config file for this as long as the retention settings are specified.
- **SlaveMaintenance**: drops old tables on the slave server. You can reuse the slave config file for this agent.

We recommend using SQL Server Agent as a scheduler for these agents, set to alert someone if a job fails. You can use any scheduler you like though. 

Note that the masters and slaves don't need to know about each other at all, they just need to talk to the relay server. One benefit of this architecture is that you can implement custom masters or slaves outside of Tesla by just interacting with the relay database similarly to how Tesla does. For example, you could create a custom "slave-like" process that uses the delta tables published by Tesla for things like processing events, expiring cache keys for data that has changed, etc. which can save you from having to write performance intensive triggers on your primary databases.

Also your relay doesn't have to be its own server. The relay can be the same server as a master or slave. Tesla will recognize this and avoid unnecessary copying of data. Currently the only database engine supported for relay servers is MSSQL.

## Features

- **Batch Based:** a given setup of Tesla replicates any number of tables in a single database. Batches allow slaves to get a mostly consistent view of the data at the time the batch started.
- **Decoupled:** the master continues to publish batches and slaves pull them on their own time. If you take a slave down for maintenance or a slave falls behind, it doesn't hinder the master or other slaves.
- **Configurable error handling:** you choose which tables are important enough to abort the process if they fail. For the rest, you can set up e-mail alerts if something breaks.
- **Alerting:** the Notifier agents sends alerts for non-critical tables, configured with the stopOnError setting. These alerts are deduped to avoid spam. How often the alerts are sent depends on how often you choose to run the notifier agent.
- **Automated Schema Changes:** Tesla is able to replicate schema changes. For MSSQL slaves it's able to handle column additions, drops, renames and data type modifications. For Netezza only additions and drops are handled, renames and modifications will result in an alert through the Notifier agent, since you need to drop/recreate the slave tables manually to apply those.
- **Table selection:** masters can be configured to publish a subset of the tables in a database. Different slaves can subscribe to different subsets of the tables that the master publishes.
- **Column selection:** similar to tables, master and slave can be configured to publish/subscribe to a subset of columns on a table. 
- **Column modifiers:** allow you to shorten individual text fields to a specified length, both for master and slave. This is especially useful when the slave database engine has stricter limits on row size than the master (i.e. Netezza, MySQL), or when you have large text fields that the master doesn't need to publish at all.
- **Sharding:** sharded masters are supported via the Shard Coordinator agent. Data from all shards is consolidated down to look like just one database for the data warehouse. 
- **Batch sizes** can be limited if you wish. For example if you have a nightly process that does a huge number of transactions, when Tesla goes to pull or apply changes you might get some blocking/locking. You can set a maximum number of transactions (not rows!) to be captured in a batch on the master using the maxBatchSize config parameter. You can also override this threshold during a specific time window (i.e. for catching up late at night).
- **Magic hours** allow you to have a slave that applies schema changes all day, but only actually replicates data changes after specific time windows are passed. We use this for netezza slaves to ensure data up to midnight is applied each night before various analytics jobs begin.

## Developing

To build Tesla, simply open the csproj file in Visual Studio and press Build, or use msbuild. Once done, what you need to actually deploy is TeslaSQL\bin\Release\TeslaSQL.exe, log4net.dll and gelf4net.dll.

To run the unit tests, use Tests/xunit-1.9.1/xunit.gui.x86.exe or run the console version. Make sure all tests pass before submitting a pull request!

Feel free to submit pull requests, we'll get to them as soon as we can.

## Setup, Configuration and Initialization

Once you've compiled the executable there is no actual install process. You can simply copy it, along with log4net.dll and gelf4net.dll, into a folder on the server(s) that will be running it. From there, you can run TeslaSQL.exe --help to see the command line options.

**Configuration:** edit the example configuration files to match your environment (see the files in the examples folder). Further details about individal configuration parameters are in comments in the config files. You need to pass a config file location to each instance of Tesla, as well as a log4net config file location. For details on the log4net config, see [this link](http://logging.apache.org/log4net/release/manual/configuration.html) or work off of our example. To generate a basic config for the tables section, run a query like this on the master with results to text mode on, and edit the resulting config to define which tables are stopOnError, which need custom column lists or modifiers, and which ones to exclude.

```sql
SET NOCOUNT ON
select '<table>
  <schemaName>' + TABLE_SCHEMA + '</schemaName>
  <name>' + TABLE_NAME + '</name>
  <stopOnError>true</stopOnError>  
</table>'
FROM INFORMATION_SCHEMA.TABLES
```

Note that for Netezza slaves there are some special options that must be set in the config file before initialization. See the Netezza considerations section below.

Maintenance agents can use the same config file as their corresponding agents, and RelayMaintenance can use any config file that points it to the right relay database.

**Initialization:** Initialization is done using scripts\Initialize-DB.ps1. Open a powershell prompt, change to the directory containing Tesla and run `Get-Help .\scripts\Initialize-DB -full` to read the help. Make sure to read the help carefully, it explains basically everything about how to do initialization. This is an important step and it can be easy to make a mistake if your setup is complicated (sharding, many slaves, etc.). If you're initializing big tables, you should do this from the server that will run Tesla, rather than your workstation, to eliminate unnecessary data movement between your datacenter(s) and your office. 

**Setup:** Set up jobs in the scheduler of your choice to run the various tesla agents. For SQL Server Agent, use the Operating System command task to execute Tesla agents. You need one job per agent, each with the command line options pointing to the appropriate config files. You should enable e-mail alerts for failure on those jobs. Make sure to set up all the required agents. You'll usually want the slave agent(s) to run pretty often (i.e. once per minute) since if there is no work to do it will just quickly exit. 

## Netezza special considerations

Netezza slaves require a bit more setup than SQL Server slaves. Specifically, you need to:

- Install the NZOLEDB driver on whichever windows server you will use to run the Tesla slave agent. This driver is not publicly available, but can be downloaded from IBM if you have a Netezza server.
- Create an NFS share that your windows server can write to and your Netezza server can read from (mounted on the Netezza server). Use this as the bcpPath config variable in the slave config file. If you don't wish to do this you can try other methods like rsync or scp, but you'll need to modify the code in the relevant DataCopy class to do so.
- Put in place a shell script somewhere on the Netezza server that can load data from said NFS share. See the example script at `scripts/load_data_tesla.sh`. As the comments at the top of the script mention, this script may need to be modified to work in your environment. Of course you must also make the script executable using chmod. Set nzLoadScriptPath in the slave config to this path.
- Create or identify a user account on your Netezza server that Tesla can use to ssh into the server and run said shell script. Create an ssh key for that user using `ssh-keygen -t rsa` and copy the private key file somewhere that Tesla can use it (netezzaPrivateKeyPath config variable).
- Ensure that user has environment variables that allow it to connect to nzsql and run nzload with enough permissions to load data into the CT databases.
- You must accept the Netezza server's host key from the windows server that will be running Tesla, as the user that will be running Tesla. To do this open a command prompt and run: `runas /user:domain\username cmd.exe`, type the password for that user and then run a command using plink such as `plink.exe -ssh -v -l netezzausername -i D:\path\to\private_key_file.ppk netezzahostname ls`, which will try to connect to the Netezza host and run ls. The netezzahostname, username and private key file should all be the exact same values that you use in the slave config file. See [here](http://www.chiark.greenend.org.uk/~sgtatham/putty/wishlist/accept-host-keys.html) for details on why plink requires this. Also note, most Netezza boxes actually have two hosts and it may fail over between them during outages or maintenance, which can cause the host key to change and can require you to repeat this step in the future. Tesla will throw a descriptive error message if the host key is not accepted when it runs.
- For Netezza slaves you need to use a data_mappings file which maps data types from the source database type to the destination type. The path to this is a command line argument of tesla. The mappings file in the examples folder should be sufficient for your most use cases.

## Other Administrative Tasks

Errors may occur due to faulty configuration, tables not existing, unsupported schema changes, etc. and these things may need to be fixed manually.

If you want to add a new table into Tesla while it's running, you can use the Initialize-DB script and tell it to only initialize a specific table or list of tables. Instructions for this are in the Get-Help output of the powershell script. You should be able to seamlessly add/remove/reinitialize tables without ever stopping Tesla.

If you find that a table gets out of sync for any reason, you can use the $reinitialize flag of the add table script to reinitialize it. This will keep any custom indexes or table distributions you put on the slave and just reinitialize the data. Possible causes of a table getting out of sync include:

- Someone truncates the table on the master. Currently in MSSQL, truncating a table that has change tracking enabled produces no meaningful events that the CHANGETABLE() function exposes, meaning we are unable to know that a truncate has happened in Tesla. You should not truncate tables that are copied by Tesla, or if you do you'll have to truncate them on slaves too.
- Someone writes directly to the slave. Obviously you shouldn't do this - make sure to lock down permissions on the slave database so that only tesla can write to it, and limit the number of users/applications with sysadmin access.
- A table is set to stopOnError = false and something bad happens, and nobody deals with the error. 
- A bug in MSSQL's change tracking. Hopefully there aren't any of those.
- A bug in Tesla. Let us know if you think you find one, but please make sure it's not any of the above items, and ideally include a pull request to fix it!

## Authors

**Scott Sandler**
- http://twitter.com/scott_sandler
- http://github.com/ssandler

**Alexander Corwin**
- http://github.com/foxlisk

## Copyright and license
Copyright 2013 Wayfair, LLC.

Licensed under the BSD3 license, see LICENSE file.
