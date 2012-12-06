using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml;

namespace TeslaSQL {
    class DDLEvent {
        public int ddeID { get; set; }

        public XmlDocument eventData { get; set; }       

        /// <summary>
        /// Constructor used when initializing this object based on data from a DDL trigger
        /// </summary>
        /// <param name="ddeID">Unique id for this event</param>
        /// <param name="eventData">XmlDocument from the EVENTDATA() SQL function</param>
        public DDLEvent(int ddeID, XmlDocument eventData) {
            this.ddeID = ddeID;
            this.eventData = eventData;
        }

        public List<SchemaChange> Parse(TableConf[] t_array) {
            var schemaChanges = new List<SchemaChange>();

            string eventType = eventData.SelectSingleNode("EVENT_INSTANCE/EventType").InnerText;

            XmlNode node;
            if (eventType == "ALTER_TABLE") {
                node = eventData.SelectSingleNode("EVENT_INSTANCE/AlterTableActionList");
            } else if (eventType == "RENAME") {
                node = eventData.SelectSingleNode("EVENT_INSTANCE/Parameters");
            } else {
                //this is a DDL event type that we don't care about publishing, so ignore it
                return schemaChanges;
            }

            string schemaName = eventData.SelectSingleNode("/EVENT_INSTANCE/SchemaName").InnerText;
            string tableName = eventData.SelectSingleNode("/EVENT_INSTANCE/TargetObjectName").InnerText;
            string columnName = eventData.SelectSingleNode("/EVENT_INSTANCE/ObjectName").InnerText;
            string newObjectName = eventData.SelectSingleNode("/EVENT_INSTANCE/NewObjectName").InnerText;
            string commandText = eventData.SelectSingleNode("/EVENT_INSTANCE/TSQLCommand/CommandText").InnerText;

            //String.Compare method returns 0 if the strings are equal, the third "true" flag is for a case insensitive comparison
            //Get table config object
            TableConf t = t_array.SingleOrDefault(item => String.Compare(item.Name, tableName, ignoreCase: true) == 0);
            
            if (t == null) {
                //the DDL event applies to a table not in our config, so we just ignore it
                return schemaChanges;
            }                      
           
            switch (node.FirstChild.Name) {
                case "Param":
                    //if there is a column list in config for this table on this slave
                    //if it does not specify this column, just continue
                    //if it does specify this column, do the rename 
                    /*
                        * SELECT @sql = 'EXEC ' + @DBName+'.' + @SchemaName+ '.'+ 'sp_rename ''''' + @ObjectName+ '.' + @ColumnName
                        + '''''' + ',' + '''''' + @NewObjectName + ''''', ''''COLUMN'''''
   						
                    SELECT @AggSQL = 'IF EXISTS(SELECT 1 FROM ' +  db_name() +'.' + 'information_schema.tables where table_name like ''' +
                        @CTprefix + @ObjectName + @AggSuffix + ''' ) ' + CHAR(10) + CHAR(13)+
                        'EXEC ' + DB_NAME() +'.' + @SchemaName+ '.'+ 'sp_rename ''''' + @CTprefix + @ObjectName + @AggSuffix + '.' + @ColumnName
                    + '''''' + ',' + '''''' + @NewObjectName + ''''', ''''COLUMN''''' + CHAR(10) + CHAR(13)
                        */
                    break;
                case "Alter":
                    foreach (XmlNode xColumn in eventData.SelectNodes("/EVENT_INSTANCE/AlterTableActionList/Alter/Columns/Name")) {
                    //if this column exists on this slave (don't bother checking column lists etc.)
                    //run the alter command on this table and the history table 
                    /*
                        * SELECT @sql = 'ALTER TABLE ' + @DBName+ '.'+@SchemaName+'.'+ @ObjectName+ ' ALTER COLUMN ' 
                        + @ColumnName + ' ' + @column_type		
                        */
                    //if history table exists, run it there too
                    }
                    break;
                case "Create":
                    foreach (XmlNode xColumn in eventData.SelectNodes("/EVENT_INSTANCE/AlterTableActionList/Create/Columns/Name")) {
                        //if columnlist for this table is specified
                    }                    
                    break;
                case "Drop":
                    foreach (XmlNode xColumn in eventData.SelectNodes("/EVENT_INSTANCE/AlterTableActionList/Drop/Columns/Name")) {
                        //if columnlist for this table is specified
                    }   
                    break;
            }
            return schemaChanges;
        }
    }

}
