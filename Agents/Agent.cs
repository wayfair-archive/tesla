#region Using Statements
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Text;
using System.Linq;
using System.Data;
using System.Diagnostics;
using Xunit;
#endregion


namespace TeslaSQL.Agents {
    //each agent (master, slave, etc.) should inherit this
    public abstract class Agent {

        public Config config;

        private IDataUtils dataUtils;

        protected Agent(Config config, IDataUtils dataUtils) {
            this.config = config;
            this.dataUtils = dataUtils;
        }

        //every agent should have a Run method
        public abstract void Run();

        public abstract void ValidateConfig();

        /// <summary>
        /// Set field list values for each table in the config 
        /// </summary>
        /// <param name="Server">Server to run on (i.e. Master, Slave, Relay)</param>
        /// <param name="Database">Database name to run on</param>
        /// <param name="tableConfArray">Array of tableconf objects to loop through and set field lists on</param>
        public void SetFieldLists(TServer server, string database, TableConf[] tableConfArray) {
            Dictionary<string, bool> dict;
            foreach (TableConf t in tableConfArray) {
                try {
                    dict = DataUtils.GetFieldList(server, database, t.Name);
                    SetFieldList(t, dict);
                } catch (Exception e) {
                    if (t.stopOnError) {
                        throw e;
                    } else {
                        Logger.Log("Error setting field lists for table " + t.Name + ": " + e.Message + " - Stack Trace:" + e.StackTrace, LogLevel.Error);
                    }
                }                
            }
        }

        /// <summary>
        /// Set several field lists on a TableConf object using its config and an smo table object.
        /// </summary>
        /// <param name="tableConf">A table configuration object</param>
        /// <param name="fields">Dictionary of field names with a bool for whether they are part of the primary key</param>   
        public void SetFieldList(TableConf tableConf, Dictionary<string, bool> fields) {
            //TODO continue to measure the performance of this and consider changing back to a pure sql query 
            Stopwatch st = new Stopwatch();
            st.Start();
            string masterColumnList = "";
            string slaveColumnList = "";
            string mergeUpdateList = "";
            string pkList = "";
            string notNullPKList = "";
            string prefix = "";

            //get dictionary of column exceptions
            Dictionary<string, string> columnModifiers = Config.ParseColumnModifiers(tableConf.columnModifiers);

            foreach (KeyValuePair<string, bool> c in fields) {
                //split column list on comma and/or space, only include columns in the list if the list is specified               
                //TODO for netezza slaves we use a separate type of list that isn't populated here, where to put that?         
                if (tableConf.columnList == null || tableConf.columnList.Contains(c.Key, StringComparer.OrdinalIgnoreCase)) {
                    if (masterColumnList != "") {
                        masterColumnList += ",";
                    }

                    if (slaveColumnList != "") {
                        slaveColumnList += ",";
                    }

                    if (c.Value) {
                        //for columnList, primary keys are prefixed with "CT." and non-PKs are prefixed with "P."
                        prefix = "CT.";

                        //pkList has an AND between each PK column, os if this isn't the first we add AND here
                        if (pkList != "")
                            pkList += " AND ";
                        pkList += "P." + c.Key + " = CT." + c.Key;

                        //not null PK list also needs an AND
                        if (notNullPKList != "")
                            notNullPKList += " AND ";
                        notNullPKList += "P." + c.Key + " IS NOT NULL";
                    } else {
                        prefix = "P.";

                        //merge update list only includes non-PK columns
                        if (mergeUpdateList != "")
                            mergeUpdateList += ",";
                        mergeUpdateList += "P." + c.Key + "=CT." + c.Key;
                    }

                    if (columnModifiers.ContainsKey(c.Key)) {
                        //prefix is excluded if there is a column exception
                        masterColumnList += columnModifiers[c.Key];
                    } else {
                        masterColumnList += prefix + c.Key;
                    }
                    slaveColumnList += c.Key;
                    prefix = "";
                }
            }

            tableConf.masterColumnList = masterColumnList;
            tableConf.slaveColumnList = slaveColumnList;
            tableConf.pkList = pkList;
            tableConf.notNullPKList = notNullPKList;
            tableConf.mergeUpdateList = mergeUpdateList;

            st.Stop();
            Logger.Log("SetFieldList Elapsed time for table " + tableConf.Name + ": " + Convert.ToString(st.ElapsedMilliseconds), LogLevel.Trace);
        }


        /// <summary>
        /// Given a table name and CTID, returns the CT table name
        /// </summary>
        /// <param name="table">Table name</param>
        /// <param name="CTID">Change tracking batch iD</param>
        /// <returns>CT table name</returns>
        public string CTTableName(string table, Int64 CTID) {
            return "tblCT" + table + "_" + Convert.ToString(CTID);
        }

    }
}
