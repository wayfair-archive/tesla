#region Using Statements
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Text;
using System.Linq;
using System.Data.SqlClient;
using System.Data;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;
using System.Diagnostics;
using Xunit;
#endregion


namespace TeslaSQL {
    //each agent (master, slave, etc.) should inherit this
    public abstract class Agent {

        //every agent should have a Run method
        public abstract int Run();

        public abstract void ValidateConfig();

        /// <summary>
        /// Set field list values for each table in the config 
        /// </summary>
        /// <param name="Server">Server to run on (i.e. Master, Slave, Relay)</param>
        /// <param name="Database">Database name to run on</param>
        /// <param name="t_array">Array of tableconf objects to loop through and set field lists on</param>
        public void SetFieldLists(TServer server, string database, TableConf[] t_array) {
            Dictionary<string, bool> dict;
            foreach (TableConf t in t_array) {
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
        /// <param name="t_smo">SMO table object</param>
        /// <param name="t_conf">Configuration TableConf object for the same table as the smo object</param>   
        public void SetFieldList(TableConf t_conf, Dictionary<string, bool> fields) {
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
            Dictionary<string, string> columnModifiers = Config.ParseColumnModifiers(t_conf.columnModifiers);

            foreach (KeyValuePair<string, bool> c in fields) {
                //split column list on comma and/or space, only include columns in the list if the list is specified               
                //TODO for netezza slaves we use a separate type of list that isn't populated here, where to put that?                
                if (t_conf.columnList == null || t_conf.columnList.columns.Contains(c.Key)) {
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

            t_conf.masterColumnList = masterColumnList;
            t_conf.slaveColumnList = slaveColumnList;
            t_conf.pkList = pkList;
            t_conf.notNullPKList = notNullPKList;
            t_conf.mergeUpdateList = mergeUpdateList;

            st.Stop();
            Logger.Log("SetFieldList Elapsed time for table " + t_conf.Name + ": " + Convert.ToString(st.ElapsedMilliseconds), LogLevel.Trace);
        }      
    }
}
