using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;

namespace TeslaSQL.Agents {
   static class MaintenanceHelper {
        /// <summary>
        /// Drops all tables in the list of tables given that have names ending in _{CTID} where CTID is one of the numbers in ctids
        /// </summary>
        /// <param name="dataUtils">Data utils for the server on which to drop the tables</param>
        public static void DeleteOldTables(IEnumerable<long> ctids, IEnumerable<TTable> tables, IDataUtils dataUtils, string dbName ) {
            foreach (var table in tables) {
                int lastUnderscore = table.name.LastIndexOf('_');
                if (lastUnderscore == -1) {
                    continue;
                }
                string end = table.name.Substring(lastUnderscore + 1);

                int tableCtid;
                if (!int.TryParse(end, out tableCtid)) {
                    continue;
                }
                if (ctids.Contains(tableCtid)) {
                    dataUtils.DropTableIfExists(dbName, table.name, table.schema);
                }
            }
        }
    }
}
