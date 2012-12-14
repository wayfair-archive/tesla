using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL.DataCopy {
    public interface IDataCopy {
        /// <summary>
        /// Copy the contents of a table from source to destination
        /// </summary>
        /// <param name="sourceDB">Source database name</param>
        /// <param name="table">Table name</param>
        /// <param name="schema">Table's schema</param>
        /// <param name="destDB">Destination database name</param>
        /// <param name="timeout">Used as timeout for both the query and the bulk copy</param>
        void CopyTable(string sourceDB, string table, string schema, string destDB, int timeout, string destTableName = null);

        void CopyTableDefinition(string sourceDB, string sourceTableName, string schema, string destDB, string destTableName);
    }
}
