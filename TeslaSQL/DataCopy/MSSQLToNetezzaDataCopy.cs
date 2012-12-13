using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;
using System.Data.SqlClient;
using Microsoft.SqlServer.Management.Smo;

namespace TeslaSQL.DataCopy {
    public class MSSQLToNetezzaDataCopy : IDataCopy {

        private MSSQLDataUtils sourceDataUtils { get; set; }
        private NetezzaDataUtils destDataUtils { get; set; }

        public MSSQLToNetezzaDataCopy(MSSQLDataUtils sourceDataUtils, NetezzaDataUtils destDataUtils) {
            this.sourceDataUtils = sourceDataUtils;
            this.destDataUtils = destDataUtils;
        }

        public void CopyTable(string sourceDB, string table, string schema, string destDB, int timeout) {
            throw new NotImplementedException();
        }

    }
}
