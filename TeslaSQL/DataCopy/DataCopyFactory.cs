using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL.DataUtils;
namespace TeslaSQL.DataCopy {
    public static class DataCopyFactory {
        /// <summary>
        /// Returns an appropriate class that implements IDataCopy based on the passed in source and dest Sql types
        /// </summary>
        public static IDataCopy GetInstance(SqlFlavor sourceSqlFlavor, SqlFlavor destSqlFlavor, IDataUtils sourceDataUtils, IDataUtils destDataUtils) {
            if (sourceDataUtils.GetType() == typeof(TestDataUtils) && destDataUtils.GetType() == typeof(TestDataUtils)) {
                return new TestDataCopy((TestDataUtils)sourceDataUtils, (TestDataUtils)destDataUtils);
            }
            switch (sourceSqlFlavor) {
                case SqlFlavor.MSSQL:
                    if (destSqlFlavor == SqlFlavor.MSSQL) {
                        return new MSSQLToMSSQLDataCopy((MSSQLDataUtils)sourceDataUtils, (MSSQLDataUtils)destDataUtils);
                    } else if (destSqlFlavor == SqlFlavor.Netezza) {
                        return new MSSQLToNetezzaDataCopy((MSSQLDataUtils)sourceDataUtils, (NetezzaDataUtils)destDataUtils);
                    }
                    break;
            }
            //if we get here without returning it means something was passed in that isn't supported
            throw new NotImplementedException("Specified SQL types not supported for data copying!");
        }
    }
}
