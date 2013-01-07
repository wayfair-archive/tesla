using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL.DataUtils {
    public static class DataUtilsFactory {
        /// <summary>
        /// Returns an appropriate class that implements IDataUtils based on the passed in Sql type
        /// </summary>
        public static IDataUtils GetInstance(Logger logger, TServer server, SqlFlavor flavor) {
            switch (flavor) {
                case SqlFlavor.MSSQL:
                    return new MSSQLDataUtils(logger, server);
                case SqlFlavor.Netezza:
                    return new NetezzaDataUtils(logger, server);
            }
            //if we get here without returning it means something was passed in that isn't supported
            throw new NotImplementedException("Specified SQL types not supported for data copying!");
        }
    }
}
