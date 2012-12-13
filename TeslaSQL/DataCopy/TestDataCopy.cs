using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using TeslaSQL.DataUtils;
namespace TeslaSQL.DataCopy {
    public class TestDataCopy : IDataCopy {

        private TestDataUtils sourceDataUtils { get; set; }
        private TestDataUtils destDataUtils { get; set; }

        public TestDataCopy(TestDataUtils sourceDataUtils, TestDataUtils destDataUtils) {
            this.sourceDataUtils = sourceDataUtils;
            this.destDataUtils = destDataUtils;
        }

        public void CopyTable(string sourceDB, string table, string schema, string destDB, int timeout) {
            //create a copy of the source table (data and schema)
            DataTable copy = sourceDataUtils.testData.Tables[schema + "." + table, sourceDataUtils.GetTableSpace(sourceDB)].Copy();
            //change the namespace to be the dest server
            copy.Namespace = destDataUtils.GetTableSpace(destDB);
            //add it to the dataset
            destDataUtils.testData.Tables.Add(copy);
            //commit
            //testData.AcceptChanges();
        }
    }
}
