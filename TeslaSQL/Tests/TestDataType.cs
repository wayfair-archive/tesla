using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using TeslaSQL;
using Xunit;

namespace TeslaSQL.Tests {
    public class TestDataType {
        [Fact]
        public void TestMappings() {
            string[] ms = new string[] {
                "binary", "bit", "datetime", "datetime2", "float", "hierarchyid", "image", "money", "ntext", "real", "smalldatetime", "smallmoney", "sql_variant", "text", "tinyint", "uniqueidentifier", "varbinary", "xml"
            };
            string[] nz = new string[] {
                "char", "byteint", "timestamp", "timestamp", "double precision", "varchar(100)", "varchar(100)", "decimal(20,4)", "nvarchar(500)", "double precision", "timestamp", "decimal(20,4)", "nvarchar(500)", "varchar(500)", "smallint", "varchar(64)", "varchar", "nvarchar(500)"
            };
            for (var i = 0; i < ms.Length; i++) {
                Assert.Equal(DataType.MapDataType(SqlFlavor.MSSQL, SqlFlavor.Netezza, ms[i]), nz[i]);
            }
            //not testing going the other way because we do not have a function from MSSQL -> Netezza.
            //for (var i = 0; i < nz.Length; i++) {
            //    Assert.Equal(DataType.MapDataType(SqlFlavor.Netezza, SqlFlavor.MSSQL, nz[i]), ms[i]);
            //}

        }
    }
}
