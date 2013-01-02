using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace TeslaSQL {
    /// <summary>
    /// Class repsenting a SQL data type including length/precision
    /// </summary>
    public class DataType {

        private static readonly string dataMappingFile = @"D:\tesla\data_mappings";
        private static Dictionary<SqlFlavor, IList<string>> dataMappings = new Dictionary<SqlFlavor, IList<string>>();

        public string baseType { get; set; }
        public int? characterMaximumLength { get; set; }
        public int? numericPrecision { get; set; }
        public int? numericScale { get; set; }

        static DataType() {
            LoadDataMappings();
        }

        private static void LoadDataMappings() {
            using (var reader = new StreamReader(dataMappingFile)) {
                var flavors = new List<SqlFlavor>();
                string[] headers = reader.ReadLine().Split(new char[] { '\t' });
                foreach (var flavor in headers) {
                    SqlFlavor fl = (SqlFlavor)Enum.Parse(typeof(SqlFlavor), flavor);
                    flavors.Add(fl);
                    dataMappings[fl] = new List<string>();
                }
                while (!reader.EndOfStream) {
                    string[] map = reader.ReadLine().Split(new char[] { '\t' });
                    for (int i = 0; i < map.Length; i++) {
                        dataMappings[flavors[i]].Add(map[i].ToLower());
                    }
                }
            }
        }
        public static string MapDataType(SqlFlavor source, SqlFlavor dest, string datatype) {
            datatype = datatype.ToLower();
            int idx = dataMappings[source].IndexOf(datatype);
            if (idx == -1) {
                return datatype;
            }
            return dataMappings[dest][idx];
        }


        public DataType(string baseType, int? characterMaximumLength = null, int? numericPrecision = null, int? numericScale = null) {
            this.baseType = baseType;
            this.characterMaximumLength = characterMaximumLength;
            this.numericPrecision = numericPrecision;
            this.numericScale = numericScale;
        }

        /// <summary>
        /// Compare two DataType objects that may both be null. Used for unit tests
        /// </summary>
        /// <param name="expected">Expected data type</param>
        /// <param name="actual">Actual data type</param>
        /// <returns>True if the objects are equal or both null</returns>
        public static bool Equals(DataType expected, DataType actual) {
            if (expected == null && actual == null) {
                return true;
            }
            if (expected == null || actual == null) {
                return false;
            }
            if (expected.baseType != actual.baseType
                || expected.characterMaximumLength != actual.characterMaximumLength
                || expected.numericPrecision != actual.numericPrecision
                || expected.numericScale != actual.numericScale
                ) {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Convert data type to string
        /// </summary>
        /// <returns>String expression representing the data type</returns>
        public override string ToString() {
            var typesUsingMaxLen = new string[4] { "varchar", "nvarchar", "char", "nchar" };
            var typesUsingScale = new string[2] { "numeric", "decimal" };

            string suffix = "";
            if (typesUsingMaxLen.Contains(baseType) && characterMaximumLength != null) {
                //(n)varchar(max) types stored with a maxlen of -1, so change that to max
                suffix = "(" + (characterMaximumLength == -1 ? "max" : Convert.ToString(characterMaximumLength)) + ")";
            } else if (typesUsingScale.Contains(baseType) && numericPrecision != null && numericScale != null) {
                suffix = "(" + numericPrecision + ", " + numericScale + ")";
            }

            return baseType + suffix;
        }
    }
}
