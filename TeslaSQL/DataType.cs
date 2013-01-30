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
        private static Dictionary<SqlFlavor, IList<string>> dataMappings = new Dictionary<SqlFlavor, IList<string>>();

        public string BaseType { get; private set; }
        public int? CharacterMaximumLength { get; private set; }
        public int? NumericPrecision { get; private set; }
        public int? NumericScale { get; private set; }

        public static void LoadDataMappingsFromFile(string filePath) {
            string s;
            using (var reader = new StreamReader(filePath)) {
                s = reader.ReadToEnd();
            }
            LoadDataMappings(s);
        }

        public static void LoadDataMappings(string mappings) {
            using (var reader = new StringReader(mappings)) {
                var flavors = new List<SqlFlavor>();
                string[] headers = reader.ReadLine().Split(new char[] { '\t' });
                foreach (var flavor in headers) {
                    SqlFlavor fl = (SqlFlavor)Enum.Parse(typeof(SqlFlavor), flavor);
                    flavors.Add(fl);
                    dataMappings[fl] = new List<string>();
                }
                while (reader.Peek() > 0) {
                    string[] map = reader.ReadLine().Split(new char[] { '\t' });
                    for (int i = 0; i < map.Length; i++) {
                        dataMappings[flavors[i]].Add(map[i].ToLower());
                    }
                }
            }
        }
        public static string MapDataType(SqlFlavor source, SqlFlavor dest, string datatype) {
            datatype = datatype.ToLower();
            if (dataMappings.Count == 0) {
                throw new Exception("Data mappings file not loaded, unable to map data types! Please specify a data mappings file using the -p argument of Tesla.");
            }
            if (!dataMappings.ContainsKey(source) || !dataMappings.ContainsKey(dest)) {
                throw new Exception("The data mappings file seems to be incorrect, please specify a data type mappings file that maps from " + source + " to " + dest);
            }
            int idx = dataMappings[source].IndexOf(datatype);
            if (idx == -1) {
                return datatype;
            }
            return dataMappings[dest][idx];
        }


        public DataType(string baseType, int? characterMaximumLength = null, int? numericPrecision = null, int? numericScale = null) {
            this.BaseType = baseType;
            this.CharacterMaximumLength = characterMaximumLength;
            this.NumericPrecision = numericPrecision;
            this.NumericScale = numericScale;
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
            if (expected.BaseType != actual.BaseType
                || expected.CharacterMaximumLength != actual.CharacterMaximumLength
                || expected.NumericPrecision != actual.NumericPrecision
                || expected.NumericScale != actual.NumericScale
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
            if (typesUsingMaxLen.Contains(BaseType) && CharacterMaximumLength != null) {
                //(n)varchar(max) types stored with a maxlen of -1, so change that to max
                suffix = "(" + (CharacterMaximumLength == -1 ? "max" : Convert.ToString(CharacterMaximumLength)) + ")";
            } else if (typesUsingScale.Contains(BaseType) && NumericPrecision != null && NumericScale != null) {
                suffix = "(" + NumericPrecision + ", " + NumericScale + ")";
            }

            return BaseType + suffix;
        }
    }
}
