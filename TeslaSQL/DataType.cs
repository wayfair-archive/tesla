using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL {
    /// <summary>
    /// Class repsenting a SQL data type including length/precision
    /// </summary>
    public class DataType {
        public string baseType { get; set; }

        public int? characterMaximumLength { get; set; }

        public int? numericPrecision { get; set; }

        public int? numericScale { get; set; }

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
        public static bool Compare(DataType expected, DataType actual) {
            if (expected == null && actual == null) {
                return true;
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
    }
}
