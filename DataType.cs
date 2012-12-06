using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL {
    class DataType {
        public string baseType { get; set; }

        public int characterMaximumLength { get; set; }

        public int numericPrecision { get; set; }

        public int numericScale { get; set; }

        public DataType(string baseType, int characterMaximumLength = 0, int numericPrecision = 0, int numericScale = 0) {
            this.baseType = baseType;
            this.characterMaximumLength = characterMaximumLength;
            this.numericPrecision = numericPrecision;
            this.numericScale = numericScale;
        }
    }
}
