using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL {
    class DataType {
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
    }
}
