using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL {
    public class RowCounts {
        public readonly int Inserted;
        public readonly int Deleted;
        public RowCounts(int ins, int del) {
            Inserted = ins;
            Deleted = del;
        }
        public override string ToString() {
            return "Inserted: " + Inserted + " Deleted: " + Deleted + " ";
        }
    }
}
