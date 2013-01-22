using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL {
    /// <summary>
    /// Represents a change table in the ChangeTracking database
    /// </summary>
    public class ChangeTable {
        public readonly string name;
        public readonly Int64? CTID;
        public readonly string slaveName;
        public readonly string schemaName;
        public string ctName {
            get {
                return string.Format("tblCT{0}_{1}", name, CTID);
            }
        }
        public string consolidatedName {
            get {
                return string.Format("tblCT{0}_{1}", name, slaveName);
            }
        }
        public string historyName {
            get {
                return string.Format("tblCT{0}_History", name);
            }
        }

        public ChangeTable(string name, Int64? CTID, String schema, string slaveName) {
            this.name = name;
            this.CTID = CTID;
            this.schemaName = schema;
            this.slaveName = slaveName;
        }
    }
}
