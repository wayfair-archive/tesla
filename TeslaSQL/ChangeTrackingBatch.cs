using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace TeslaSQL {
    /// <summary>
    /// Class representing a change tracking batch
    /// </summary>
    public class ChangeTrackingBatch {

        public Int64 CTID { get; set; }

        public Int64 SyncStartVersion { get; set; }

        public Int64 SyncStopVersion { get; set; }

        public Int32 SyncBitWise { get; set; }

        public DateTime? SyncStartTime { get; set; }

        public ChangeTrackingBatch(Int64 CTID, Int64 syncStartVersion, Int64 syncStopVersion, Int32 syncBitWise) {
            this.CTID = CTID;
            this.SyncStartVersion = syncStartVersion;
            this.SyncStopVersion = syncStopVersion;
            this.SyncBitWise = syncBitWise;
        }

        public ChangeTrackingBatch(Int64 CTID, Int64 syncStartVersion, Int64 syncStopVersion, Int32 syncBitWise, DateTime syncStartTime)
            : this(CTID, syncStartVersion, syncStopVersion, syncBitWise) {
            this.SyncStartTime = syncStartTime;
        }

        public ChangeTrackingBatch(DataRow row) {
            CTID = row.Field<Int64>("CTID");
            long? start = row.Field<Int64?>("syncStartVersion");
            if (start.HasValue) { SyncStartVersion = start.Value; }
            long? stop = row.Field<Int64?>("syncStopVersion");
            if (stop.HasValue) { SyncStopVersion = stop.Value; }
            SyncBitWise = row.Field<Int32>("syncBitWise");
            if (row.Table.Columns.Contains("syncStartTime")) {
                SyncStartTime = row.Field<DateTime>("syncStartTime");
            }
        }

        /// <summary>
        /// Compare two ChangeTrackingBatch objects. Used for unit tests.
        /// </summary>
        public bool Equals(ChangeTrackingBatch expected) {
            return (CTID == expected.CTID
                && SyncStartVersion == expected.SyncStartVersion
                && SyncStopVersion == expected.SyncStopVersion
                && SyncBitWise == expected.SyncBitWise);
        }

        public string schemaChangeTable { get { return "tblCTSchemaChange_" + CTID; } }
        public string infoTable { get { return "tblCTTableInfo_" + CTID; } }
    }
}
