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

        public Int64 syncStartVersion { get; set; }

        public Int64 syncStopVersion { get; set; }

        public Int32 syncBitWise { get; set; }

        public DateTime? syncStartTime { get; set; }

        public ChangeTrackingBatch(Int64 CTID, Int64 syncStartVersion, Int64 syncStopVersion, Int32 syncBitWise) {
            this.CTID = CTID;
            this.syncStartVersion = syncStartVersion;
            this.syncStopVersion = syncStopVersion;
            this.syncBitWise = syncBitWise;
        }

        public ChangeTrackingBatch(Int64 CTID, Int64 syncStartVersion, Int64 syncStopVersion, Int32 syncBitWise, DateTime syncStartTime)
            : this(CTID, syncStartVersion, syncStopVersion, syncBitWise) {
            this.syncStartTime = syncStartTime;
        }

        public ChangeTrackingBatch(DataRow row) {
            CTID = row.Field<Int64>("CTID");
            long? start = row.Field<Int64?>("syncStartVersion");
            if (start.HasValue) { syncStartVersion = start.Value; }
            long? stop = row.Field<Int64?>("syncStopVersion");
            if (stop.HasValue) { syncStopVersion = stop.Value; }
            syncBitWise = row.Field<Int32>("syncBitWise");
            if (row.Table.Columns.Contains("syncStartTime")) {
                syncStartTime = row.Field<DateTime>("syncStartTime");
            }
        }

        /// <summary>
        /// Compare two ChangeTrackingBatch objects. Used for unit tests.
        /// </summary>
        public bool Equals(ChangeTrackingBatch expected) {
            if (CTID != expected.CTID
                || syncStartVersion != expected.syncStartVersion
                || syncStopVersion != expected.syncStopVersion
                || syncBitWise != expected.syncBitWise
               ) {
                return false;
            } else {
                return true;
            }
        }

    }
}
