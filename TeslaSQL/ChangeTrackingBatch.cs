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
            syncStartVersion = row.Field<Int64>("syncStartVersion");
            syncStopVersion = row.Field<Int64>("syncStopVersion");
            syncBitWise = row.Field<Int32>("syncBitWise");
            if (row.Table.Columns.Contains("syncStartTime")) {
                syncStartTime = row.Field<DateTime>("syncStartTime");
            }
        }

    }
}
