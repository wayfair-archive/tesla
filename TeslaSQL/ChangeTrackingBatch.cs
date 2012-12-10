using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL {
    /// <summary>
    /// Class repsenting a change tracking batch
    /// </summary>
    class ChangeTrackingBatch {
        public Int64 CTID { get; set; }

        public Int64 syncStartVersion { get; set; }

        public Int64 syncStopVersion { get; set; }

        public Int32 syncBitWise { get; set; }

        public ChangeTrackingBatch(Int64 CTID, Int64 syncStartVersion, Int64 syncStopVersion, Int32 syncBitWise) {
            this.CTID = CTID;
            this.syncStartVersion = syncStartVersion;
            this.syncStopVersion = syncStopVersion;
            this.syncBitWise = syncBitWise;
        }
    }
}
