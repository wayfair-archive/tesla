using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL {
    public class SyncVersion {
        public readonly long syncStartVersion;
        public readonly long syncStopVersion;
        public readonly int syncBitWise;
        public readonly DateTime syncStartTime;

        public SyncVersion(long syncStartVersion, long syncStopVersion, int syncBitWise, DateTime syncStartTime) {
            this.syncStartVersion = syncStartVersion;
            this.syncStopVersion = syncStopVersion;
            this.syncBitWise = syncBitWise;
            this.syncStartTime = syncStartTime;
        }
    }
}
