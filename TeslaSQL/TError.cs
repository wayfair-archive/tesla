using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL {
   public struct TError {
       public readonly string headers;
       public readonly string message;
       public readonly DateTime logDate;
       public readonly int id;
       public TError(string headers, string message, DateTime logDate, int id) {
           this.headers = headers;
           this.message = message;
           this.logDate = logDate;
           this.id = id;
       }
    }
}
