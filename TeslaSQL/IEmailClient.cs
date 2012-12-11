using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TeslaSQL {
    interface IEmailClient {
        void SendEmail(string recipient, string subject, string body);
    }
}
