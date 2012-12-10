using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Mail;

namespace TeslaSQL {
    class SimpleEmailClient : IEmailClient {
        private string host;
        private string fromAddress;
        private int port;
        public SimpleEmailClient(string host, string fromAddress, int port) {
            this.host = host;
            this.fromAddress = fromAddress;
            this.port = port;
        }
        public void SendEmail(string recipient, string subject, string body) {
            MailMessage message = new MailMessage(fromAddress, recipient, subject, body);
            SmtpClient client = new SmtpClient(host, port);
            client.Send(message);
        }
    }
}
