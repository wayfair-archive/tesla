﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using TeslaSQL.DataUtils;
using System.Text.RegularExpressions;

namespace TeslaSQL.Agents {
    /// <summary>
    /// This agent sends alerts that are generated by other agents.
    /// </summary>
    class Notifier : Agent {
        private IEmailClient emailClient;

        //base keyword invokes the base class's constructor
        public Notifier(IDataUtils dataUtils, IEmailClient emailClient, Logger logger)
            : base(dataUtils, null, logger) {
            this.emailClient = emailClient;
        }

        public override void ValidateConfig() {
            Config.ValidateRequiredHost(Config.RelayServer);
            if (Config.RelayType == SqlFlavor.None) {
                throw new Exception("Notifier agent requires a valid SQL flavor for relay");
            }
        }

        public override void Run() {
            var errors = sourceDataUtils.GetUnsentErrors();
            var errorBlocks = new List<string>();
            var ids = new List<int>();
            
            foreach (DataRow row in errors.Rows) {
                var block = "<div><p><b>" + row.Field<string>("CelHeaders") + "</b></p>";
                block += row.Field<string>("CelError");
                block += "</div><br/>";
                block = Regex.Replace(block, "\r?\n", "<br />");
                errorBlocks.Add(block);
                ids.Add(row.Field<int>("CelId"));
            }
            if (errorBlocks.Count == 0) {
                return;
            }
            emailClient.SendEmail(Config.EmailErrorRecipient, "Errors occurred during changetracking", "<html>" + string.Join("", errorBlocks) + "</html>");
            sourceDataUtils.MarkErrorsSent(ids);
        }
    }
}
