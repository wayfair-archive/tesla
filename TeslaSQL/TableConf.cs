using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Serialization;
using TeslaSQL.DataUtils;

namespace TeslaSQL {

    [XmlType("table")]
    public class TableConf {
        [XmlElement("name")]
        public string Name { get; set; }

        [XmlElement("schemaName")]
        public string schemaName { get; set; }

        [XmlElement("stopOnError")]
        public bool stopOnError { get; set; }

        [XmlArrayItem("column")]
        public string[] columnList { get; set; }

        [XmlElement("columnModifier")]
        public ColumnModifier[] columnModifiers { get; set; }

        private Dictionary<string, string> parsedColumnModifiers_m;
        [XmlIgnore]
        public Dictionary<string, string> parsedColumnModifiers {
            get {
                if (parsedColumnModifiers_m == null) {
                    parsedColumnModifiers_m = Config.ParseColumnModifiers(this.columnModifiers);
                }
                return parsedColumnModifiers_m;
            }
        }

        //used only on slaves to keep a historical record of changes
        [XmlElement("recordHistoryTable")]
        public bool recordHistoryTable { get; set; }

        [XmlIgnore]
        public IList<TColumn> columns = new List<TColumn>();

        [XmlIgnore]
        public string masterColumnList {
            get {
                return string.Join(",",
                    columns.Select(col => {
                        return col.isPk ? "CT." + col.name : "P." + col.name;
                    }
                ));

            }
        }

        [XmlIgnore]
        public string modifiedMasterColumnList {
            get {
                return string.Join(",",
                        columns.Select(col => {
                            if (parsedColumnModifiers.ContainsKey(col.name)) {
                                return parsedColumnModifiers[col.name];
                            } else {
                                return col.isPk ? "CT." + col.name : "P." + col.name;
                            }
                        }
                    ));
            }
        }


        [XmlIgnore]
        public string simpleColumnList {
            get {
                return string.Join(",", columns.Select(col => col.name));
            }
        }


        [XmlIgnore]
        public string netezzaColumnList {
            get {
                return string.Join(",", columns.Select(col => NetezzaDataUtils.MapReservedWord(col.name)));
            }
        }

        [XmlIgnore]
        public string slaveColumnList {
            get {
                return simpleColumnList;
            }
        }

        [XmlIgnore]
        public string mergeUpdateList {
            get {
                return string.Join(
                    ",",
                    columns.Where(c => !c.isPk)
                    .Select(c => String.Format("P.{0}=CT.{0}", c.name)));
            }
        }

        [XmlIgnore]
        public string pkList {
            get {
                return string.Join(
                    " AND ",
                    columns.Where(c => c.isPk)
                    .Select(c => String.Format("P.{0} = CT.{0}", c.name)));
            }
        }

        [XmlIgnore]
        public string notNullPKList {
            get {
                return string.Join(
                    " AND ",
                    columns.Where(c => c.isPk)
                    .Select(c => String.Format("P.{0} IS NOT NULL", c.name)));
            }
        }

        [XmlIgnore]
        public string fullName {
            get {
                return schemaName + "." + Name;
            }
        }

        public string ToCTName(Int64 CTID) {
            return "tblCT" + Name + "_" + CTID;
        }
        public string ToFullCTName(Int64 CTID) {
            return string.Format("[{0}].[{1}]", schemaName, ToCTName(CTID));
        }
        public override string ToString() {
            return fullName;
        }
    }
}
