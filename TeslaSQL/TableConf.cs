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
        public string SchemaName { get; set; }

        [XmlElement("stopOnError")]
        public bool StopOnError { get; set; }

        [XmlArray("columnList")]
        [XmlArrayItem("column")]
        public string[] ColumnList { get; set; }

        [XmlElement("columnModifier")]
        public ColumnModifier[] ColumnModifiers { get; set; }

        private Dictionary<string, string> parsedColumnModifiers_m;
        [XmlIgnore]
        public Dictionary<string, string> ParsedColumnModifiers {
            get {
                if (parsedColumnModifiers_m == null) {
                    parsedColumnModifiers_m = Config.ParseColumnModifiers(this.ColumnModifiers);
                }
                return parsedColumnModifiers_m;
            }
        }

        //used only on slaves to keep a historical record of changes
        [XmlElement("recordHistoryTable")]
        public bool RecordHistoryTable { get; set; }

        [XmlIgnore]
        public IList<TColumn> columns = new List<TColumn>();

        [XmlIgnore]
        public string MasterColumnList {
            get {
                return string.Join(",",
                    columns.Select(col => {
                        return col.isPk ? "CT." + col.name : "P." + col.name;
                    }
                ));

            }
        }

        [XmlIgnore]
        public string ModifiedMasterColumnList {
            get {
                return string.Join(",",
                        columns.Select(col => {
                            if (ParsedColumnModifiers.ContainsKey(col.name)) {
                                return ParsedColumnModifiers[col.name];
                            } else {
                                return col.isPk ? "CT." + col.name : "P." + col.name;
                            }
                        }
                    ));
            }
        }


        [XmlIgnore]
        public string SimpleColumnList {
            get {
                return string.Join(",", columns.Select(col => col.name));
            }
        }


        [XmlIgnore]
        public string NetezzaColumnList {
            get {
                return string.Join(",", columns.Select(col => NetezzaDataUtils.MapReservedWord(col.name)));
            }
        }

        [XmlIgnore]
        public string SlaveColumnList {
            get {
                return SimpleColumnList;
            }
        }

        [XmlIgnore]
        public string MergeUpdateList {
            get {
                return string.Join(
                    ",",
                    columns.Where(c => !c.isPk)
                    .Select(c => String.Format("P.{0}=CT.{0}", c.name)));
            }
        }

        [XmlIgnore]
        public string PkList {
            //if stringType is true, it's a primary key column on a case sensitive
            //slave, and the source database is case insensitive, so we wrap the PK
            //in an UPPER function on both sides to simulate a case insensitive comparison
            get {
                return string.Join(
                    " AND ",
                    columns.Where(c => c.isPk)
                    .Select(c => c.dataType.IsStringType() && Config.IgnoreCase ? String.Format("UPPER(P.{0}) = UPPER(CT.{0})", c.name)
                        : String.Format("P.{0} = CT.{0}", c.name)));
            }
        }

        [XmlIgnore]
        public string NotNullPKList {
            get {
                return string.Join(
                    " AND ",
                    columns.Where(c => c.isPk)
                    .Select(c => String.Format("P.{0} IS NOT NULL", c.name)));
            }
        }

        [XmlIgnore]
        public string FullName {
            get {
                return SchemaName + "." + Name;
            }
        }

        public string ToCTName(Int64 CTID) {
            return "tblCT" + Name + "_" + CTID;
        }
        public string ToFullCTName(Int64 CTID) {
            return string.Format("[{0}].[{1}]", SchemaName, ToCTName(CTID));
        }
        public override string ToString() {
            return FullName;
        }
    }
}
