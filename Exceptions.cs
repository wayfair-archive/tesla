#region Using Statements
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Xml;
using System.IO;
#endregion

namespace TeslaSQL
{
    public class DoesNotExistException : System.Exception {

        //must define default constructor explicitly
        public DoesNotExistException() {
        }

        //the constructor we'll actually use, invoke the base constructor
        public DoesNotExistException(string message): base(message) {
        }
    }    
}