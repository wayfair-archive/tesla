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
    //TODO decide if we want to use this or scrap it
    public class ReadOnlyConfigException : System.Exception
    {
        
        //must define default constructor explicitly
        public ReadOnlyConfigException()
        {
        }

        //the constructor we'll actually use
        public ReadOnlyConfigException(string message): base(message)
        {
        }

        
    }

    public class DoesNotExistException : System.Exception {

        //must define default constructor explicitly
        public DoesNotExistException() {
        }

        //the constructor we'll actually use
        public DoesNotExistException(string message): base(message) {
        }


    }    
}