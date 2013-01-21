$Source = @"
using System;
using System.Text;
using System.Security.Cryptography;
using System.IO;

public class cTripleDes {
    private readonly byte[] m_key = new byte[] { 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 1, 2, 3, 5 };
    private readonly byte[] m_iv = new byte[] { 1, 2, 3, 5, 7, 11, 13, 17 };

    // define the triple des provider
    private TripleDESCryptoServiceProvider m_des = new TripleDESCryptoServiceProvider();

    //define the string handler
    private UTF8Encoding m_utf8 = new UTF8Encoding();

    public string Encrypt(string text) {
        byte[] input = Convert.FromBase64String(text);
        byte[] output = Transform(input, m_des.CreateEncryptor(m_key, m_iv));
        return m_utf8.GetString(output);
    }
    public string Decrypt(string text) {
        byte[] input = Convert.FromBase64String(text);
        byte[] output = Transform(input, m_des.CreateDecryptor(m_key, m_iv));
        return m_utf8.GetString(output);
    }
    public byte[] Transform(byte[] input, ICryptoTransform CryptoTransform) {
        //create the necessary streams
        MemoryStream memStream = new MemoryStream();
        CryptoStream cryptStream = new CryptoStream(memStream, CryptoTransform, CryptoStreamMode.Write);
        //transform the bytes as requested
        cryptStream.Write(input, 0, input.Length);
        cryptStream.FlushFinalBlock();
        //Read the memory stream and convert it back into byte array
        memStream.Position = 0;
        byte[] result = new byte[(int)(memStream.Length)];
        memStream.Read(result, 0, (int)result.Length);
        //close and release the streams
        memStream.Close();
        cryptStream.Close();
        //hand back the encrypted buffer
        return result;
    }
}
"@

Add-Type -TypeDefinition $Source -Language CSharp

Function Invoke-NetezzaQuery {
[CmdletBinding()]
    param(
    [Parameter(Position=0, Mandatory=$true)] [string]$ServerInstance,
    [Parameter(Position=1, Mandatory=$true)] [string]$Database,
    [Parameter(Position=2, Mandatory=$true)] [string]$User,
    [Parameter(Position=3, Mandatory=$true)] [string]$Password,
    [Parameter(Position=4, Mandatory=$true)] [string]$Query
    )
    $ctripledes = new-object cTripleDes
    $Password = $ctripledes.Decrypt($Password)
    $connstr = "Data Source=$ServerInstance; Initial Catalog=$Database; User ID=$User;Password=$Password;Provider=NZOLEDB;";
    $conn = new-object System.Data.OleDb.OleDbConnection($connstr)
    $conn.Open();
    $cmd = new-object System.Data.OleDb.OleDbCOmmand("$query")
    $cmd.Connection = $conn;
    $ds = new-object System.Data.DataSet;
    $da = new-object System.Data.OleDb.OleDbDataAdapter($cmd);
    $da.Fill($ds) | out-null;
    $conn.Close();
    return $ds.Tables[0]
}

#######################
<#
.SYNOPSIS
Runs a T-SQL script.
.DESCRIPTION
Runs a T-SQL script. Invoke-Sqlcmd2 only returns message output, such as the output of PRINT statements when -verbose parameter is specified
.INPUTS
None
    You cannot pipe objects to Invoke-Sqlcmd2
.OUTPUTS
   System.Data.DataTable
.EXAMPLE
Invoke-Sqlcmd2 -ServerInstance "MyComputer\MyInstance" -Query "SELECT login_time AS 'StartTime' FROM sysprocesses WHERE spid = 1"
This example connects to a named instance of the Database Engine on a computer and runs a basic T-SQL query.
StartTime
-----------
2010-08-12 21:21:03.593
.EXAMPLE
Invoke-Sqlcmd2 -ServerInstance "MyComputer\MyInstance" -InputFile "C:\MyFolder\tsqlscript.sql" | Out-File -filePath "C:\MyFolder\tsqlscript.rpt"
This example reads a file containing T-SQL statements, runs the file, and writes the output to another file.
.EXAMPLE
Invoke-Sqlcmd2  -ServerInstance "MyComputer\MyInstance" -Query "PRINT 'hello world'" -Verbose
This example uses the PowerShell -Verbose parameter to return the message output of the PRINT command.
VERBOSE: hello world
.NOTES
Version History
v1.0   - Chad Miller - Initial release
v1.1   - Chad Miller - Fixed Issue with connection closing
v1.2   - Chad Miller - Added inputfile, SQL auth support, connectiontimeout and output message handling. Updated help documentation
v1.3   - Chad Miller - Added As parameter to control DataSet, DataTable or array of DataRow Output type
#>
function Invoke-Sqlcmd2
{
    [CmdletBinding()]
    param(
    [Parameter(Position=0, Mandatory=$true)] [string]$ServerInstance,
    [Parameter(Position=1, Mandatory=$false)] [string]$Database,
    [Parameter(Position=2, Mandatory=$false)] [string]$Query,
    [Parameter(Position=3, Mandatory=$false)] [string]$Username,
    [Parameter(Position=4, Mandatory=$false)] [string]$Password,
    [Parameter(Position=5, Mandatory=$false)] [Int32]$QueryTimeout=0,
    [Parameter(Position=6, Mandatory=$false)] [Int32]$ConnectionTimeout=20,
    [Parameter(Position=7, Mandatory=$false)] [ValidateScript({test-path $_})] [string]$InputFile,
    [Parameter(Position=8, Mandatory=$false)] [ValidateSet("DataSet", "DataTable", "DataRow")] [string]$As="DataRow"
    )

    if ($InputFile)
    {
        $filePath = $(resolve-path $InputFile).path
        $Query =  [System.IO.File]::ReadAllText("$filePath")
    }

    $conn=new-object System.Data.SqlClient.SQLConnection
     
    if ($Username)
    { $ConnectionString = "Server={0};Database={1};User ID={2};Password={3};Trusted_Connection=False;Connect Timeout={4}" -f $ServerInstance,$Database,$Username,$Password,$ConnectionTimeout }
    else
    { $ConnectionString = "Server={0};Database={1};Integrated Security=True;Connect Timeout={2}" -f $ServerInstance,$Database,$ConnectionTimeout }

    $conn.ConnectionString=$ConnectionString
    
    #Following EventHandler is used for PRINT and RAISERROR T-SQL statements. Executed when -Verbose parameter specified by caller
    if ($PSBoundParameters.Verbose)
    {
        $conn.FireInfoMessageEventOnUserErrors=$true
        $handler = [System.Data.SqlClient.SqlInfoMessageEventHandler] {Write-Verbose "$($_)"}
        $conn.add_InfoMessage($handler)
    }
    
    $conn.Open()
    $cmd=new-object system.Data.SqlClient.SqlCommand($Query,$conn)
    $cmd.CommandTimeout=$QueryTimeout
    $ds=New-Object system.Data.DataSet
    $da=New-Object system.Data.SqlClient.SqlDataAdapter($cmd)
    [void]$da.fill($ds)
    $conn.Close()
    switch ($As)
    {
        'DataSet'   { Write-Output ($ds) }
        'DataTable' { Write-Output ($ds.Tables) }
        'DataRow'   { Write-Output ($ds.Tables[0]) }
    }

} #Invoke-Sqlcmd2


$source = @"
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using TeslaSQL;

namespace TeslaSQL {

    /// <summary>
    /// Class repsenting a SQL data type including length/precision
    /// </summary>
    public class DataType {
        private static Dictionary<string, IList<string>> dataMappings = new Dictionary<string, IList<string>>();

        public string baseType { get; set; }
        public int? characterMaximumLength { get; set; }
        public int? numericPrecision { get; set; }
        public int? numericScale { get; set; }

        public static void LoadDataMappingsFromFile(string filePath) {
            string s;
            using (var reader = new StreamReader(filePath)) {
                s = reader.ReadToEnd();
            }
            LoadDataMappings(s);
        }

        public static void LoadDataMappings(string mappings) {
            using (var reader = new StringReader(mappings)) {
                var flavors = new List<string>();
                string[] headers = reader.ReadLine().Split(new char[] { '\t' });
                foreach (var flavor in headers) {
                    string fl = flavor;
                    flavors.Add(fl);
                    dataMappings[fl] = new List<string>();
                }
                while (reader.Peek() > 0) {
                    string[] map = reader.ReadLine().Split(new char[] { '\t' });
                    for (int i = 0; i < map.Length; i++) {
                        dataMappings[flavors[i]].Add(map[i].ToLower());
                    }
                }
            }
        }
        public static string MapDataType(string source, string dest, string datatype) {
            datatype = datatype.ToLower();
            int idx = dataMappings[source].IndexOf(datatype);
            if (idx == -1) {
                return datatype;
            }
            return dataMappings[dest][idx];
        }


        public DataType(string baseType, int? characterMaximumLength, int? numericPrecision, int? numericScale) {
            this.baseType = baseType;
            this.characterMaximumLength = characterMaximumLength;
            this.numericPrecision = numericPrecision;
            this.numericScale = numericScale;
        }

        /// <summary>
        /// Compare two DataType objects that may both be null. Used for unit tests
        /// </summary>
        /// <param name="expected">Expected data type</param>
        /// <param name="actual">Actual data type</param>
        /// <returns>True if the objects are equal or both null</returns>
        public static bool Equals(DataType expected, DataType actual) {
            if (expected == null && actual == null) {
                return true;
            }
            if (expected == null || actual == null) {
                return false;
            }
            if (expected.baseType != actual.baseType
                || expected.characterMaximumLength != actual.characterMaximumLength
                || expected.numericPrecision != actual.numericPrecision
                || expected.numericScale != actual.numericScale
                ) {
                return false;
            }
            return true;
        }

        /// <summary>
        /// Convert data type to string
        /// </summary>
        /// <returns>String expression representing the data type</returns>
        public override string ToString() {
            var typesUsingMaxLen = new string[4] { "varchar", "nvarchar", "char", "nchar" };
            var typesUsingScale = new string[2] { "numeric", "decimal" };

            string suffix = "";
            if (Array.IndexOf(typesUsingMaxLen, baseType) >= 0 && characterMaximumLength != null) {
                //(n)varchar(max) types stored with a maxlen of -1, so change that to max
                suffix = "(" + (characterMaximumLength == -1 ? "max" : Convert.ToString(characterMaximumLength)) + ")";
            } else if (Array.IndexOf(typesUsingScale, baseType) >= 0 && numericPrecision != null && numericScale != null) {
                suffix = "(" + numericPrecision + ", " + numericScale + ")";
            }

            return baseType + suffix;
        }
    }
}
"@

Add-Type -TypeDefinition $Source -Language CSharpVersion3