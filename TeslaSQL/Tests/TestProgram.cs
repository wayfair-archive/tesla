using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using TeslaSQL;
using Xunit;
using NDesk.Options;
namespace TeslaSQL.Tests {
    public class TestProgram : TeslaSQL.Program {

        [Fact]
        public void TestParseArgs() {
            Params parameters = new Params();
            string[] testargs;
            //test basic parameter
            testargs = new string[2] { "-c", "C:\test.txt" };
            parameters = ParseArgs(testargs);
            Assert.Equal("C:\test.txt", parameters.configFile);
            Assert.Equal(false, parameters.validate);
            Assert.Equal(false, parameters.showHelp);
            Assert.True(String.IsNullOrEmpty(parameters.logLevelOverride));

            //config file and validate param
            testargs = new string[3] { "-c", "C:\test.txt", "--validate" };
            parameters = ParseArgs(testargs);
            Assert.Equal("C:\test.txt", parameters.configFile);
            Assert.Equal(true, parameters.validate);
            Assert.Equal(false, parameters.showHelp);

            //config file and logLevelOverride
            testargs = new string[4] { "-c", "C:\test.txt", "-l", "Debug" };
            parameters = ParseArgs(testargs);
            Assert.Equal("C:\test.txt", parameters.configFile);
            Assert.Equal("Debug", parameters.logLevelOverride);
            Assert.Equal(false, parameters.validate);
            Assert.Equal(false, parameters.showHelp);

            //help param
            testargs = new string[1] { "--help" };
            parameters = ParseArgs(testargs);
            Assert.True(String.IsNullOrEmpty(parameters.configFile));
            Assert.Equal(false, parameters.validate);
            Assert.Equal(true, parameters.showHelp);

            //help param with another param
            testargs = new string[3] { "-c", "C:\test.txt", "-h" };
            parameters = ParseArgs(testargs);
            Assert.Equal(false, parameters.validate);
            Assert.Equal(true, parameters.showHelp);

            //add the more param
            testargs = new string[5] { "-c", "C:\test.txt", "--validate", "--more", "30" };
            parameters = ParseArgs(testargs);
            Assert.Equal("C:\test.txt", parameters.configFile);
            Assert.Equal(true, parameters.validate);
            Assert.Equal(false, parameters.showHelp);
            Assert.Equal(30, parameters.more);

            //add logLevel
            testargs = new string[7] { "-c", "C:\test.txt", "--validate", "-m", "30", "--loglevel", "Warn" };
            parameters = ParseArgs(testargs);
            Assert.Equal("C:\test.txt", parameters.configFile);
            Assert.Equal(true, parameters.validate);
            Assert.Equal(false, parameters.showHelp);
            Assert.Equal(30, parameters.more);
            Assert.Equal("Warn", parameters.logLevelOverride);

            //invalid --more param should throw an OptionException
            testargs = new string[5] { "-c", "C:\test.txt", "--validate", "--more", "notanint" };
            Assert.Throws<OptionException>(delegate { ParseArgs(testargs); });
        }
    }
}
