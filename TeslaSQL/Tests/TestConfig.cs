using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using TeslaSQL;
using Xunit;

namespace TeslaSQL.Tests {
    public class TestConfig {
        [Fact]
        public void TestValidateNullableIdentifier() {
            //valid database identifiers
            Assert.Equal("test", Config.ValidateNullableIdentifier("test"));
            Assert.Equal("test_1", Config.ValidateNullableIdentifier("test_1"));
            Assert.Equal("_test", Config.ValidateNullableIdentifier("_test"));
            Assert.Equal("test1", Config.ValidateNullableIdentifier("test1"));
            Assert.Equal("te$t moar", Config.ValidateNullableIdentifier("te$t moar"));

            //null and empty are okay too
            Assert.Equal("", Config.ValidateNullableIdentifier(""));
            Assert.Equal(null, Config.ValidateNullableIdentifier(null));

            //invalid identifiers should all throw
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateNullableIdentifier("$test"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateNullableIdentifier("1test"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateNullableIdentifier("#test"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateNullableIdentifier(" test"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateNullableIdentifier("@test"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateNullableIdentifier(" "); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateNullableIdentifier("\t"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateNullableIdentifier("\r\n"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateNullableIdentifier("\r\ntest"); });
        }

        [Fact]
        public void TestValidateRequiredHost() {
            //valid hostnames or ips
            Assert.Equal("testhost", Config.ValidateRequiredHost("testhost"));
            Assert.Equal("192.168.1.1", Config.ValidateRequiredHost("192.168.1.1"));
            Assert.Equal("10.25.30.40", Config.ValidateRequiredHost("10.25.30.40"));
            Assert.Equal("testhost01", Config.ValidateRequiredHost("testhost01"));
            Assert.Equal("test\\instance", Config.ValidateRequiredHost("test\\instance"));

            //null and empty are not okay
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateRequiredHost(""); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateRequiredHost(null); });

            //invalid hostnames and ips
            //TODO decide whether we care that bogus ips like 256.0.0.0 will get through because they are valid hostnames?
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateRequiredHost(" startswithspace"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateRequiredHost("has a space"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateRequiredHost("has\twhitespace"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateRequiredHost("has\r\nnewline"); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateRequiredHost(" "); });

        }

        [Fact]
        public void TestValidateSqlFlavor() {
            Assert.Equal(SqlFlavor.MSSQL, Config.ValidateSqlFlavor("MSSQL"));
            Assert.Equal(SqlFlavor.Netezza, Config.ValidateSqlFlavor("Netezza"));

            Assert.Throws<InvalidDataException>(delegate { Config.ValidateSqlFlavor(""); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateSqlFlavor(null); });
            Assert.Throws<InvalidDataException>(delegate { Config.ValidateSqlFlavor("SomethingElseInvalid"); });
        }
    }
}
