using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using TeslaSQL;
using Xunit;

namespace TeslaSQL.Tests {
    public class TestConfig : Config {
        [Fact]
        public void TestValidateNullableIdentifier() {
            //valid database identifiers
            Assert.Equal("test", ValidateNullableIdentifier("test"));
            Assert.Equal("test_1", ValidateNullableIdentifier("test_1"));
            Assert.Equal("_test", ValidateNullableIdentifier("_test"));
            Assert.Equal("test1", ValidateNullableIdentifier("test1"));
            Assert.Equal("te$t moar", ValidateNullableIdentifier("te$t moar"));

            //null and empty are okay too
            Assert.Equal("", ValidateNullableIdentifier(""));
            Assert.Equal(null, ValidateNullableIdentifier(null));

            //invalid identifiers should all throw
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("$test"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("1test"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("#test"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier(" test"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("@test"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier(" "); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("\t"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("\r\n"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateNullableIdentifier("\r\ntest"); });
        }

        [Fact]
        public void TestValidateRequiredHost() {
            //valid hostnames or ips
            Assert.Equal("testhost", ValidateRequiredHost("testhost"));
            Assert.Equal("192.168.1.1", ValidateRequiredHost("192.168.1.1"));
            Assert.Equal("10.25.30.40", ValidateRequiredHost("10.25.30.40"));
            Assert.Equal("testhost01", ValidateRequiredHost("testhost01"));
            Assert.Equal("test\\instance", ValidateRequiredHost("test\\instance"));

            //null and empty are not okay
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost(""); });
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost(null); });

            //invalid hostnames and ips
            //TODO decide whether we care that bogus ips like 256.0.0.0 will get through because they are valid hostnames?
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost(" startswithspace"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost("has a space"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost("has\twhitespace"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost("has\r\nnewline"); });
            Assert.Throws<InvalidDataException>(delegate { ValidateRequiredHost(" "); });

        }

        [Fact]
        public void TestValidateSqlFlavor() {
            Assert.Equal(SqlFlavor.MSSQL, ValidateSqlFlavor("MSSQL"));
            Assert.Equal(SqlFlavor.Netezza, ValidateSqlFlavor("Netezza"));

            Assert.Throws<InvalidDataException>(delegate { ValidateSqlFlavor(""); });
            Assert.Throws<InvalidDataException>(delegate { ValidateSqlFlavor(null); });
            Assert.Throws<InvalidDataException>(delegate { ValidateSqlFlavor("SomethingElseInvalid"); });
        }
    }
}
