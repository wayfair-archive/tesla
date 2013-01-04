#region Using Statements
using System;
using System.Text;
using System.Security.Cryptography;
using System.IO;
#endregion

namespace TeslaSQL {
    public class cTripleDes {
        private readonly byte[] m_key = new byte[] { 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 1, 2, 3, 5 };
        private readonly byte[] m_iv = new byte[] { 1, 2, 3, 5, 7, 11, 13, 17 };

        // define the triple des provider
        private TripleDESCryptoServiceProvider m_des = new TripleDESCryptoServiceProvider();

        //define the string handler
        private UTF8Encoding m_utf8 = new UTF8Encoding();

        public cTripleDes() {

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
}
