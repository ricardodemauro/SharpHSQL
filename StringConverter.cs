/*
 * stringConverter.cs
 *
 * Copyright (c) 2001, The HSQL Development Group
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of the HSQL Development Group nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This package is based on HypersonicSQL, originally developed by Thomas Mueller.
 *
 * C# port by Mark Tutt
 *
 */
namespace SharpHSQL
{
	using System;
	using System.Collections;
	using System.Text;
	using System.IO;

	/**
	 * Class declaration
	 *
	 *
	 * @version 1.0.0.1
	 */
	class stringConverter 
	{
		private static char[]   HEXCHAR = 
		{
			'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd',
			'e', 'f'
		};
		private static string HEXINDEX = "0123456789abcdef          ABCDEF";

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public static byte[] hexToByte(string s) 
		{
			int  l = s.Length / 2;
			byte[] data = new byte[l];
			int  j = 0;

			for (int i = 0; i < l; i++) 
			{
				char c = s.Substring(j++,1).ToChar();
				int  n, b;

				n = HEXINDEX.IndexOf(c);
				b = (n & 0xf) << 4;
				c = s.Substring(j++,1).ToChar();
				n = HEXINDEX.IndexOf(c);
				b += (n & 0xf);
				data[i] = (byte) b;
			}

			return data;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param b
		 *
		 * @return
		 */
		public static string byteToHex(byte[] b) 
		{
			int	     len = b.Length;
			StringBuilder s = new StringBuilder();

			for (int i = 0; i < len; i++) 
			{
				int c = ((int) b[i]) & 0xff;

				s.Append(HEXCHAR[c >> 4 & 0xf]);
				s.Append(HEXCHAR[c & 0xf]);
			}

			return s.ToString();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		static string unicodeToHexstring(string s) 
		{
			MemoryStream bout = new MemoryStream();
			BinaryWriter dout = new BinaryWriter(bout);

			try 
			{
				dout.Write(s);
				dout.Close();
				bout.Close();
			} 
			catch (Exception e) 
			{
				return null;
			}

			return byteToHex(bout.ToArray());
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public static string hexstringToUnicode(string s) 
		{
			byte[]		     b = hexToByte(s);
			MemoryStream bin = new MemoryStream(b);
			BinaryReader din = new BinaryReader(bin);

			try 
			{
				return din.ReadString();
			} 
			catch (Exception e) 
			{
				return null;
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public static string unicodeToAscii(string s) 
		{
			if (s == null || s.Equals("")) 
			{
				return s;
			}

			int	     len = s.Length;
			StringBuilder b = new StringBuilder();

			for (int i = 0; i < len; i++) 
			{
				char c = s.Substring(i,1).ToChar();

				if (c == '\\') 
				{
					if (i < len - 1 && s.Substring(i + 1,1).ToChar() == 'u') 
					{
						b.Append(c);    // encode the \ as unicode, so 'u' is ignored
						b.Append("u005c");    // splited so the source code is not changed...
					} 
					else 
					{
						b.Append(c);
					}
				} 
				else if ((c >= 0x0020) && (c <= 0x007f)) 
				{
					b.Append(c);    // this is 99%
				} 
				else 
				{
					b.Append("\\u");
					b.Append(HEXCHAR[(c >> 12) & 0xf]);
					b.Append(HEXCHAR[(c >> 8) & 0xf]);
					b.Append(HEXCHAR[(c >> 4) & 0xf]);
					b.Append(HEXCHAR[c & 0xf]);
				}
			}

			return b.ToString();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public static string asciiToUnicode(string s) 
		{
			if (s == null || s.IndexOf("\\u") == -1) 
			{
				return s;
			}

			int  len = s.Length;
			char[] b = new char[len];
			int  j = 0;

			for (int i = 0; i < len; i++) 
			{
				char c = s.Substring(i,1).ToChar();

				if (c != '\\' || i == len - 1) 
				{
					b[j++] = c;
				} 
				else 
				{
					c = s.Substring(++i,1).ToChar();

					if (c != 'u' || i == len - 1) 
					{
						b[j++] = '\\';
						b[j++] = c;
					} 
					else 
					{
						int k = (HEXINDEX.IndexOf(s.Substring(++i,1).ToChar()) & 0xf) << 12;

						k += (HEXINDEX.IndexOf(s.Substring(++i,1).ToChar()) & 0xf) << 8;
						k += (HEXINDEX.IndexOf(s.Substring(++i,1).ToChar()) & 0xf) << 4;
						k += (HEXINDEX.IndexOf(s.Substring(++i,1).ToChar()) & 0xf);
						b[j++] = (char) k;
					}
				}
			}

			return new string(b, 0, j);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param x
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static string InputStreamTostring(BinaryReader x)
		{
			StringWriter      write = new StringWriter();
			int		  blocksize = 8 * 1024;    // todo: is this a good value?
			char[]		  buffer = new char[blocksize];

			try 
			{
				while (true) 
				{
					int l = x.Read(buffer, 0, blocksize);

					if (l == -1) 
					{
						break;
					}

					write.Write(buffer, 0, l);
				}

				write.Close();
				x.Close();
			} 
			catch (IOException e) 
			{
				throw Trace.error(Trace.INPUTSTREAM_ERROR, e.Message);
			}

			return write.ToString();
		}

	}
}
