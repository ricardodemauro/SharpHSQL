/*
 * Library.cs
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
	using System.Data.Internal;
	using System.Data.SQLTypes;


	/**
	 * Class declaration
	 *
	 *
	 * @version 1.0.0.1
	 */
	class Library 
	{
		static string[] sNumeric = 
		{
			"ABS", "Abs", "ACOS", "Acos", "ASIN",
			"Asin", "ATAN", "Atan", "ATAN2",
			"atan2", "CEILING", "Ceil", "COS",
			"Cos", "COT", "org.hsqldb.Library.cot", "DEGREES",
			"java.lang.Math.toDegrees", "EXP", "Exp", "FLOOR",
			"Floor", "LOG", "Log", "LOG10",
			"Log10", "MOD", "org.hsqldb.Library.mod", "PI",
			"org.hsqldb.Library.pi", "POWER", "Pow", "RADIANS",
			"java.lang.Math.toRadians", "RAND", "java.lang.Math.random", "ROUND",
			"org.hsqldb.Library.round", "SIGN", "org.hsqldb.Library.sign", "SIN",
			"Sin", "SQRT", "Sqrt", "TAN",
			"Tan", "TRUNCATE", "org.hsqldb.Library.truncate",
			"BITAND", "org.hsqldb.Library.bitand", "BITOR",
			"org.hsqldb.Library.bitor", "ROUNDMAGIC",
			"org.hsqldb.Library.roundMagic",
		};
		static string[] sstring = 
		{
			"ASCII", "org.hsqldb.Library.ascii", "CHAR",
			"org.hsqldb.Library.character", "CONCAT", "org.hsqldb.Library.concat",
			"DIFFERENCE", "org.hsqldb.Library.difference", "INSERT",
			"org.hsqldb.Library.insert", "LCASE", "org.hsqldb.Library.lcase", "LEFT",
			"org.hsqldb.Library.left", "LENGTH", "org.hsqldb.Library.Length",
			"LOCATE", "org.hsqldb.Library.locate", "LTRIM",
			"org.hsqldb.Library.ltrim", "REPEAT", "org.hsqldb.Library.repeat",
			"REPLACE", "org.hsqldb.Library.replace", "RIGHT",
			"org.hsqldb.Library.right", "RTRIM", "org.hsqldb.Library.rtrim",
			"SOUNDEX", "org.hsqldb.Library.soundex", "SPACE",
			"org.hsqldb.Library.space", "SUBSTRING", "org.hsqldb.Library.Substring",
			"UCASE", "org.hsqldb.Library.ucase", "LOWER", "org.hsqldb.Library.lcase",
			"UPPER", "org.hsqldb.Library.ucase"
		};
		static string[] sTimeDate = 
		{
			"CURDATE", "org.hsqldb.Library.curdate", "CURTIME",
			"org.hsqldb.Library.curtime", "DAYNAME", "org.hsqldb.Library.dayname",
			"DAYOFMONTH", "org.hsqldb.Library.dayofmonth", "DAYOFWEEK",
			"org.hsqldb.Library.dayofweek", "DAYOFYEAR",
			"org.hsqldb.Library.dayofyear", "HOUR", "org.hsqldb.Library.hour",
			"MINUTE", "org.hsqldb.Library.minute", "MONTH",
			"org.hsqldb.Library.month", "MONTHNAME", "org.hsqldb.Library.monthname",
			"NOW", "org.hsqldb.Library.now", "QUARTER", "org.hsqldb.Library.quarter",
			"SECOND", "org.hsqldb.Library.second", "WEEK", "org.hsqldb.Library.week",
			"YEAR", "org.hsqldb.Library.year",
		};
		static string[] sSystem = 
		{
			"DATABASE", "org.hsqldb.Library.database", "USER",
			"org.hsqldb.Library.user", "IDENTITY", "org.hsqldb.Library.identity"
		};

		/**
		 * Method declaration
		 *
		 *
		 * @param h
		 */
		public static void register(Hashtable h) 
		{
			register(h, sNumeric);
			register(h, sstring);
			register(h, sTimeDate);
			register(h, sSystem);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param h
		 * @param s
		 */
		private static void register(Hashtable h, string[] s) 
		{
			for (int i = 0; i < s.Length; i += 2) 
			{
				h.Add(s[i], s[i + 1]);
			}
		}

		static Random rRandom = new Random();

		// NUMERIC

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 *
		 * @return
		 */
		public static double rand(int i) 
		{
			return rRandom.NextDouble();
		}

		// this magic number works for 100000000000000; but not for 0.1 and 0.01
		static double LOG10_FACTOR = 0.43429448190325183;

		/**
		 * Method declaration
		 *
		 *
		 * @param x
		 *
		 * @return
		 */
		public static double log10(double x) 
		{
			return roundMagic(Math.Log(x) * LOG10_FACTOR);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static double roundMagic(double d) 
		{

			// this function rounds numbers in a good way but slow:
			// - special handling for numbers around 0
			// - only numbers <= +/-1000000000000
			// - convert to a string
			// - check the last 4 characters:
			// '000x' becomes '0000'
			// '999x' becomes '999999' (this is rounded automatically)
			if (d < 0.0000000000001 && d > -0.0000000000001) 
			{
				return 0.0;
			}

			if ((d > 1000000000000) || (d < -1000000000000) )
			{
				return d;
			}

			string s = d.ToString();

			int len = s.Length;

			if (len < 16) 
			{
				return d;
			}

			char cx = s.Substring(len - 1,1).ToChar();
			char c1 = s.Substring(len - 2,1).ToChar();
			char c2 = s.Substring(len - 3,1).ToChar();
			char c3 = s.Substring(len - 4,1).ToChar();

			if (c1 == '0' && c2 == '0' && c3 == '0' && cx != '.') 
			{
				s.Remove(len - 1,1);
				s.Insert(len -1,"0");
			} 
			else if (c1 == '9' && c2 == '9' && c3 == '9' && cx != '.') 
			{
				s.Remove(len - 1,1);
				s.Insert(len -1,"9");
				s += "9";
				s += "9";
			}

			return s.ToDouble();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static double cot(double d) 
		{
			return (1 / Math.Tan(d));
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i1
		 * @param i2
		 *
		 * @return
		 */
		public static int mod(int i1, int i2) 
		{
			return i1 % i2;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public static double pi() 
		{
			return Math.PI;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 * @param p
		 *
		 * @return
		 */
		public static double round(double d, int p) 
		{
			double f = Math.Pow(10, p);

			return Math.Round(d * f) / f;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static int sign(double d) 
		{
			return d < 0 ? -1 : (d > 0 ? 1 : 0);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 * @param p
		 *
		 * @return
		 */
		public static double truncate(double d, int p) 
		{
			double f = Math.Pow(10, p);
			double g = d * f;

			return ((d < 0) ? Math.Ceil(g) : Math.Floor(g)) / f;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 * @param j
		 *
		 * @return
		 */
		public static int bitand(int i, int j) 
		{
			return i & j;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 * @param j
		 *
		 * @return
		 */
		public static int bitor(int i, int j) 
		{
			return i | j;
		}

		// STRING

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public static int ascii(string s) 
		{
/*			if (s == null || s.Length == 0) 
			{
				return null;
			}
*/
			return Int32.FromString(s);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param code
		 *
		 * @return
		 */
		public static string character(int code) 
		{
			return "" + (char) code;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s1
		 * @param s2
		 *
		 * @return
		 */
		public static string concat(string s1, string s2) 
		{
			if (s1 == null) 
			{
				if (s2 == null) 
				{
					return null;
				}

				return s2;
			}

			if (s2 == null) 
			{
				return s1;
			}

			return s1 + s2;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s1
		 * @param s2
		 *
		 * @return
		 */
		public static int difference(string s1, string s2) 
		{

			// todo: check if this is the standard algorithm
			if (s1 == null || s2 == null) 
			{
				return 0;
			}

			s1 = soundex(s1);
			s2 = soundex(s2);

			int len1 = s1.Length, len2 = s2.Length;
			int e = 0;

			for (int i = 0; i < 4; i++) 
			{
				if (i >= len1 || i >= len2 || s1.Substring(i,1) != s2.Substring(i,1)) 
				{
					e++;
				}
			}

			return e;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s1
		 * @param start
		 * @param length
		 * @param s2
		 *
		 * @return
		 */
		public static string insert(string s1, int start, int length, string s2) 
		{
			if (s1 == null) 
			{
				return s2;
			}

			if (s2 == null) 
			{
				return s1;
			}

			int len1 = s1.Length;
			int len2 = s2.Length;

			start--;

			if (start < 0 || length <= 0 || len2 == 0 || start > len1) 
			{
				return s1;
			}

			if (start + length > len1) 
			{
				length = len1 - start;
			}

			return s1.Substring(0, start) + s2 + s1.Substring(start + length);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public static string lcase(string s) 
		{
			return s == null ? null : s.ToLower();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 * @param i
		 *
		 * @return
		 */
		public static string left(string s, int i) 
		{
			return s == null ? null
				: s.Substring(0,
				(i < 0 ? 0 : i < s.Length ? i : s.Length));
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public static int length(string s) 
		{
			return (s == null || s.Length < 1) ? 0 : s.Length;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param search
		 * @param s
		 * @param start
		 *
		 * @return
		 */
		public static int locate(string search, string s, int start) 
		{
			if (s == null || search == null) 
			{
				return 0;
			}

			return s.IndexOf(search, start < 0 ? 0 : start) + 1;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public static string ltrim(string s) 
		{
			return s.TrimStart(null);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 * @param i
		 *
		 * @return
		 */
		public static string repeat(string s, int i) 
		{
			if (s == null) 
			{
				return null;
			}

			StringBuilder b = new StringBuilder();

			while (i-- > 0) 
			{
				b.Append(s);
			}

			return b.ToString();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 * @param replace
		 * @param with
		 *
		 * @return
		 */
		public static string replace(string s, string replace, string with) 
		{
			if (s == null || replace == null) 
			{
				return s;
			}

			if (with == null) 
			{
				with = "";
			}

			StringBuilder b = new StringBuilder();
			int	     start = 0;
			int	     lenreplace = replace.Length;

			while (true) 
			{
				int i = s.IndexOf(replace, start);

				if (i == -1) 
				{
					b.Append(s);

					break;
				}

				b.Append(s.Substring(start, i - start));
				b.Append(with);

				start = i + lenreplace;
			}

			return b.ToString();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 * @param i
		 *
		 * @return
		 */
		public static string right(string s, int i) 
		{
			if (s == null) 
			{
				return null;
			}

			i = s.Length - i;

			return s.Substring(i < 0 ? 0 : i < s.Length ? i : s.Length);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public static string rtrim(string s) 
		{
			return s.TrimEnd(null);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public static string soundex(string s) 
		{
			if (s == null) 
			{
				return s;
			}

			s = s.ToUpper();

			int  len = s.Length;
			char[] b = new char[4];
			
			b[0] = s.Substring(0,1).ToChar();

			int j = 1;

			for (int i = 1; i < len && j < 4; i++) 
			{
				char c = s.Substring(i,1).ToChar();

				if ("BFPV".IndexOf(c) != -1) 
				{
					b[j++] = '1';
				} 
				else if ("CGJKQSXZ".IndexOf(c) != -1) 
				{
					b[j++] = '2';
				} 
				else if (c == 'D' || c == 'T') 
				{
					b[j++] = '3';
				} 
				else if (c == 'L') 
				{
					b[j++] = '4';
				} 
				else if (c == 'M' || c == 'N') 
				{
					b[j++] = '5';
				} 
				else if (c == 'R') 
				{
					b[j++] = '6';
				}
			}

			return new string(b, 0, j);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 *
		 * @return
		 */
		public static string space(int i) 
		{
			if (i < 0) 
			{
				return null;
			}

			char[] c = new char[i];

			while (i > 0) 
			{
				c[--i] = ' ';
			}

			return new string(c);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 * @param start
		 * @param length
		 *
		 * @return
		 */
		public static string Substring(string s, int start, int length) 
		{
			if (s == null) 
			{
				return null;
			}

			int len = s.Length;

			start--;
			start = start > len ? len : start;

			if (length == 0) 
			{
				return s.Substring(start);
			} 
			else 
			{
				int l = length;

				return s.Substring(start, start + l > len ? len : l);
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
		public static string ucase(string s) 
		{
			return s.ToUpper();
		}

		// TIME AND DATE

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public static DateTime curdate() 
		{
			return DateTime.Now;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public static DateTime curtime() 
		{
			return DateTime.Now;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static string dayname(SQLDateTime d) 
		{
			return d.ToString();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 * @param part
		 *
		 * @return
		 */
//		private static int getDateTimePart(SQLDateTime d, int part) 
//		{
//			Calendar c = new GregorianCalendar();

//			c.setTime(d);

//			return c.get(part);
//		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static int dayofmonth(SQLDateTime d) 
		{
			return d.Value.Day;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static int dayofweek(SQLDateTime d) 
		{
			return d.Value.DayOfWeek;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static int dayofyear(SQLDateTime d) 
		{
			return d.Value.DayOfYear;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param t
		 *
		 * @return
		 */
		public static int hour(SQLDateTime t) 
		{
			return t.Value.Hour;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param t
		 *
		 * @return
		 */
		public static int minute(SQLDateTime t) 
		{
			return t.Value.Minute;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static int month(SQLDateTime d) 
		{
			return d.Value.Month;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static string monthname(SQLDateTime d) 
		{
			return d.Value.Format("MMMM",null);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public static DateTime now() 
		{
			return DateTime.Now;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static int quarter(SQLDateTime d) 
		{
			return (d.Value.Month / 3) + 1;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static int second(SQLDateTime d) 
		{
			return d.Value.Second;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
//		public static int week(SQLDateTime d) 
//		{
//			return getDateTimePart(d, Calendar.WEEK_OF_YEAR);
//		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 *
		 * @return
		 */
		public static int year(SQLDateTime d) 
		{
			return d.Value.Year;
		}

		// SYSTEM

		/**
		 * Method declaration
		 *
		 *
		 * @param conn
		 *
		 * @return
		 *
		 * @throws Exception
		 */
/*		public static string database(DBConnection conn) 
		{
			Statement stat = conn.createStatement();
			string    s =
				"SELECT Value FROM SYSTEM_CONNECTIONINFO WHERE KEY='DATABASE'";
			ResultSet r = stat.executeQuery(s);

			r.next();

			return r.getstring(1);
		}
*/
		/**
		 * Method declaration
		 *
		 *
		 * @param conn
		 *
		 * @return
		 *
		 * @throws Exception
		 */
/*		public static string user(DBConnection conn) 
		{
			Statement stat = conn.createStatement();
			string    s =
				"SELECT Value FROM SYSTEM_CONNECTIONINFO WHERE KEY='USER'";
			ResultSet r = stat.executeQuery(s);

			r.next();

			return r.getstring(1);
		}
*/
		/**
		 * Method declaration
		 *
		 *
		 * @param conn
		 *
		 * @return
		 *
		 * @throws Exception
		 */
/*		public static int identity(DBConnection conn) 
		{
			DBCommand stat = new DBCommand();
			conn.createStatement();
			string    s =
				"SELECT VALUE FROM SYSTEM_CONNECTIONINFO WHERE KEY='IDENTITY'";
			ResultSet r = stat.executeQuery(s);

			r.next();

			return r.getInt(1);
		}
*/
	} 
}
