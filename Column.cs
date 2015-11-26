/*
 * Column.cs
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
	using System.IO;
	using System.Text;


	/**
	 * Class declaration
	 *
	 *
	 * @version 1.0.0.1
	 */
	class Column 
	{
		private static Hashtable hTypes;
		public const int	     BIT = -7;
		public const int	     TINYINT = -6;
		public const int	     BIGINT = -5;
		public const int	     LONGVARBINARY = -4;
		public const int	     VARBINARY = -3;
		public const int	     BINARY = -2;
		public const int	     LONGVARCHAR = -1;
		public const int	     CHAR = 1;
		public const int	     NUMERIC = 2;
		public const int	     DECIMAL = 3;
		public const int	     INTEGER = 4;
		public const int	     SMALLINT = 5;
		public const int	     FLOAT = 6;
		public const int	     REAL = 7;
		public const int	     DOUBLE = 8;
		public const int	     VARCHAR = 12;
		public const int	     DATE = 91;
		public const int	     TIME = 92;
		public const int	     TIMESTAMP = 93;
		public const int	     OTHER = 1111;
		public const int	     NULL = 0;
		public const int	     VARCHAR_IGNORECASE = 100;	    // this is the only non-standard type

		// NULL and VARCHAR_IGNORECASE is not part of TYPES
		public static int[]	     TYPES = 
		{
			BIT, TINYINT, BIGINT, LONGVARBINARY, VARBINARY, BINARY, LONGVARCHAR,
			CHAR, NUMERIC, DECIMAL, INTEGER, SMALLINT, FLOAT, REAL, DOUBLE,
			VARCHAR, DATE, TIME, TIMESTAMP, OTHER
		};
		public string		     sName;
		public int			     iType;
		private bool			 bNullable;
		private bool			 bIdentity;
    
		/**
		 * Method declaration
		 *
		 *
		 * @param type
		 * @param name
		 * @param n2
		 * @param n3
		 */
		private static void addTypes(int type, string name, string n2, string n3) 
		{
			addType(type, name);
			addType(type, n2);
			addType(type, n3);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param type
		 * @param name
		 */
		private static void addType(int type, string name) 
		{
			if (name != null) 
			{
				hTypes.Add(name, type);
			}
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param name
		 * @param nullable
		 * @param type
		 * @param identity
		 */
		public Column(string name, bool nullable, int type, bool identity) 
		{
	
			sName = name;
			bNullable = nullable;
			iType = type;
			bIdentity = identity;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isIdentity() 
		{
			return bIdentity;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static int getTypeNr(string type) 
		{
			if (hTypes == null)
			{
				hTypes = new Hashtable();

				addTypes(INTEGER, "INTEGER", "int", "java.lang.int");
				addType(INTEGER, "INT");
				addTypes(DOUBLE, "DOUBLE", "double", "java.lang.Double");
				addType(FLOAT, "FLOAT");		       // this is a Double
				addTypes(VARCHAR, "VARCHAR", "java.lang.string", null);
				addTypes(CHAR, "CHAR", "CHARACTER", null);
				addType(LONGVARCHAR, "LONGVARCHAR");

				// for ignorecase data types, the 'original' type name is lost
				addType(VARCHAR_IGNORECASE, "VARCHAR_IGNORECASE");
				addTypes(DATE, "DATE", "java.sql.Date", null);
				addTypes(TIME, "TIME", "java.sql.Time", null);

				// DATETIME is for compatibility with MS SQL 7
				addTypes(TIMESTAMP, "TIMESTAMP", "java.sql.Timestamp", "DATETIME");
				addTypes(DECIMAL, "DECIMAL", "java.math.BigDecimal", null);
				addType(NUMERIC, "NUMERIC");
				addTypes(BIT, "BIT", "java.lang.Boolean", "bool");
				addTypes(TINYINT, "TINYINT", "java.lang.Short", "short");
				addType(SMALLINT, "SMALLINT");
				addTypes(BIGINT, "BIGINT", "java.lang.Long", "long");
				addTypes(REAL, "REAL", "java.lang.Float", "float");
				addTypes(BINARY, "BINARY", "byte[]", null);    // maybe better "[B"
				addType(VARBINARY, "VARBINARY");
				addType(LONGVARBINARY, "LONGVARBINARY");
				addTypes(OTHER, "OTHER", "java.lang.object", "OBJECT");
			}

			if (hTypes.ContainsKey(type))
			{
				int i = (int)hTypes[type];
				return i;
			}
			else
				throw Trace.error(Trace.WRONG_DATA_TYPE, type);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static string getType(int type) 
		{
			switch (type) 
			{

				case NULL:
					return "NULL";

				case INTEGER:
					return "INTEGER";

				case DOUBLE:
					return "DOUBLE";

				case VARCHAR_IGNORECASE:
					return "VARCHAR_IGNORECASE";

				case VARCHAR:
					return "VARCHAR";

				case CHAR:
					return "CHAR";

				case LONGVARCHAR:
					return "LONGVARCHAR";

				case DATE:
					return "DATE";

				case TIME:
					return "TIME";

				case DECIMAL:
					return "DECIMAL";

				case BIT:
					return "BIT";

				case TINYINT:
					return "TINYINT";

				case SMALLINT:
					return "SMALLINT";

				case BIGINT:
					return "BIGINT";

				case REAL:
					return "REAL";

				case FLOAT:
					return "FLOAT";

				case NUMERIC:
					return "NUMERIC";

				case TIMESTAMP:
					return "TIMESTAMP";

				case BINARY:
					return "BINARY";

				case VARBINARY:
					return "VARBINARY";

				case LONGVARBINARY:
					return "LONGVARBINARY";

				case OTHER:
					return "OBJECT";

				default:
					throw Trace.error(Trace.WRONG_DATA_TYPE, type);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isNullable() 
		{
			return bNullable;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param b
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object add(object a, object b, int type) 
		{
			if (a == null || b == null) 
			{
				return null;
			}

			switch (type) 
			{

				case NULL:
					return null;

				case INTEGER:
					int ai = (int)a;
					int bi = (int)b;

					return (ai + bi);

				case FLOAT:
				case REAL:
					float ar = (float) a;
					float br = (float) b;

					return (ar + br);

				case DOUBLE:
					double ad = (double) a;
					double bd = (double) b;

					return (ad + bd);

				case VARCHAR:
				case CHAR:
				case LONGVARCHAR:
				case VARCHAR_IGNORECASE:
					return (string) a + (string) b;

				case NUMERIC:
				case DECIMAL:
					decimal abd = (decimal) a;
					decimal bbd = (decimal) b;

					return (abd + bbd);

				case TINYINT:
					byte at = (byte) a;
					byte bt = (byte) b;

					return (at + bt);

				case SMALLINT:
					short shorta = (short) a;
					short shortb = (short) b;

					return (shorta + shortb);

				case BIGINT:
					long longa = (long) a;
					long longb = (long) b;

					return (longa + longb);

				default:
					throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED, type);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param b
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object concat(object a, object b) 
		{
			if (a == null) 
			{
				return b;
			} 
			else if (b == null) 
			{
				return a;
			}

			return convertobject(a) + convertobject(b);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object negate(object a, int type) 
		{
			if (a == null) 
			{
				return null;
			}

			switch (type) 
			{

				case NULL:
					return null;

				case INTEGER:
					return (-(int) a);

				case FLOAT:
				case REAL:
					return (-(float) a);

				case DOUBLE:
					return (-(double) a);

				case NUMERIC:
				case DECIMAL:
					return (-(decimal)a);

				case TINYINT:
					return (-(byte)a);

				case SMALLINT:
					return (-(short)a);

				case BIGINT:
					return (-(long) a);

				default:
					throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED, type);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param b
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object multiply(object a, object b, int type) 
		{
			if (a == null || b == null) 
			{
				return null;
			}

			switch (type) 
			{

				case NULL:
					return null;

				case INTEGER:
					int ai = (int) a;
					int bi = (int) b;

					return (ai * bi);

				case FLOAT:
				case REAL:
					float floata = (float) a;
					float floatb = (float) b;

					return (floata * floatb);

				case DOUBLE:
					double ad = (double) a;
					double bd = (double) b;

					return (ad * bd);

				case NUMERIC:
				case DECIMAL:
					decimal abd = (decimal) a;
					decimal bbd = (decimal) b;

					return (abd * bbd);

				case TINYINT:
					byte ba = (byte) a;
					byte bb = (byte) b;

					return (ba * bb);

				case SMALLINT:
					short shorta = (short) a;
					short shortb = (short) b;

					return (shorta * shortb);

				case BIGINT:
					long longa = (long) a;
					long longb = (long) b;

					return (longa * longb);

				default:
					throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED, type);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param b
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object divide(object a, object b, int type) 
		{
			if (a == null || b == null) 
			{
				return null;
			}

			switch (type) 
			{

				case NULL:
					return null;

				case INTEGER:
					if ((int)b == 0)
						return null;
					return ((int)a / (int)b);

				case FLOAT:
				case REAL:
					if ((float) b == 0)
						return null;
					return ((float)a / (float)b);


				case DOUBLE:
					if ((double) b == 0)
						return null;
					return ((double)a / (double)b);

				case NUMERIC:
				case DECIMAL:
					if ((decimal) b == 0)
						return null;
					return ((decimal)a / (decimal)b);

				case TINYINT:
					if ((byte) b == 0)
						return null;
					return ((byte)a / (byte)b);

				case SMALLINT:
					if ((short) b == 0)
						return null;
					return ((short)a / (short)b);

				case BIGINT:
					if ((long) b == 0)
						return null;
					return ((long)a / (long)b);

				default:
					throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED, type);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param b
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object subtract(object a, object b, int type) 
		{
			if (a == null || b == null) 
			{
				return null;
			}

			switch (type) 
			{

				case NULL:
					return null;

				case INTEGER:
					return ((int)a - (int)b);

				case FLOAT:
				case REAL:
					return ((float)a - (float)b);

				case DOUBLE:
					return ((double)a - (double)b);

				case NUMERIC:
				case DECIMAL:
					return ((decimal)a - (decimal)b);

				case TINYINT:
					return ((byte)a - (byte)b);

				case SMALLINT:
					return ((short)a - (short)b);

				case BIGINT:
					return ((long)a - (long)b);

				default:
					throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED, type);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param b
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object sum(object a, object b, int type) 
		{
			if (a == null) 
			{
				return b;
			}

			if (b == null) 
			{
				return a;
			}

			switch (type) 
			{

				case NULL:
					return null;

				case INTEGER:
					return (((int) a) + ((int) b));

				case FLOAT:
				case REAL:
					return (((float) a)	+ ((float) b));

				case DOUBLE:
					return (((double) a) + ((double) b));

				case NUMERIC:
				case DECIMAL:
					return (((decimal) a) + ((decimal) b));

				case TINYINT:
					return  (((byte) a) + ((byte) b));

				case SMALLINT:
					return  (((short) a) + ((short) b));

				case BIGINT:
					return (((long) a) + ((long) b));

				default:
					Trace.error(Trace.SUM_OF_NON_NUMERIC);
			}

			return null;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param type
		 * @param count
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object avg(object a, int type, int count) 
		{
			if (a == null || count == 0) 
			{
				return null;
			}

			switch (type) 
			{

				case NULL:
					return null;

				case INTEGER:
					return ((int)a / count);

				case FLOAT:
				case REAL:
					return ((float) a / count);

				case DOUBLE:
					return ((double) a / count);

				case NUMERIC:
				case DECIMAL:
					return ((decimal) a / (decimal)count);

				case TINYINT:
					return ((byte) a / count);

				case SMALLINT:
					return ((short) a / count);

				case BIGINT:
					return ((long) a / count);

				default:
					Trace.error(Trace.SUM_OF_NON_NUMERIC);
			}

			return null;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param b
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object min(object a, object b, int type) 
		{
			if (a == null) 
			{
				return b;
			}

			if (b == null) 
			{
				return a;
			}

			if (compare(a, b, type) < 0) 
			{
				return a;
			}

			return b;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param b
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object max(object a, object b, int type) 
		{
			if (a == null) 
			{
				return b;
			}

			if (b == null) 
			{
				return a;
			}

			if (compare(a, b, type) > 0) 
			{
				return a;
			}

			return b;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 * @param b
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static int compare(object a, object b, int type) 
		{
			int i = 0;

			// null handling: null==null and smaller any value
			// todo: implement standard SQL null handling
			// it is also used for grouping ('null' is one group)
			if (a == null) 
			{
				if (b == null) 
				{
					return 0;
				}

				return -1;
			}

			if (b == null) 
			{
				return 1;
			}

			switch (type) 
			{

				case NULL:
					return 0;

				case INTEGER:
					return ((int)a > (int)b) ? 1 : ((int)b > (int)a ? -1 : 0);

				case FLOAT:
				case REAL:
					return ((float)a > (float)b) ? 1 : ((float)b > (float)a ? -1 : 0);

				case DOUBLE:
					return ((double)a > (double)b) ? 1 : ((double)b > (double)a ? -1 : 0);

				case VARCHAR:

				case CHAR:

				case LONGVARCHAR:
					i = ((string) a).CompareTo((string) b);
					break;

				case VARCHAR_IGNORECASE:
					i = ((string) a).ToUpper().CompareTo(((string) b).ToUpper());

					break;

				case DATE:
				case TIME:
				case TIMESTAMP:
					if ((DateTime)a > (DateTime) b)
					{
						return 1;
					} 
					if ((DateTime)a >(DateTime) b)
					{
						return -1;
					} 
					else 
					{
						return 0;
					}

				case NUMERIC:
				case DECIMAL:
					return ((decimal)a > (decimal)b) ? 1 : ((decimal)b > (decimal)a ? -1 : 0);

				case BIT:
					return (((bool)a == (bool)b) ? 0 : 1);

				case TINYINT:
					return ((byte)a > (byte)b) ? 1 : ((byte)b > (byte)a ? -1 : 0);

				case SMALLINT:
					return ((short)a > (short)b) ? 1 : ((short)b > (short)a ? -1 : 0);

				case BIGINT:
					return ((long)a > (long)b) ? 1 : ((long)b > (long)a ? -1 : 0);

				case BINARY:

				case VARBINARY:

				case LONGVARBINARY:

				case OTHER:
					i = ((ByteArray) a).compareTo((ByteArray) b);

					break;

				default:
					throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED, type);
			}

			return (i > 0) ? 1 : (i < 0 ? -1 : 0);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object convertstring(string s, int type) 
		{
			if (s == null) 
			{
				return null;
			}

			switch (type) 
			{

				case NULL:
					return null;

				case INTEGER:
					return s.ToInt32();

				case FLOAT:
				case REAL:
					return s.ToSingle();

				case DOUBLE:
					return s.ToDouble();

				case VARCHAR_IGNORECASE:

				case VARCHAR:

				case CHAR:

				case LONGVARCHAR:
					return s;

				case DATE:
				case TIME:
				case TIMESTAMP:
					return s.ToDateTime();

				case NUMERIC:

				case DECIMAL:
					return s.ToDecimal();

				case BIT:
					return s.ToBoolean();

				case TINYINT:
					return s.ToByte();

				case SMALLINT:
					return s.ToInt16();

				case BIGINT:
					return s.ToInt64();

				case BINARY:

				case VARBINARY:

				case LONGVARBINARY:

				case OTHER:
					return new ByteArray(s);

				default:
					throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED, type);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param o
		 *
		 * @return
		 */
		public static string convertobject(object o) 
		{
			if (o == null) 
			{
				return null;
			}

			return o.ToString();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param o
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static object convertobject(object o, int type) 
		{
			if (o == null) 
			{
				return null;
			}
			return convertstring(o.ToString(), type);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param o
		 * @param type
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public static string createstring(object o, int type) 
		{
			if (o == null) 
			{
				return "NULL";
			}

			switch (type) 
			{

				case NULL:
					return "NULL";

				case BINARY:

				case VARBINARY:

				case LONGVARBINARY:

				case DATE:

				case TIME:

				case TIMESTAMP:

				case OTHER:
					return "'" + o.ToString() + "'";

				case VARCHAR_IGNORECASE:

				case VARCHAR:

				case CHAR:

				case LONGVARCHAR:
					return createstring((string) o);

				default:
					return o.ToString();
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
		public static string createstring(string s) 
		{
			StringBuilder b = new StringBuilder();
			b.Append("\'");

			if (s != null) 
			{
				for (int i = 0, len = s.Length; i < len; i++) 
				{
					char c = s.Substring(i,1).ToChar();

					if (c == '\'') 
					{
						b.Append(c.ToString());
					}

					b.Append(c.ToString());
				}
			}

			return b.Append('\'').ToString();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param in
		 * @param l
		 *
		 * @return
		 *
		 * @throws IOException
		 * @throws Exception
		 */
		public static object[] readData(BinaryReader din, int l) 
		{
			object[] data = new object[l];

			for (int i = 0; i < l; i++) 
			{
				int    type = din.ReadInt32();
				object o = null;
				switch (type) 
				{

					case NULL:
						o = null;

						break;

					case FLOAT:
					case REAL:
						o = din.ReadSingle();

						break;

					case DOUBLE:
						o = din.ReadDouble();

						break;

					case VARCHAR_IGNORECASE:

					case VARCHAR:

					case CHAR:

					case LONGVARCHAR:
						o = din.ReadString();

						break;

					case DATE:
					case TIME:
					case TIMESTAMP:
						o = DateTime.Parse(din.ReadString());

						break;

					case NUMERIC:

					case DECIMAL:
						o = Decimal.Parse(din.ReadString());

						break;

					case BIT:
						o = din.ReadBoolean();

						break;

					case TINYINT:
						o = din.ReadByte();

						break;

					case SMALLINT:
						o = din.ReadInt16();

						break;

					case INTEGER:
						o = din.ReadInt32();

						break;

					case BIGINT:
						o = din.ReadInt64();

						break;

					case BINARY:

					case VARBINARY:

					case LONGVARBINARY:

					case OTHER:
						int len = din.ReadInt32();
						byte[] b = new byte[len];
						din.ReadBytes(len);
						o = new ByteArray(b);

						break;

					default:
						throw Trace.error(Trace.FUNCTION_NOT_SUPPORTED, type);
				}

				data[i] = o;
			}

			return data;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param out
		 * @param data
		 * @param t
		 *
		 * @throws IOException
		 */
		public static void writeData(BinaryWriter dout, object[] data, Table t) 
		{
			int len = t.getInternalColumnCount();
			int[] type = new int[len];

			for (int i = 0; i < len; i++) 
			{
				type[i] = t.getType(i);
			}

			writeData(dout, len, type, data);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param out
		 * @param l
		 * @param type
		 * @param data
		 *
		 * @throws IOException
		 */
		public static void writeData(BinaryWriter dout, int l, int[] type, object[] data) 
		{
			for (int i = 0; i < l; i++) 
			{
				object o = data[i];

				if (o == null) 
				{
					dout.Write(NULL);
				} 
				else 
				{
					int t = type[i];

					dout.Write(t);

					switch (t) 
					{

						case NULL:
							o = null;
							break;

						case FLOAT:
						case REAL:
							dout.Write((float)o);
							break;

						case DOUBLE:
							dout.Write((double)o);
							break;

						case BIT:
							dout.Write((bool)o);
							break;

						case TINYINT:
							dout.Write((byte)o);
							break;

						case SMALLINT:
							dout.Write((short)o);
							break;
	
						case INTEGER:
							dout.Write((int)o);
							break;

						case BIGINT:
							dout.Write((long)o);
							break;

						case BINARY:
						case VARBINARY:
						case LONGVARBINARY:
						case OTHER:
							byte[] b = ((ByteArray)o).byteValue();
							dout.Write(b.Length);
							dout.Write(b);
							break;

						default:
							dout.WriteString(o.ToString());
							break;
					}
				}
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param data
		 * @param t
		 *
		 * @return
		 */
		public static int getSize(object[] data, Table t) 
		{
			int l = data.Length;
			int[] type = new int[l];

			for (int i = 0; i < l; i++) 
			{
				type[i] = t.getType(i);
			}

			return getSize(data, l, type);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param data
		 * @param l
		 * @param type
		 *
		 * @return
		 */
		unsafe private static int getSize(object[] data, int l, int[] type) 
		{
			int s = 0;

			for (int i = 0; i < l; i++) 
			{
				object o = data[i];

				s += 4;    // type

				if (o != null) 
				{
					switch (type[i]) 
					{

						case FLOAT:
						case REAL:
							s += sizeof(float);
							break;

						case DOUBLE:
							s += sizeof(double);
							break;

						case BIT:
							s += sizeof(bool);
							break;

						case TINYINT:
							s += sizeof(byte);
							break;

						case SMALLINT:
							s += sizeof(short);
							break;
	
						case INTEGER:
							s += sizeof(int);
							break;

						case BIGINT:
							s += sizeof(long);
							break;

						case BINARY:
						case VARBINARY:
						case LONGVARBINARY:
						case OTHER:
							s += 4;
							s += ((ByteArray)o).byteValue().Length;

							break;

						default:
							s += o.ToString().Length;
							s += 1; 
					}
				}
			}

			return s;
		}

	}
}
