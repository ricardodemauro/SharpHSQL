/*
 * Trace.cs
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
	using System.IO;
	using System.Collections;

	/**
	 * Class declaration
	 *
	 *
	 * @version 1.0.0.1
	 */
	class Trace
	{

		//#ifdef TRACE
		/*
			public static bool TRACE=true;
		*/
		//#else
		public static bool TRACE = false;
		//#endif
		public static bool STOP = false;
		public static bool ASSERT = false;
		private static Trace	tTracer = new Trace();
		private static int		iLine;
		private static string       sTrace;
		private static int		iStop = 0;
		public static int		DATABASE_ALREADY_IN_USE = 0,
			CONNECTION_IS_CLOSED = 1,
			CONNECTION_IS_BROKEN = 2,
			DATABASE_IS_SHUTDOWN = 3,
			COLUMN_COUNT_DOES_NOT_MATCH = 4,
			DIVISION_BY_ZERO = 5, INVALID_ESCAPE = 6,
			INTEGRITY_CONSTRAINT_VIOLATION = 7,
			VIOLATION_OF_UNIQUE_INDEX = 8,
			TRY_TO_INSERT_NULL = 9,
			UNEXPECTED_TOKEN = 10,
			UNEXPECTED_END_OF_COMMAND = 11,
			UNKNOWN_FUNCTION = 12, NEED_AGGREGATE = 13,
			SUM_OF_NON_NUMERIC = 14,
			WRONG_DATA_TYPE = 15,
			SINGLE_VALUE_EXPECTED = 16,
			SERIALIZATION_FAILURE = 17,
			TRANSFER_CORRUPTED = 18,
			FUNCTION_NOT_SUPPORTED = 19,
			TABLE_ALREADY_EXISTS = 20,
			TABLE_NOT_FOUND = 21,
			INDEX_ALREADY_EXISTS = 22,
			SECOND_PRIMARY_KEY = 23,
			DROP_PRIMARY_KEY = 24, INDEX_NOT_FOUND = 25,
			COLUMN_ALREADY_EXISTS = 26,
			COLUMN_NOT_FOUND = 27, FILE_IO_ERROR = 28,
			WRONG_DATABASE_FILE_VERSION = 29,
			DATABASE_IS_READONLY = 30,
			ACCESS_IS_DENIED = 31,
			INPUTSTREAM_ERROR = 32,
			NO_DATA_IS_AVAILABLE = 33,
			USER_ALREADY_EXISTS = 34,
			USER_NOT_FOUND = 35, ASSERT_FAILED = 36,
			EXTERNAL_STOP = 37, GENERAL_ERROR = 38,
			WRONG_OUT_PARAMETER = 39,
			ERROR_IN_FUNCTION = 40,
			TRIGGER_NOT_FOUND = 41;
		private static string[]       sDescription = 
		{
			"08001 The database is already in use by another process",
			"08003 Connection is closed", "08003 Connection is broken",
			"08003 The database is shutdown",
			"21S01 Column count does not match", "22012 Division by zero",
			"22019 Invalid escape character",
			"23000 Integrity constraint violation",
			"23000 Violation of unique index",
			"23000 Try to insert null into a non-nullable column",
			"37000 Unexpected token", "37000 Unexpected end of command",
			"37000 Unknown function",
			"37000 Need aggregate function or group by",
			"37000 Sum on non-numeric data not allowed", "37000 Wrong data type",
			"37000 Single value expected", "40001 Serialization failure",
			"40001 Transfer corrupted", "IM001 This function is not supported",
			"S0001 Table already exists", "S0002 Table not found",
			"S0011 Index already exists",
			"S0011 Attempt to define a second primary key",
			"S0011 Attempt to drop the primary key", "S0012 Index not found",
			"S0021 Column already exists", "S0022 Column not found",
			"S1000 File input/output error", "S1000 Wrong database file version",
			"S1000 The database is in read only mode", "S1000 Access is denied",
			"S1000 InputStream error", "S1000 No data is available",
			"S1000 User already exists", "S1000 User not found",
			"S1000 Assert failed", "S1000 External stop request",
			"S1000 General error", "S1009 Wrong OUT parameter",
			"S1010 Error in function", "S0002 Trigger not found"
		};

		/**
		 * Method declaration
		 *
		 *
		 * @param code
		 * @param add
		 *
		 * @return
		 */
		public static Exception getError(int code, string add) 
		{
			string s = getMessage(code);

			if (add != null) 
			{
				s += ": " + add;
			}

			return getError(s);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param code
		 *
		 * @return
		 */
		public static string getMessage(int code) 
		{
			return sDescription[code];
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param e
		 *
		 * @return
		 */
		public static string getMessage(Exception e) 
		{
			return e.Message;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param msg
		 *
		 * @return
		 */
		public static Exception getError(string msg) 
		{
			return new Exception(msg);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param code
		 *
		 * @return
		 */
		public static Exception error(int code) 
		{
			return getError(code, null);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param code
		 * @param s
		 *
		 * @return
		 */
		public static Exception error(int code, string s) 
		{
			return getError(code, s);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param code
		 * @param i
		 *
		 * @return
		 */
		public static Exception error(int code, int i) 
		{
			return getError(code, "" + i);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param condition
		 *
		 * @throws Exception
		 */
		public static void assert(bool condition) 
		{
			assert(condition, null);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param condition
		 * @param error
		 *
		 * @throws Exception
		 */
		public static void assert(bool condition, string error) 
		{
			if (!condition) 
			{
				printStack();

				throw getError(ASSERT_FAILED, error);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param condition
		 * @param code
		 *
		 * @throws Exception
		 */
		public static void check(bool condition, int code) 
		{
			check(condition, code, null);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param condition
		 * @param code
		 * @param s
		 *
		 * @throws Exception
		 */
		public static void check(bool condition, int code,	string s) 
		{
			if (!condition) 
			{
				throw getError(code, s);
			}
		}

		// for the PrinterWriter interface

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 */
		public void println(char[] c) 
		{
			if (iLine++ == 2) 
			{
				string s = new string(c);
				int    i = s.IndexOf('.');

				if (i != -1) 
				{
					s = s.Substring(i + 1);
				}

				i = s.IndexOf('(');

				if (i != -1) 
				{
					s = s.Substring(0, i);
				}

				sTrace = s;
			}
		}

		/**
		 * Constructor declaration
		 *
		 */
		public Trace() 
		{
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param l
		 */
		public static void trace(long l) 
		{
			traceCaller("" + l);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 */
		public static void trace(int i) 
		{
			traceCaller("" + i);
		}

		/**
		 * Method declaration
		 *
		 */
		public static void trace() 
		{
			traceCaller("");
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 */
		public static void trace(string s) 
		{
			traceCaller(s);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public static void stop() 
		{
			stop(null);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @throws Exception
		 */
		public static void stop(string s) 
		{
			if (iStop++ % 10000 != 0) 
			{
				return;
			}

			if (new File("trace.stop").Exists) 
			{
				printStack();

				throw getError(EXTERNAL_STOP, s);
			}
		}

		/**
		 * Method declaration
		 *
		 */
		static private void printStack() 
		{
			Exception e = new Exception();

			Console.WriteLine(e.StackTrace);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 */
		static private void traceCaller(string s) 
		{
			Exception e = new Exception();

			iLine = 0;

			Console.WriteLine(e.StackTrace);

			s = sTrace + "\t" + s;

			// trace to System.out is handy if only trace messages of hsql are required
			//#ifdef TRACESYSTEMOUT
			/*

				System.out.println(s);

			*/
			//#else
			//			DriverManager.println(s);
			//#endif
		}

	}
}
