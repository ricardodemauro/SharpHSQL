/*
 * Tokenizer.cs
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
	using System.Globalization;

	/**
	 * Tokenizer class declaration
	 *
	 *
	 * @version 1.0.0.1
	 */
	class Tokenizer 
	{
		private const int NAME = 1, LONG_NAME = 2, SPECIAL = 3,
			NUMBER = 4, FLOAT = 5, STRING = 6, LONG = 7;

		// used only internally
		private const int QUOTED_IDENTIFIER = 9, REMARK_LINE = 10,
			REMARK = 11;
		private string	     sCommand;
		private char[]	     cCommand;
		private int		     iLength;
		//		private object	     oValue;
		private int		     iIndex;
		private int		     iType;
		private string	     sToken, sLongNameFirst, sLongNameLast;
		private bool	     bWait;
		private static Hashtable hKeyword;

		string[] keyword = 
		{
			"AND", "ALL", "AVG", "BY", "BETWEEN", "COUNT", "CASEWHEN",
			"DISTINCT", "EXISTS", "EXCEPT", "FALSE", "FROM",
			"GROUP", "IF", "INTO", "IFNULL", "IS", "IN", "INTERSECT", "INNER",
			"LEFT", "LIKE", "MAX", "MIN", "NULL", "NOT", "ON", "ORDER", "OR",
			"OUTER", "PRIMARY", "SELECT", "SET", "SUM", "TO", "TRUE",
			"UNIQUE", "UNION", "VALUES", "WHERE", "CONVERT", "CAST",
			"CONCAT", "MINUS", "CALL"
		};

	
    

		/**
		 * Constructor declaration
		 *
		 *
		 * @param s
		 */
		public Tokenizer(string s) 
		{
			if (hKeyword == null)
			{
				hKeyword = new Hashtable();
				for (int i = 0; i < keyword.Length; i++) 
				{
					hKeyword.Add(keyword[i], i);
				}	
			}

			sCommand = s;
			cCommand = s.ToCharArray();
			iLength = cCommand.Length;
			iIndex = 0;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void back() 
		{
			Trace.assert(!bWait, "back");

			bWait = true;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param match
		 *
		 * @throws Exception
		 */
		public void getThis(string match) 
		{
			getToken();

			if (!sToken.Equals(match)) 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public string getstringToken() 
		{
			getToken();

			// todo: this is just compatibility for old style USER 'sa'
			if (iType == STRING) 
			{
				return sToken.Substring(1).ToUpper();
			} 
			else if (iType == NAME) 
			{
				return sToken;
			} 
			else if (iType == QUOTED_IDENTIFIER) 
			{
				return sToken.ToUpper();
			}

			throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool wasValue() 
		{
			if (iType == STRING || iType == NUMBER || iType == FLOAT) 
			{
				return true;
			}

			if (sToken.Equals("NULL")) 
			{
				return true;
			}

			if (sToken.Equals("TRUE") || sToken.Equals("FALSE")) 
			{
				return true;
			}

			return false;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool wasLongName() 
		{
			return iType == LONG_NAME;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool wasName() 
		{
			if (iType == QUOTED_IDENTIFIER) 
			{
				return true;
			}

			if (iType != NAME) 
			{
				return false;
			}

			return !hKeyword.ContainsKey(sToken);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public string getLongNameFirst() 
		{
			return sLongNameFirst;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public string getLongNameLast() 
		{
			return sLongNameLast;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public string getName() 
		{
			getToken();

			if (!wasName()) 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
			}

			return sToken;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public string getstring() 
		{
			getToken();

			return sToken;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getType() 
		{

			// todo: make sure it's used only for Values!
			// todo: synchronize iType with hColumn
			switch (iType) 
			{

				case STRING:
					return Column.VARCHAR;

				case NUMBER:
					return Column.INTEGER;

				case FLOAT:
					return Column.DOUBLE;

				case LONG:
					return Column.BIGINT;
			}

			return Column.NULL;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public object getAsValue() 
		{
			if (!wasValue()) 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
			}

			if (iType == STRING) 
			{
				return sToken.Substring(1);    // todo: this is a bad solution: remove '
			}

			// convert NULL to null string if not a string
			// todo: make this more straightforward
			if (sToken.Equals("NULL")) 
			{
				return null;
			}

			if (iType == NUMBER) 
			{
				if (sToken.Length > 9) 
				{

					// 2147483647 is the biggest int value, so more than
					// 9 digits are better returend as a long
					iType = LONG;

					return sToken.ToInt64();
				}

				return sToken.ToInt32();
			} 
			else if (iType == FLOAT) 
			{
				return sToken.ToDouble();
			}

			return sToken;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getPosition() 
		{
			return iIndex;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param begin
		 * @param end
		 *
		 * @return
		 */
		public string getPart(int begin, int end) 
		{
			return sCommand.Substring(begin, end);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void getToken() 
		{
			if (bWait) 
			{
				bWait = false;

				return;
			}

			while (iIndex < iLength && CharacterInfo.IsWhiteSpace(cCommand[iIndex])) 
			{
				iIndex++;
			}

			sToken = "";

			if (iIndex >= iLength) 
			{
				iType = 0;

				return;
			}

			bool      point = false, digit = false, exp = false,
				afterexp = false;
			bool      end = false;
			char	     c = cCommand[iIndex];
			char	     cfirst = '0';
			StringBuilder name = new StringBuilder();

            if (CharacterInfo.IsLetter(c)) {
	            iType = NAME;
			}
	        else if ("(),*=;+%".IndexOf(c) >= 0) 
			{
				iType = SPECIAL;
				iIndex++;
				sToken = "" + c;

				return;
			} 
			else if (CharacterInfo.IsDigit(c)) 
			{
				iType = NUMBER;
				digit = true;
			} 
			else if ("!<>|/-".IndexOf(c) >= 0) 
			{
				cfirst = c;
				iType = SPECIAL;
			} 
			else if (c == '\"') 
			{
				iType = QUOTED_IDENTIFIER;
			} 
			else if (c == '\'') 
			{
				iType = STRING;

				name.Append('\'');
			} 
			else if (c == '.') 
			{
				iType = FLOAT;
				point = true;
			} 
			else 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, "" + c);
			}

			int start = iIndex++;

			while (true) 
			{
				if (iIndex >= iLength) 
				{
					c = ' ';
					end = true;

					Trace.check(iType != STRING && iType != QUOTED_IDENTIFIER,
						Trace.UNEXPECTED_END_OF_COMMAND);
				} 
				else 
				{
					c = cCommand[iIndex];
				}

				switch (iType) 
				{

					case NAME:
                        if (CharacterInfo.IsLetter(c) || CharacterInfo.IsDigit(c) || c.Equals('_')) {
	                        break;
			            }

						sToken = sCommand.Substring(start, (iIndex - start)).ToUpper();

						if (c == '.') 
						{
							sLongNameFirst = sToken;
							iIndex++;

							getToken();	       // todo: eliminate recursion

							sLongNameLast = sToken;
							iType = LONG_NAME;
							sToken = sLongNameFirst + "." + sLongNameLast;
						}

						return;

					case QUOTED_IDENTIFIER:
						if (c == '\"') 
						{
							iIndex++;

							if (iIndex >= iLength) 
							{
								sToken = name.ToString();

								return;
							}

							c = cCommand[iIndex];

							if (c == '.') 
							{
								sLongNameFirst = name.ToString();
								iIndex++;

								getToken();    // todo: eliminate recursion

								sLongNameLast = sToken;
								iType = LONG_NAME;
								sToken = sLongNameFirst + "." + sLongNameLast;

								return;
							}

							if (c != '\"') 
							{
								sToken = name.ToString();

								return;
							}
						}

						name.Append(c);

						break;

					case STRING:
						if (c == '\'') 
						{
							iIndex++;

							if (iIndex >= iLength || cCommand[iIndex] != '\'') 
							{
								sToken = name.ToString();

								return;
							}
						}

						name.Append(c);

						break;

					case REMARK:
						if (end) 
						{

							// unfinished remark
							// maybe print error here
							iType = 0;

							return;
						} 
						else if (c == '*') 
						{
							iIndex++;

							if (iIndex < iLength && cCommand[iIndex] == '/') 
							{

								// using recursion here
								iIndex++;

								getToken();

								return;
							}
						}

						break;

					case REMARK_LINE:
						if (end) 
						{
							iType = 0;

							return;
						} 
						else if (c == '\r' || c == '\n') 
						{

							// using recursion here
							getToken();

							return;
						}

						break;

					case SPECIAL:
						if (c == '/' && cfirst == '/') 
						{
							iType = REMARK_LINE;

							break;
						} 
						else if (c == '-' && cfirst == '-') 
						{
							iType = REMARK_LINE;

							break;
						} 
						else if (c == '*' && cfirst == '/') 
						{
							iType = REMARK;

							break;
						} 
						else if (">=|".IndexOf(c) >= 0) 
						{
							break;
						}

						sToken = sCommand.Substring(start, (iIndex - start));

						return;

					case FLOAT:

					case NUMBER:
						if (CharacterInfo.IsDigit(c)) 
						{
							digit = true;
						} 
						else if (c == '.') 
						{
							iType = FLOAT;

							if (point) 
							{
								throw Trace.error(Trace.UNEXPECTED_TOKEN, ".");
							}

							point = true;
						} 
						else if (c == 'E' || c == 'e') 
						{
							if (exp) 
							{
								throw Trace.error(Trace.UNEXPECTED_TOKEN, "E");
							}

							afterexp = true;    // first character after exp may be + or -
							point = true;
							exp = true;
						} 
						else if (c == '-' && afterexp) 
						{
							afterexp = false;
						} 
						else if (c == '+' && afterexp) 
						{
							afterexp = false;
						} 
						else 
						{
							afterexp = false;

							if (!digit) 
							{
								if (point && start == iIndex - 1) 
								{
									sToken = ".";
									iType = SPECIAL;

									return;
								}

								throw Trace.error(Trace.UNEXPECTED_TOKEN, "" + c);
							}

							sToken = sCommand.Substring(start, (iIndex - start));

							return;
						}
				}

				iIndex++;
			}
		}

	}
}
