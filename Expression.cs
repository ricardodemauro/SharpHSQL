/*
 * Expression.cs
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

	/**
	 * Expression class declaration
	 *
	 *
	 * @version 1.0.0.1
	 */
	class Expression 
	{

		// leaf types
		public const int    VALUE = 1, COLUMN = 2, QUERY = 3, TRUE = 4,
			VALUELIST = 5, ASTERIX = 6, FUNCTION = 7;

		// operations
		public const int    NEGATE = 9, ADD = 10, SUBTRACT = 11, MULTIPLY = 12,
			DIVIDE = 14, CONCAT = 15;

		// logical operations
		public const int    NOT = 20, EQUAL = 21, BIGGER_EQUAL = 22, BIGGER = 23,
			SMALLER = 24, SMALLER_EQUAL = 25, NOT_EQUAL = 26,
			LIKE = 27, AND = 28, OR = 29, IN = 30, EXISTS = 31;

		// aggregate functions
		public const int    COUNT = 40, SUM = 41, MIN = 42, MAX = 43, AVG = 44;

		// system functions
		public const int    IFNULL = 60, CONVERT = 61, CASEWHEN = 62;

		// temporary used during paring
		public const int    PLUS = 100, OPEN = 101, CLOSE = 102, SELECT = 103,
			COMMA = 104, STRINGCONCAT = 105, BETWEEN = 106,
			CAST = 107, END = 108;
		
		private int		iType;

		// nodes

		private Expression  eArg, eArg2;

		// VALUE, VALUELIST
		private object      oData;
		private Hashtable   hList;
		private int		iDataType;

		// QUERY (correlated subquery)

		private Select      sSelect;

		// FUNCTION

		// Needed features of .NET do not exist (Yet)private Function    fFunction;

		// LIKE
		private char	cLikeEscape;

		// COLUMN
		private string      sTable, sColumn;
		private TableFilter tFilter;	// null if not yet resolved
		private int		iColumn;
		private string      sAlias;    // if it is a column of a select column list
		private bool     bDescending;    // if it is a column in a order by

		/**
		 * Constructor declaration
		 *
		 *
		 * @param f
		 */
		/*		Expression(Function f) 
				{
					iType = FUNCTION;
					fFunction = f;
				}
		*/
		/**
		 * Constructor declaration
		 *
		 *
		 * @param e
		 */
		public Expression(Expression e) 
		{
			iType = e.iType;
			iDataType = e.iDataType;
			eArg = e.eArg;
			eArg2 = e.eArg2;
			cLikeEscape = e.cLikeEscape;
			sSelect = e.sSelect;
			//			fFunction = e.fFunction;
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param s
		 */
		public Expression(Select s) 
		{
			iType = QUERY;
			sSelect = s;
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param v
		 */
		public Expression(ArrayList v) 
		{
			iType = VALUELIST;
			iDataType = Column.VARCHAR;

			int len = v.Count;

			hList = new Hashtable(len);

			for (int i = 0; i < len; i++) 
			{
				object o = v[i];

				if (o != null) 
				{
					hList.Add(o, this);    // todo: don't use such dummy objects
				}
			}
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param type
		 * @param e
		 * @param e2
		 */
		public Expression(int type, Expression e, Expression e2) 
		{
			iType = type;
			eArg = e;
			eArg2 = e2;
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param table
		 * @param column
		 */
		public Expression(string table, string column) 
		{
			sTable = table;

			if (column == null) 
			{
				iType = ASTERIX;
			} 
			else 
			{
				iType = COLUMN;
				sColumn = column;
			}
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param datatype
		 * @param o
		 */
		public Expression(int datatype, object o) 
		{
			iType = VALUE;
			iDataType = datatype;
			oData = o;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 */
		public void setLikeEscape(char c) 
		{
			cLikeEscape = c;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param type
		 */
		public void setDataType(int type) 
		{
			iDataType = type;
		}

		/**
		 * Method declaration
		 *
		 */
		public void setTrue() 
		{
			iType = TRUE;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isAggregate() 
		{
			if (iType == COUNT || iType == MAX || iType == MIN || iType == SUM
				|| iType == AVG) 
			{
				return true;
			}

			// todo: recurse eArg and eArg2; maybe they are grouped.
			// grouping 'correctly' would be quite complex
			return false;
		}

		/**
		 * Method declaration
		 *
		 */
		public void setDescending() 
		{
			bDescending = true;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isDescending() 
		{
			return bDescending;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 */
		public void setAlias(string s) 
		{
			sAlias = s;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public string getAlias() 
		{
			if (sAlias != null) 
			{
				return sAlias;
			}

			if (iType == VALUE) 
			{
				return "";
			}

			if (iType == COLUMN) 
			{
				return sColumn;
			}

			// todo
			return "";
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getType() 
		{
			return iType;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getColumnNr() 
		{
			return iColumn;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public Expression getArg() 
		{
			return eArg;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public Expression getArg2() 
		{
			return eArg2;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public TableFilter getFilter() 
		{
			return tFilter;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void checkResolved() 
		{
			Trace.check(iType != COLUMN || tFilter != null,
				Trace.COLUMN_NOT_FOUND, sColumn);

			if (eArg != null) 
			{
				eArg.checkResolved();
			}

			if (eArg2 != null) 
			{
				eArg2.checkResolved();
			}

			if (sSelect != null) 
			{
				sSelect.checkResolved();
			}

			/*			if (fFunction != null) 
						{
							fFunction.checkResolved();
						}
			*/		}

		/**
		 * Method declaration
		 *
		 *
		 * @param f
		 *
		 * @throws Exception
		 */
		public void resolve(TableFilter f) 
		{
			if (f != null && iType == COLUMN) 
			{
				if (sTable == null || f.getName().Equals(sTable)) 
				{
					int i = f.getTable().searchColumn(sColumn);

					if (i != -1) 
					{

						// todo: other error message: multiple tables are possible
						Trace.check(tFilter == null || tFilter == f,
							Trace.COLUMN_NOT_FOUND, sColumn);

						tFilter = f;
						iColumn = i;
						sTable = f.getName();
						iDataType = f.getTable().getColumnType(i);
					}
				}
			}

			// currently sets only data type
			// todo: calculate fixed expressions if possible
			if (eArg != null) 
			{
				eArg.resolve(f);
			}

			if (eArg2 != null) 
			{
				eArg2.resolve(f);
			}

			if (sSelect != null) 
			{
				sSelect.resolve(f, false);
				sSelect.resolve();
			}

			/*			if (fFunction != null) 
						{
							fFunction.resolve(f);
						}
			*/
			if (iDataType != 0) 
			{
				return;
			}

			switch (iType) 
			{

					/*				case FUNCTION:
										iDataType = fFunction.getReturnType();

										break;
					*/
				case QUERY:
					iDataType = sSelect.eColumn[0].iDataType;

					break;

				case NEGATE:
					iDataType = eArg.iDataType;

					break;

				case ADD:

				case SUBTRACT:

				case MULTIPLY:

				case DIVIDE:
					iDataType = eArg.iDataType;

					break;

				case CONCAT:
					iDataType = Column.VARCHAR;

					break;

				case NOT:

				case EQUAL:

				case BIGGER_EQUAL:

				case BIGGER:

				case SMALLER:

				case SMALLER_EQUAL:

				case NOT_EQUAL:

				case LIKE:

				case AND:

				case OR:

				case IN:

				case EXISTS:
					iDataType = Column.BIT;

					break;

				case COUNT:
					iDataType = Column.INTEGER;

					break;

				case MAX:

				case MIN:

				case SUM:

				case AVG:
					iDataType = eArg.iDataType;

					break;

				case CONVERT:

					// it is already set
					break;

				case IFNULL:

				case CASEWHEN:
					iDataType = eArg2.iDataType;

					break;
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isResolved() 
		{
			if (iType == VALUE) 
			{
				return true;
			}

			if (iType == COLUMN) 
			{
				return tFilter != null;
			}

			// todo: could recurse here, but never miss a 'false'!
			return false;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 *
		 * @return
		 */
		public static bool isCompare(int i) 
		{
			switch (i) 
			{

				case EQUAL:

				case BIGGER_EQUAL:

				case BIGGER:

				case SMALLER:

				case SMALLER_EQUAL:

				case NOT_EQUAL:
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
		public string getTableName() 
		{
			if (iType == ASTERIX) 
			{
				return sTable;
			}

			if (iType == COLUMN) 
			{
				if (tFilter == null) 
				{
					return sTable;
				} 
				else 
				{
					return tFilter.getTable().getName();
				}
			}

			// todo
			return "";
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public string getColumnName() 
		{
			if (iType == COLUMN) 
			{
				if (tFilter == null) 
				{
					return sColumn;
				} 
				else 
				{
					return tFilter.getTable().getColumnName(iColumn);
				}
			}

			return getAlias();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void swapCondition() 
		{
			int i = EQUAL;

			switch (iType) 
			{

				case BIGGER_EQUAL:
					i = SMALLER_EQUAL;

					break;

				case SMALLER_EQUAL:
					i = BIGGER_EQUAL;

					break;

				case SMALLER:
					i = BIGGER;

					break;

				case BIGGER:
					i = SMALLER;

					break;

				case EQUAL:
					break;

				default:
					Trace.assert(false, "Expression.swapCondition");
			}

			iType = i;

			Expression e = eArg;

			eArg = eArg2;
			eArg2 = e;
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
		public object getValue(int type) 
		{
			object o = getValue();

			if (o == null || iDataType == type) 
			{
				return o;
			}

			string s = Column.convertobject(o);

			return Column.convertstring(s, type);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getDataType() 
		{
			return iDataType;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public object getValue() 
		{
			switch (iType) 
			{

				case VALUE:
					return oData;

				case COLUMN:
					try 
					{
						return tFilter.oCurrentData[iColumn];
					} 
					catch (Exception e) 
					{
						throw Trace.error(Trace.COLUMN_NOT_FOUND, sColumn);
					}

					/*				case FUNCTION:
										return fFunction.getValue();
					*/
				case QUERY:
					return sSelect.getValue(iDataType);

				case NEGATE:
					return Column.negate(eArg.getValue(iDataType), iDataType);

				case COUNT:

					// count(*): sum(1); count(col): sum(col<>null)
					if (eArg.iType == ASTERIX || eArg.getValue() != null) 
					{
						return 1;
					}

					return 0;

				case MAX:

				case MIN:

				case SUM:

				case AVG:
					return eArg.getValue();

				case EXISTS:
					return test();

				case CONVERT:
					return eArg.getValue(iDataType);

				case CASEWHEN:
					if (eArg.test()) 
					{
						return eArg2.eArg.getValue();
					} 
					else 
					{
						return eArg2.eArg2.getValue();
					}
			}

			// todo: simplify this
			object a = null, b = null;

			if (eArg != null) 
			{
				a = eArg.getValue(iDataType);
			}

			if (eArg2 != null) 
			{
				b = eArg2.getValue(iDataType);
			}

			switch (iType) 
			{

				case ADD:
					return Column.add(a, b, iDataType);

				case SUBTRACT:
					return Column.subtract(a, b, iDataType);

				case MULTIPLY:
					return Column.multiply(a, b, iDataType);

				case DIVIDE:
					return Column.divide(a, b, iDataType);

				case CONCAT:
					return Column.concat(a, b);

				case IFNULL:
					return a == null ? b : a;

				default:

					// must be comparisation
					// todo: make sure it is
					return test();
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param o
		 * @param datatype
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private bool testValueList(object o,
			int datatype) 
		{
			if (iType == VALUELIST) 
			{
				if (datatype != iDataType) 
				{
					o = Column.convertobject(o, iDataType);
				}

				return hList.ContainsKey(o);
			} 
			else if (iType == QUERY) 
			{

				// todo: convert to valuelist before if everything is resolvable
				Result r = sSelect.getResult(0);
				Record n = r.rRoot;
				int    type = r.iType[0];

				if (datatype != type) 
				{
					o = Column.convertobject(o, type);
				}

				while (n != null) 
				{
					object o2 = n.data[0];

					if (o2 != null && o2.Equals(o)) 
					{
						return true;
					}

					n = n.next;
				}

				return false;
			}

			throw Trace.error(Trace.WRONG_DATA_TYPE);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public bool test() 
		{
			switch (iType) 
			{

				case TRUE:
					return true;

				case NOT:
					Trace.assert(eArg2 == null, "Expression.test");

					return !eArg.test();

				case AND:
					return eArg.test() && eArg2.test();

				case OR:
					return eArg.test() || eArg2.test();

				case LIKE:

					// todo: now for all tests a new 'like' object required!
					string s = (string) eArg2.getValue(Column.VARCHAR);
					int    type = eArg.iDataType;
					Like   l = new Like(s, cLikeEscape,	type == Column.VARCHAR_IGNORECASE);
					string c = (string) eArg.getValue(Column.VARCHAR);

					return l.compare(c);

				case IN:
					return eArg2.testValueList(eArg.getValue(), eArg.iDataType);

				case EXISTS:
					Result r = eArg.sSelect.getResult(1);    // 1 is already enough

					return r.rRoot != null;
			}

			Trace.check(eArg != null, Trace.GENERAL_ERROR);

			object o = eArg.getValue();
			int    dtype = eArg.iDataType;

			Trace.check(eArg2 != null, Trace.GENERAL_ERROR);

			object o2 = eArg2.getValue(dtype);
			int    result = Column.compare(o, o2, dtype);

			switch (iType) 
			{

				case EQUAL:
					return result == 0;

				case BIGGER:
					return result > 0;

				case BIGGER_EQUAL:
					return result >= 0;

				case SMALLER_EQUAL:
					return result <= 0;

				case SMALLER:
					return result < 0;

				case NOT_EQUAL:
					return result != 0;
			}

			Trace.assert(false, "Expression.test2");

			return false;
		}

	}
}
