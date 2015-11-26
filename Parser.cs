/*
 * Parser.cs
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
	 * Class declaration
	 *
	 *
	 * @version 1.0.0.1
	 */
	class Parser 
	{
		private Database  dDatabase;
		private Tokenizer tTokenizer;
		private Channel   cChannel;
		private string    sTable;
		private string    sToken;
		private object    oData;
		private int       iType;
		private int       iToken;

		/**
		 * Constructor declaration
		 *
		 *
		 * @param db
		 * @param t
		 * @param channel
		 */
		public Parser(Database db, Tokenizer t, Channel channel) 
		{
			dDatabase = db;
			tTokenizer = t;
			cChannel = channel;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Result processSelect() 
		{
			Select select = parseSelect();

			if (select.sIntoTable == null) 
			{
				// fredt@users.sourceforge.net begin changes from 1.50
				//	   return select.getResult(cChannel.getMaxRows());
				return select.getResult( select.limitStart, select.limitCount );
				// fredt@users.sourceforge.net end changes from 1.50
			} 
			else 
			{
				Result r = select.getResult(0);
				Table  t = new Table(dDatabase, true, select.sIntoTable, false);

				t.addColumns(r);
				t.createPrimaryKey();

				// SELECT .. INTO can't fail because of violation of primary key
				t.insert(r, cChannel);
				dDatabase.linkTable(t);

				int i = r.getSize();

				r = new Result();
				r.iUpdateCount = i;

				return r;
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
		public Result processCall() 
		{
			Expression e = parseExpression();

			e.resolve(null);

			int    type = e.getDataType();
			object o = e.getValue();
			Result r = new Result(1);

			r.sTable[0] = "";
			r.iType[0] = type;
			r.sLabel[0] = "";
			r.sName[0] = "";

			object[] row = new object[1];

			row[0] = o;

			r.add(row);

			return r;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Result processUpdate() 
		{
			string token = tTokenizer.getstring();

			cChannel.checkReadWrite();
			cChannel.check(token, Access.UPDATE);

			Table       table = dDatabase.getTable(token, cChannel);
			TableFilter filter = new TableFilter(table, null, false);

			tTokenizer.getThis("SET");

			ArrayList vColumn = new ArrayList();
			ArrayList eColumn = new ArrayList();
			int    len = 0;

			token = null;

			do 
			{
				len++;

				int i = table.getColumnNr(tTokenizer.getstring());

				vColumn.Add(i);
				tTokenizer.getThis("=");

				Expression e = parseExpression();

				e.resolve(filter);
				eColumn.Add(e);

				token = tTokenizer.getstring();
			} while (token.Equals(","));

			Expression eCondition = null;

			if (token.Equals("WHERE")) 
			{
				eCondition = parseExpression();

				eCondition.resolve(filter);
				filter.setCondition(eCondition);
			} 
			else 
			{
				tTokenizer.back();
			}

			// do the update
			Expression[] exp = new Expression[len];

			eColumn.CopyTo(exp);

			int[] col = new int[len];
			int[] type = new int[len];

			for (int i = 0; i < len; i++) 
			{
				col[i] = ((int) vColumn[i]);
				type[i] = table.getType(col[i]);
			}

			int count = 0;

			if (filter.findFirst()) 
			{
				Result del = new Result();    // don't need column count and so on
				Result ins = new Result();
				int    size = table.getColumnCount();

				do 
				{
					if (eCondition == null || eCondition.test()) 
					{
						object[] nd = filter.oCurrentData;

						del.add(nd);

						object[] ni = table.getNewRow();

						for (int i = 0; i < size; i++) 
						{
							ni[i] = nd[i];
						}

						for (int i = 0; i < len; i++) 
						{
							ni[col[i]] = exp[i].getValue(type[i]);
						}

						ins.add(ni);
					}
				} while (filter.next());

				cChannel.beginNestedTransaction();

				try 
				{
					Record nd = del.rRoot;

					while (nd != null) 
					{
						table.deleteNoCheck(nd.data, cChannel);

						nd = nd.next;
					}

					Record ni = ins.rRoot;

					while (ni != null) 
					{
						table.insertNoCheck(ni.data, cChannel);

						ni = ni.next;
						count++;
					}

					table.checkUpdate(col, del, ins);

					ni = ins.rRoot;

					while (ni != null) 
					{
						ni = ni.next;
					}

					cChannel.endNestedTransaction(false);
				} 
				catch (Exception e) 
				{

					// update failed (violation of primary key / referential integrity)
					cChannel.endNestedTransaction(true);

					throw e;
				}
			}

			Result r = new Result();

			r.iUpdateCount = count;

			return r;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Result processDelete() 
		{
			tTokenizer.getThis("FROM");

			string token = tTokenizer.getstring();

			cChannel.checkReadWrite();
			cChannel.check(token, Access.DELETE);

			Table       table = dDatabase.getTable(token, cChannel);
			TableFilter filter = new TableFilter(table, null, false);

			token = tTokenizer.getstring();

			Expression eCondition = null;

			if (token.Equals("WHERE")) 
			{
				eCondition = parseExpression();

				eCondition.resolve(filter);
				filter.setCondition(eCondition);
			} 
			else 
			{
				tTokenizer.back();
			}

			int count = 0;

			if (filter.findFirst()) 
			{
				Result del = new Result();    // don't need column count and so on

				do 
				{
					if (eCondition == null || eCondition.test()) 
					{
						del.add(filter.oCurrentData);
					}
				} while (filter.next());

				Record n = del.rRoot;

				while (n != null) 
				{
					table.delete(n.data, cChannel);

					count++;
					n = n.next;
				}
			}

			Result r = new Result();

			r.iUpdateCount = count;

			return r;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Result processInsert() 
		{
			tTokenizer.getThis("INTO");

			string token = tTokenizer.getstring();

			cChannel.checkReadWrite();
			cChannel.check(token, Access.INSERT);

			Table t = dDatabase.getTable(token, cChannel);

			token = tTokenizer.getstring();

			ArrayList vcolumns = null;

			if (token.Equals("(")) 
			{
				vcolumns = new ArrayList();

				int i = 0;

				while (true) 
				{
					vcolumns.Add(tTokenizer.getstring());

					i++;
					token = tTokenizer.getstring();

					if (token.Equals(")")) 
					{
						break;
					}

					if (!token.Equals(",")) 
					{
						throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
					}
				}

				token = tTokenizer.getstring();
			}

			int count = 0;
			int len;

			if (vcolumns == null) 
			{
				len = t.getColumnCount();
			} 
			else 
			{
				len = vcolumns.Count;
			}

			if (token.Equals("VALUES")) 
			{
				tTokenizer.getThis("(");

				object[] row = t.getNewRow();
				int    i = 0;

				while (true) 
				{
					int column;

					if (vcolumns == null) 
					{
						column = i;

						if (i > len) 
						{
							throw Trace.error(Trace.COLUMN_COUNT_DOES_NOT_MATCH);
						}
					} 
					else 
					{
						column = t.getColumnNr((string) vcolumns[i]);
					}

					row[column] = getValue(t.getType(column));
					i++;
					token = tTokenizer.getstring();

					if (token.Equals(")")) 
					{
						break;
					}

					if (!token.Equals(",")) 
					{
						throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
					}
				}

				t.insert(row, cChannel);

				count = 1;
			} 
			else if (token.Equals("SELECT")) 
			{
				Result result = processSelect();
				Record r = result.rRoot;

				Trace.check(len == result.getColumnCount(),
					Trace.COLUMN_COUNT_DOES_NOT_MATCH);

				int[] col = new int[len];
				int[] type = new int[len];

				for (int i = 0; i < len; i++) 
				{
					int j;

					if (vcolumns == null) 
					{
						j = i;
					} 
					else 
					{
						j = t.getColumnNr((string) vcolumns[i]);
					}

					col[i] = j;
					type[i] = t.getType(j);
				}

				cChannel.beginNestedTransaction();

				try 
				{
					while (r != null) 
					{
						object[] row = t.getNewRow();

						for (int i = 0; i < len; i++) 
						{
							row[col[i]] = Column.convertobject(r.data[i],
								type[i]);
						}

						t.insert(row, cChannel);

						count++;
						r = r.next;
					}

					cChannel.endNestedTransaction(false);
				} 
				catch (Exception e) 
				{

					// insert failed (violation of primary key)
					cChannel.endNestedTransaction(true);

					throw e;
				}
			} 
			else 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
			}

			Result rs = new Result();

			rs.iUpdateCount = count;

			return rs;
		}

		/**
		 * Method declaration
		 *
		 * ALTER TABLE tableName ADD COLUMN columnName columnType;
		 * ALTER TABLE tableName DELETE COLUMN columnName;
		 *
		 * <B>Note: </B>The only change I've made to Sergio's original code was
		 * changing the insert's to call insertNoCheck to bypass the trigger
		 * mechanism that is a part of hsqldb 1.60 and beyond. - Mark Tutt
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Result processAlter() 
		{
			tTokenizer.getThis("TABLE");

			string token = tTokenizer.getstring();

			cChannel.checkReadWrite();

			// cChannel.check(token,Access.ALTER); --> Accessul nu-l inca controleaza...
			string tName = token;
			string swap = tName + "SWAP";

			// nimicirea swapului...
			dDatabase.execute("DROP TABLE " + swap, cChannel);

			Table initialTable = dDatabase.getTable(token, cChannel);
			int   count = 0;

			token = tTokenizer.getstring();

			if (token.Equals("ADD")) 
			{
				token = tTokenizer.getstring();

				if (token.Equals("COLUMN")) 
				{
					Table swapTable = new Table(dDatabase, true, swap,
						initialTable.isCached());

					// copiem coloanele (fara date) din tabelul initial in swap
					for (int i = 0; i < initialTable.getColumnCount(); i++) 
					{
						Column aColumn = initialTable.getColumn(i);

						swapTable.addColumn(aColumn);
					}

					// end Of copiem coloanele...
					// aflam daca are PrimaryKey & o cream...
					string  cName = tTokenizer.getstring();
					string  cType = tTokenizer.getstring();
					int     iType = Column.getTypeNr(cType);
					string  sToken = cType;
					//					int     primarykeycolumn = -1;
					bool identity = false;
					int     column = initialTable.getColumnCount() + 1;

					// !--
					// stolen from CREATE TABLE...
					string  sColumn = cName;

					if (iType == Column.VARCHAR && dDatabase.isIgnoreCase()) 
					{
						iType = Column.VARCHAR_IGNORECASE;
					}

					sToken = tTokenizer.getstring();

					if (iType == Column.DOUBLE && sToken.Equals("PRECISION")) 
					{
						sToken = tTokenizer.getstring();
					}

					if (sToken.Equals("(")) 
					{

						// overread length
						do 
						{
							sToken = tTokenizer.getstring();
						} while (!sToken.Equals(")"));

						sToken = tTokenizer.getstring();
					}

					// !--
					bool nullable = true;

					if (sToken.Equals("NULL")) 
					{
						sToken = tTokenizer.getstring();
					} 
					else if (sToken.Equals("NOT")) 
					{
						tTokenizer.getThis("NULL");

						nullable = false;
						sToken = tTokenizer.getstring();
					}

					/*
					 * if(sToken.Equals("IDENTITY")) {
					 * identity=true;
					 * Trace.check(primarykeycolumn==-1,Trace.SECOND_PRIMARY_KEY,sColumn);
					 * sToken=tTokenizer.getstring();
					 * primarykeycolumn=column;
					 * }
					 *
					 * if(sToken.Equals("PRIMARY")) {
					 * tTokenizer.getThis("KEY");
					 * Trace.check(identity || primarykeycolumn==-1,
					 * Trace.SECOND_PRIMARY_KEY,sColumn);
					 * primarykeycolumn=column;
					 * //sToken=tTokenizer.getstring();
					 * }
					 * //end of STOLEN...
					 */
					swapTable.addColumn(cName, iType, nullable,
						identity);    // under construction...

					if (initialTable.getColumnCount()
						< initialTable.getInternalColumnCount()) 
					{
						swapTable.createPrimaryKey();
					} 
					else 
					{
						swapTable.createPrimaryKey(initialTable.getPrimaryIndex().getColumns()[0]);
					}

					// endof PrimaryKey...
					// sa ne farimam cu indicii... ;-((
					Index idx = null;

					while (true) 
					{
						idx = initialTable.getNextIndex(idx);

						if (idx == null) 
						{
							break;
						}

						if (idx == initialTable.getPrimaryIndex()) 
						{
							continue;
						}

						swapTable.createIndex(idx);
					}

					// end of Index...
					cChannel.commit();
					dDatabase.linkTable(swapTable);

					Tokenizer tmpTokenizer = new Tokenizer("SELECT * FROM "
						+ tName);
					Parser    pp = new Parser(dDatabase, tmpTokenizer, cChannel);
					string    ff = tmpTokenizer.getstring();

					if (!initialTable.isEmpty()) 
					{
						Record n = ((Result) pp.processSelect()).rRoot;

						do 
						{
							object[] row = swapTable.getNewRow();
							object[] row1 = n.data;

							for (int i = 0; i < initialTable.getColumnCount();
								i++) 
							{
								row[i] = row1[i];
							}

							swapTable.insertNoCheck(row, cChannel);

							n = n.next;
						} while (n != null);
					}

					dDatabase.execute("DROP TABLE " + tName, cChannel);

					// cream tabelul vechi cu proprietatile celui nou...
					initialTable = new Table(dDatabase, true, tName,
						swapTable.isCached());

					for (int i = 0; i < swapTable.getColumnCount(); i++) 
					{
						Column aColumn = swapTable.getColumn(i);

						initialTable.addColumn(aColumn);
					}

					if (swapTable.getColumnCount()
						< swapTable.getInternalColumnCount()) 
					{
						initialTable.createPrimaryKey();
					} 
					else 
					{
						initialTable.createPrimaryKey(swapTable.getPrimaryIndex().getColumns()[0]);
					}

					// endof PrimaryKey...
					// sa ne farimam cu indicii... ;-((
					idx = null;

					while (true) 
					{
						idx = swapTable.getNextIndex(idx);

						if (idx == null) 
						{
							break;
						}

						if (idx == swapTable.getPrimaryIndex()) 
						{
							continue;
						}

						initialTable.createIndex(idx);
					}

					// end of Index...
					cChannel.commit();
					dDatabase.linkTable(initialTable);

					// end of cream...
					// copiem datele din swap in tabel...
					tmpTokenizer = new Tokenizer("SELECT * FROM " + swap);
					pp = new Parser(dDatabase, tmpTokenizer, cChannel);
					ff = tmpTokenizer.getstring();

					if (!swapTable.isEmpty()) 
					{
						Record n = ((Result) pp.processSelect()).rRoot;

						do 
						{
							object[] row = initialTable.getNewRow();
							object[] row1 = n.data;

							for (int i = 0; i < swapTable.getColumnCount(); i++) 
							{
								row[i] = row1[i];
							}

							initialTable.insertNoCheck(row, cChannel);

							n = n.next;
						} while (n != null);

						// end of copiem...
					}

					dDatabase.execute("DROP TABLE " + swap, cChannel);

					count = 4;
				} 
				else 
				{
					throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
				}
			} 
			else if (token.Equals("DELETE")) 
			{
				token = tTokenizer.getstring();

				if (token.Equals("COLUMN")) 
				{
					Table  swapTable = new Table(dDatabase, true, swap,
						initialTable.isCached());
					string cName = tTokenizer.getstring();
					int    undesired = initialTable.getColumnNr(cName);

					for (int i = 0; i < initialTable.getColumnCount(); i++) 
					{
						Column aColumn = initialTable.getColumn(i);

						if (i != undesired) 
						{
							swapTable.addColumn(aColumn);
						}
					}

					int pKey = -1;

					// !--
					if (initialTable.getColumnCount()
						< initialTable.getInternalColumnCount()) 
					{
						swapTable.createPrimaryKey();
					} 
					else 
					{
						int[] cols = initialTable.getPrimaryIndex().getColumns();

						pKey = cols[0];

						if ((cols[0] > undesired)
							|| (cols[0] + cols.Length < undesired)) 
						{
							if (undesired
								< initialTable.getPrimaryIndex().getColumns()[0]) 
							{

								// reindexarea...
								for (int i = 0; i < cols.Length; i++) 
								{
									cols[i]--;
								}

								// endOf reindexarea...
							}
							// MT: This initially wouldn't compile, missing the array index on cols[]
							swapTable.createPrimaryKey(cols[0]);
						} 
						else 
						{
							swapTable.createPrimaryKey();
						}
					}

					// endof PrimaryKey...
					// sa ne farimam cu indicii... ;-((
					Index idx = null;

					while (true) 
					{
						idx = initialTable.getNextIndex(idx);

						if (idx == null) 
						{
							break;
						}

						if (idx == initialTable.getPrimaryIndex()) 
						{
							continue;
						}

						bool flag = true;
						int[]   cols = idx.getColumns();

						for (int i = 0; i < cols.Length; i++) 
						{
							if (cols[i] == undesired) 
							{
								flag = false;
							}
						}

						if (flag) 
						{
							Index tIdx;

							for (int i = 0; i < cols.Length; i++) 
							{
								if (cols[i] > undesired) 
								{
									cols[i]--;
								}
							}

							tIdx = new Index(idx.getName(), idx.getColumns(),
								idx.getType(), idx.isUnique());

							swapTable.createIndex(tIdx);
						}
					}

					// !--
					cChannel.commit();
					dDatabase.linkTable(swapTable);

					Tokenizer tmpTokenizer = new Tokenizer("SELECT * FROM "
						+ tName);
					Parser    pp = new Parser(dDatabase, tmpTokenizer, cChannel);
					string    ff = tmpTokenizer.getstring();

					if (!initialTable.isEmpty()) 
					{
						Record n = ((Result) pp.processSelect()).rRoot;

						do 
						{
							object[] row = swapTable.getNewRow();
							object[] row1 = n.data;
							int    j = 0;

							for (int i = 0; i < initialTable.getColumnCount();
								i++) 
							{
								if (i != undesired) 
								{
									row[j] = row1[i];
									j++;
								}
							}

							swapTable.insertNoCheck(row, cChannel);

							n = n.next;
						} while (n != null);
					}

					dDatabase.execute("DROP TABLE " + tName, cChannel);

					// cream tabelul vechi cu proprietatile celui nou...
					initialTable = new Table(dDatabase, true, tName,
						swapTable.isCached());

					for (int i = 0; i < swapTable.getColumnCount(); i++) 
					{
						Column aColumn = swapTable.getColumn(i);

						initialTable.addColumn(aColumn);
					}

					// !--
					if (swapTable.getColumnCount()
						< swapTable.getInternalColumnCount()) 
					{
						initialTable.createPrimaryKey();
					} 
					else 
					{
						initialTable.createPrimaryKey(swapTable.getPrimaryIndex().getColumns()[0]);
					}

					// endof PrimaryKey...
					// sa ne farimam cu indicii... ;-((
					idx = null;

					while (true) 
					{
						idx = swapTable.getNextIndex(idx);

						if (idx == null) 
						{
							break;
						}

						if (idx == swapTable.getPrimaryIndex()) 
						{
							continue;
						}

						initialTable.createIndex(idx);
					}

					// end of Index...
					// !--
					cChannel.commit();
					dDatabase.linkTable(initialTable);

					// end of cream...
					// copiem datele din swap in tabel...
					tmpTokenizer = new Tokenizer("SELECT * FROM " + swap);
					pp = new Parser(dDatabase, tmpTokenizer, cChannel);
					ff = tmpTokenizer.getstring();

					if (!swapTable.isEmpty()) 
					{
						Record n = ((Result) pp.processSelect()).rRoot;

						do 
						{
							object[] row = initialTable.getNewRow();
							object[] row1 = n.data;

							for (int i = 0; i < swapTable.getColumnCount(); i++) 
							{
								row[i] = row1[i];
							}

							initialTable.insertNoCheck(row, cChannel);

							n = n.next;
						} while (n != null);

						// end of copiem...
					}

					dDatabase.execute("DROP TABLE " + swap, cChannel);

					count = 3;
				} 
				else 
				{
					throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
				}

				count = 3;
			}

			Result r = new Result();

			r.iUpdateCount = count;    // --> nu tre inca...??

			return r;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Select parseSelect() 
		{
			Select select = new Select();
			// fredt@users.sourceforge.net begin changes from 1.50
			select.limitStart = 0;
			select.limitCount = cChannel.getMaxRows();
			// fredt@users.sourceforge.net end changes from 1.50
			string token = tTokenizer.getstring();

			if (token.Equals("DISTINCT")) 
			{
				select.bDistinct = true;
				// fredt@users.sourceforge.net begin changes from 1.50
			} 
			else if( token.Equals("LIMIT")) 
			{
				string limStart = tTokenizer.getstring();
				string limEnd = tTokenizer.getstring();
				//System.out.println( "LIMIT used from "+limStart+","+limEnd);
				select.limitStart = limStart.ToInt32();
				select.limitCount = limEnd.ToInt32();
				// fredt@users.sourceforge.net end changes from 1.50
			} 
			else 
			{
				tTokenizer.back();
			}

			// parse column list
			ArrayList vcolumn = new ArrayList();

			do 
			{
				Expression e = parseExpression();

				token = tTokenizer.getstring();

				if (token.Equals("AS")) 
				{
					e.setAlias(tTokenizer.getName());

					token = tTokenizer.getstring();
				} 
				else if (tTokenizer.wasName()) 
				{
					e.setAlias(token);

					token = tTokenizer.getstring();
				}

				vcolumn.Add(e);
			} while (token.Equals(","));

			if (token.Equals("INTO")) 
			{
				select.sIntoTable = tTokenizer.getstring();
				token = tTokenizer.getstring();
			}

			if (!token.Equals("FROM")) 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, token);
			}

			Expression condition = null;

			// parse table list
			ArrayList     vfilter = new ArrayList();

			vfilter.Add(parseTableFilter(false));

			while (true) 
			{
				token = tTokenizer.getstring();

				if (token.Equals("LEFT")) 
				{
					token = tTokenizer.getstring();

					if (token.Equals("OUTER")) 
					{
						token = tTokenizer.getstring();
					}

					Trace.check(token.Equals("JOIN"), Trace.UNEXPECTED_TOKEN,
						token);
					vfilter.Add(parseTableFilter(true));
					tTokenizer.getThis("ON");

					condition = addCondition(condition, parseExpression());
				} 
				else if (token.Equals("INNER")) 
				{
					tTokenizer.getThis("JOIN");
					vfilter.Add(parseTableFilter(false));
					tTokenizer.getThis("ON");

					condition = addCondition(condition, parseExpression());
				} 
				else if (token.Equals(",")) 
				{
					vfilter.Add(parseTableFilter(false));
				} 
				else 
				{
					break;
				}
			}

			tTokenizer.back();

			int	    len = vfilter.Count;
			TableFilter[] filter = new TableFilter[len];

			vfilter.CopyTo(filter);

			select.tFilter = filter;

			// expand [table.]* columns
			len = vcolumn.Count;

			for (int i = 0; i < len; i++) 
			{
				Expression e = (Expression) (vcolumn[i]);

				if (e.getType() == Expression.ASTERIX) 
				{
					int    current = i;
					Table  table = null;
					string n = e.getTableName();

					for (int t = 0; t < filter.Length; t++) 
					{
						TableFilter f = filter[t];

						e.resolve(f);

						if (n != null &&!n.Equals(f.getName())) 
						{
							continue;
						}

						table = f.getTable();

						int col = table.getColumnCount();

						for (int c = 0; c < col; c++) 
						{
							Expression ins =
								new Expression(f.getName(),
								table.getColumnName(c));

							vcolumn.Insert(current++,ins);

							// now there is one element more to parse
							len++;
						}
					}

					Trace.check(table != null, Trace.TABLE_NOT_FOUND, n);

					// minus the asterix element
					len--;

					vcolumn.RemoveAt(current);
				}
				else if (e.getType()==Expression.COLUMN)
				{
					if (e.getTableName() == null) 
					{
						for (int filterIndex=0; filterIndex < filter.Length; filterIndex++) 
						{
							e.resolve(filter[filterIndex]);
						}
					}
				}
			}

			select.iResultLen = len;

			// where
			token = tTokenizer.getstring();

			if (token.Equals("WHERE")) 
			{
				condition = addCondition(condition, parseExpression());
				token = tTokenizer.getstring();
			}

			select.eCondition = condition;

			if (token.Equals("GROUP")) 
			{
				tTokenizer.getThis("BY");

				len = 0;

				do 
				{
					vcolumn.Add(parseExpression());

					token = tTokenizer.getstring();
					len++;
				} while (token.Equals(","));

				select.iGroupLen = len;
			}

			if (token.Equals("ORDER")) 
			{
				tTokenizer.getThis("BY");

				len = 0;

				do 
				{
					Expression e = parseExpression();

					if (e.getType() == Expression.VALUE) 
					{

						// order by 1,2,3
						if (e.getDataType() == Column.INTEGER) 
						{
							int i = ((int) e.getValue()).ToInt32();

							e = (Expression) vcolumn[i - 1];
						}
					} 
					else if (e.getType() == Expression.COLUMN
						&& e.getTableName() == null) 
					{

						// this could be an alias column
						string s = e.getColumnName();

						for (int i = 0; i < vcolumn.Count; i++) 
						{
							Expression ec = (Expression) vcolumn[i];

							if (s.Equals(ec.getAlias())) 
							{
								e = ec;

								break;
							}
						}
					}

					token = tTokenizer.getstring();

					if (token.Equals("DESC")) 
					{
						e.setDescending();

						token = tTokenizer.getstring();
					} 
					else if (token.Equals("ASC")) 
					{
						token = tTokenizer.getstring();
					}

					vcolumn.Add(e);

					len++;
				} while (token.Equals(","));

				select.iOrderLen = len;
			}

			len = vcolumn.Count;
			select.eColumn = new Expression[len];

			vcolumn.CopyTo(select.eColumn);

			if (token.Equals("UNION")) 
			{
				token = tTokenizer.getstring();

				if (token.Equals("ALL")) 
				{
					select.iUnionType = Select.UNIONALL;
				} 
				else 
				{
					select.iUnionType = Select.UNION;

					tTokenizer.back();
				}

				tTokenizer.getThis("SELECT");

				select.sUnion = parseSelect();
			} 
			else if (token.Equals("INTERSECT")) 
			{
				tTokenizer.getThis("SELECT");

				select.iUnionType = Select.INTERSECT;
				select.sUnion = parseSelect();
			} 
			else if (token.Equals("EXCEPT") || token.Equals("MINUS")) 
			{
				tTokenizer.getThis("SELECT");

				select.iUnionType = Select.EXCEPT;
				select.sUnion = parseSelect();
			} 
			else 
			{
				tTokenizer.back();
			}

			return select;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param outerjoin
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private TableFilter parseTableFilter(bool outerjoin)
		{
			string token = tTokenizer.getstring();
			Table  t = null;

			if (token.Equals("(")) 
			{
				tTokenizer.getThis("SELECT");

				Select s = parseSelect();
				Result r = s.getResult(0);

				// it's not a problem that this table has not a unique name
				t = new Table(dDatabase, false, "SYSTEM_SUBQUERY", false);

				tTokenizer.getThis(")");
				t.addColumns(r);
				t.createPrimaryKey();

				// subquery creation can't fail because of violation of primary key
				t.insert(r, cChannel);
			} 
			else 
			{
				cChannel.check(token, Access.SELECT);

				t = dDatabase.getTable(token, cChannel);
			}

			string sAlias = null;

			token = tTokenizer.getstring();

			if (token.Equals("AS")) 
			{
				sAlias = tTokenizer.getName();
			} 
			else if (tTokenizer.wasName()) 
			{
				sAlias = token;
			} 
			else 
			{
				tTokenizer.back();
			}

			return new TableFilter(t, sAlias, outerjoin);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param e1
		 * @param e2
		 *
		 * @return
		 */
		private Expression addCondition(Expression e1, Expression e2) 
		{
			if (e1 == null) 
			{
				return e2;
			} 
			else if (e2 == null) 
			{
				return e1;
			} 
			else 
			{
				return new Expression(Expression.AND, e1, e2);
			}
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
		private object getValue(int type) 
		{
			Expression r = parseExpression();

			r.resolve(null);

			return r.getValue(type);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Expression parseExpression() 
		{
			read();

			// todo: really this should be in readTerm
			// but then grouping is much more complex
			if (iToken == Expression.MIN || iToken == Expression.MAX
				|| iToken == Expression.COUNT || iToken == Expression.SUM
				|| iToken == Expression.AVG) 
			{
				int type = iToken;

				read();

				Expression r = new Expression(type, readOr(), null);

				tTokenizer.back();

				return r;
			}

			Expression rx = readOr();

			tTokenizer.back();

			return rx;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Expression readOr() 
		{
			Expression r = readAnd();

			while (iToken == Expression.OR) 
			{
				int	       type = iToken;
				Expression a = r;

				read();

				r = new Expression(type, a, readAnd());
			}

			return r;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Expression readAnd() 
		{
			Expression r = readCondition();

			while (iToken == Expression.AND) 
			{
				int	       type = iToken;
				Expression a = r;

				read();

				r = new Expression(type, a, readCondition());
			}

			return r;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Expression readCondition() 
		{
			if (iToken == Expression.NOT) 
			{
				int type = iToken;

				read();

				return new Expression(type, readCondition(), null);
			} 
			else if (iToken == Expression.EXISTS) 
			{
				int type = iToken;

				read();
				readThis(Expression.OPEN);
				Trace.check(iToken == Expression.SELECT, Trace.UNEXPECTED_TOKEN);

				Expression s = new Expression(parseSelect());

				read();
				readThis(Expression.CLOSE);

				return new Expression(type, s, null);
			} 
			else 
			{
				Expression a = readConcat();
				bool    not = false;

				if (iToken == Expression.NOT) 
				{
					not = true;

					read();
				}

				if (iToken == Expression.LIKE) 
				{
					read();

					Expression b = readConcat();
					char       escape = "0".ToChar();

					if (sToken.Equals("ESCAPE")) 
					{
						read();

						Expression c = readTerm();

						Trace.check(c.getType() == Expression.VALUE,
							Trace.INVALID_ESCAPE);

						string s = (string) c.getValue(Column.VARCHAR);

						if (s == null || s.Length < 1) 
						{
							throw Trace.error(Trace.INVALID_ESCAPE, s);
						}

						escape = s.Substring(0,1).ToChar();
					}

					a = new Expression(Expression.LIKE, a, b);

					a.setLikeEscape(escape);
				} 
				else if (iToken == Expression.BETWEEN) 
				{
					read();

					Expression l = new Expression(Expression.BIGGER_EQUAL, a,
						readConcat());

					readThis(Expression.AND);

					Expression h = new Expression(Expression.SMALLER_EQUAL, a,
						readConcat());

					a = new Expression(Expression.AND, l, h);
				} 
				else if (iToken == Expression.IN) 
				{
					int type = iToken;

					read();
					readThis(Expression.OPEN);

					Expression b = null;

					if (iToken == Expression.SELECT) 
					{
						b = new Expression(parseSelect());

						read();
					} 
					else 
					{
						tTokenizer.back();

						ArrayList v = new ArrayList();

						while (true) 
						{
							v.Add(getValue(Column.VARCHAR));
							read();

							if (iToken != Expression.COMMA) 
							{
								break;
							}
						}

						b = new Expression(v);
					}

					readThis(Expression.CLOSE);

					a = new Expression(type, a, b);
				} 
				else 
				{
					Trace.check(!not, Trace.UNEXPECTED_TOKEN);

					if (Expression.isCompare(iToken)) 
					{
						int type = iToken;

						read();

						return new Expression(type, a, readConcat());
					}

					return a;
				}

				if (not) 
				{
					a = new Expression(Expression.NOT, a, null);
				}

				return a;
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param type
		 *
		 * @throws Exception
		 */
		private void readThis(int type) 
		{
			Trace.check(iToken == type, Trace.UNEXPECTED_TOKEN);
			read();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Expression readConcat() 
		{
			Expression r = readSum();

			while (iToken == Expression.STRINGCONCAT) 
			{
				int	       type = Expression.CONCAT;
				Expression a = r;

				read();

				r = new Expression(type, a, readSum());
			}

			return r;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Expression readSum() 
		{
			Expression r = readFactor();

			while (true) 
			{
				int type;

				if (iToken == Expression.PLUS) 
				{
					type = Expression.ADD;
				} 
				else if (iToken == Expression.NEGATE) 
				{
					type = Expression.SUBTRACT;
				} 
				else 
				{
					break;
				}

				Expression a = r;

				read();

				r = new Expression(type, a, readFactor());
			}

			return r;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Expression readFactor() 
		{
			Expression r = readTerm();

			while (iToken == Expression.MULTIPLY || iToken == Expression.DIVIDE) 
			{
				int	       type = iToken;
				Expression a = r;

				read();

				r = new Expression(type, a, readTerm());
			}

			return r;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Expression readTerm() 
		{
			Expression r = null;

			if (iToken == Expression.COLUMN) 
			{
				string name = sToken;

				r = new Expression(sTable, sToken);

				read();

				/*				if (iToken == Expression.OPEN) 
								{
									Function f = new Function(dDatabase.getAlias(name), cChannel);
									int      len = f.getArgCount();
									int      i = 0;

									read();

									if (iToken != Expression.CLOSE) 
									{
										while (true) 
										{
											f.setArgument(i++, readOr());

											if (iToken != Expression.COMMA) 
											{
												break;
											}

											read();
										}
									}

									readThis(Expression.CLOSE);

									r = new Expression(f);
								} */
			}

			else if (iToken == Expression.NEGATE) 
			{
				int type = iToken;

				read();

				r = new Expression(type, readTerm(), null);
			} 
			else if (iToken == Expression.PLUS) 
			{
				read();

				r = readTerm();
			} 
			else if (iToken == Expression.OPEN) 
			{
				read();

				r = readOr();

				if (iToken != Expression.CLOSE) 
				{
					throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
				}

				read();
			} 
			else if (iToken == Expression.VALUE) 
			{
				r = new Expression(iType, oData);

				read();
			} 
			else if (iToken == Expression.SELECT) 
			{
				r = new Expression(parseSelect());

				read();
			} 
			else if (iToken == Expression.MULTIPLY) 
			{
				r = new Expression(sTable, null);

				read();
			} 
			else if (iToken == Expression.IFNULL
				|| iToken == Expression.CONCAT) 
			{
				int type = iToken;

				read();
				readThis(Expression.OPEN);

				r = readOr();

				readThis(Expression.COMMA);

				r = new Expression(type, r, readOr());

				readThis(Expression.CLOSE);
			} 
			else if (iToken == Expression.CASEWHEN) 
			{
				int type = iToken;

				read();
				readThis(Expression.OPEN);

				r = readOr();

				readThis(Expression.COMMA);

				Expression thenelse = readOr();

				readThis(Expression.COMMA);

				// thenelse part is never evaluated; only init
				thenelse = new Expression(type, thenelse, readOr());
				r = new Expression(type, r, thenelse);

				readThis(Expression.CLOSE);
			} 
			else if (iToken == Expression.CONVERT) 
			{
				int type = iToken;

				read();
				readThis(Expression.OPEN);

				r = readOr();

				readThis(Expression.COMMA);

				int t = Column.getTypeNr(sToken);

				r = new Expression(type, r, null);

				r.setDataType(t);
				read();
				readThis(Expression.CLOSE);
			} 
			else if (iToken == Expression.CAST) 
			{
				read();
				readThis(Expression.OPEN);

				r = readOr();

				Trace.check(sToken.Equals("AS"), Trace.UNEXPECTED_TOKEN, sToken);
				read();

				int t = Column.getTypeNr(sToken);

				r = new Expression(Expression.CONVERT, r, null);

				r.setDataType(t);
				read();
				readThis(Expression.CLOSE);
			} 
			else 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
			}

			return r;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void read() 
		{
			sToken = tTokenizer.getstring();

			if (tTokenizer.wasValue()) 
			{
				iToken = Expression.VALUE;
				oData = tTokenizer.getAsValue();
				iType = tTokenizer.getType();
			} 
			else if (tTokenizer.wasName()) 
			{
				iToken = Expression.COLUMN;
				sTable = null;
			} 
			else if (tTokenizer.wasLongName()) 
			{
				sTable = tTokenizer.getLongNameFirst();
				sToken = tTokenizer.getLongNameLast();

				if (sToken.Equals("*")) 
				{
					iToken = Expression.MULTIPLY;
				} 
				else 
				{
					iToken = Expression.COLUMN;
				}
			} 
			else if (sToken.Equals("")) 
			{
				iToken = Expression.END;
			} 
			else if (sToken.Equals("AND")) 
			{
				iToken = Expression.AND;
			} 
			else if (sToken.Equals("OR")) 
			{
				iToken = Expression.OR;
			} 
			else if (sToken.Equals("NOT")) 
			{
				iToken = Expression.NOT;
			} 
			else if (sToken.Equals("IN")) 
			{
				iToken = Expression.IN;
			} 
			else if (sToken.Equals("EXISTS")) 
			{
				iToken = Expression.EXISTS;
			} 
			else if (sToken.Equals("BETWEEN")) 
			{
				iToken = Expression.BETWEEN;
			} 
			else if (sToken.Equals("+")) 
			{
				iToken = Expression.PLUS;
			} 
			else if (sToken.Equals("-")) 
			{
				iToken = Expression.NEGATE;
			} 
			else if (sToken.Equals("*")) 
			{
				iToken = Expression.MULTIPLY;
				sTable = null;    // in case of ASTERIX
			} 
			else if (sToken.Equals("/")) 
			{
				iToken = Expression.DIVIDE;
			} 
			else if (sToken.Equals("||")) 
			{
				iToken = Expression.STRINGCONCAT;
			} 
			else if (sToken.Equals("(")) 
			{
				iToken = Expression.OPEN;
			} 
			else if (sToken.Equals(")")) 
			{
				iToken = Expression.CLOSE;
			} 
			else if (sToken.Equals("SELECT")) 
			{
				iToken = Expression.SELECT;
			} 
			else if (sToken.Equals("<")) 
			{
				iToken = Expression.SMALLER;
			} 
			else if (sToken.Equals("<=")) 
			{
				iToken = Expression.SMALLER_EQUAL;
			} 
			else if (sToken.Equals(">=")) 
			{
				iToken = Expression.BIGGER_EQUAL;
			} 
			else if (sToken.Equals(">")) 
			{
				iToken = Expression.BIGGER;
			} 
			else if (sToken.Equals("=")) 
			{
				iToken = Expression.EQUAL;
			} 
			else if (sToken.Equals("IS")) 
			{
				sToken = tTokenizer.getstring();

				if (sToken.Equals("NOT")) 
				{
					iToken = Expression.NOT_EQUAL;
				} 
				else 
				{
					iToken = Expression.EQUAL;

					tTokenizer.back();
				}
			} 
			else if (sToken.Equals("<>") || sToken.Equals("!=")) 
			{
				iToken = Expression.NOT_EQUAL;
			} 
			else if (sToken.Equals("LIKE")) 
			{
				iToken = Expression.LIKE;
			} 
			else if (sToken.Equals("COUNT")) 
			{
				iToken = Expression.COUNT;
			} 
			else if (sToken.Equals("SUM")) 
			{
				iToken = Expression.SUM;
			} 
			else if (sToken.Equals("MIN")) 
			{
				iToken = Expression.MIN;
			} 
			else if (sToken.Equals("MAX")) 
			{
				iToken = Expression.MAX;
			} 
			else if (sToken.Equals("AVG")) 
			{
				iToken = Expression.AVG;
			} 
			else if (sToken.Equals("IFNULL")) 
			{
				iToken = Expression.IFNULL;
			} 
			else if (sToken.Equals("CONVERT")) 
			{
				iToken = Expression.CONVERT;
			} 
			else if (sToken.Equals("CAST")) 
			{
				iToken = Expression.CAST;
			} 
			else if (sToken.Equals("CASEWHEN")) 
			{
				iToken = Expression.CASEWHEN;
			} 
			else if (sToken.Equals(",")) 
			{
				iToken = Expression.COMMA;
			} 
			else 
			{
				iToken = Expression.END;
			}
		}
	}
}
