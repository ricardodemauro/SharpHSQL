/*
 * Database.cs
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
	 * Database class declaration
	 * <P>Database is the root class for HSQL Database Engine database.
	 * This class should not be used directly by the application,
	 * instead the jdbc* classes should be used.
	 *
	 * @version 1.0.0.1
	 */
	class Database 
	{
		private string				sName;
		private Access				aAccess;
		private ArrayList			tTable;
		private DatabaseInformation dInfo;
		private Log					lLog;
		private bool				bReadOnly;
		private bool				bShutdown;
		private Hashtable			hAlias;
		private bool				bIgnoreCase;
		private bool				bReferentialIntegrity;
		private ArrayList			cChannel;

		/**
		 * Constructor declaration
		 *
		 *
		 * @param name
		 */
		public Database(string name) 
		{
			if (Trace.TRACE) 
			{
				Trace.trace();
			}

			sName = name;
			tTable = new ArrayList();
			aAccess = new Access();
			cChannel = new ArrayList();
			hAlias = new Hashtable();
			bReferentialIntegrity = true;

//			Library.register(hAlias);

			dInfo = new DatabaseInformation(this, tTable, aAccess);

			bool newdatabase = false;
			Channel sys = new Channel(this, new User(null, null, true, null),
				true, false, 0);

			registerChannel(sys);

			if (name.Equals(".")) 
			{
				newdatabase = true;
			} 
			else 
			{
				lLog = new Log(this, sys, name);
				newdatabase = lLog.open();
			}

			if (newdatabase) 
			{
				execute("CREATE USER SA PASSWORD \"\" ADMIN", sys);
			}

//			aAccess.grant("PUBLIC", "CLASS \"org.hsqldb.Library\"", Access.ALL);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public string getName() 
		{
			return sName;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isShutdown() 
		{
			return bShutdown;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param username
		 * @param password
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Channel connect(string username,
			string password) 
		{
			User user = aAccess.getUser(username.ToUpper(),
				password.ToUpper());
			int  size = cChannel.Count, id = size;

			for (int i = 0; i < size; i++) 
			{
				if (cChannel[i] == null) 
				{
					id = i;

					break;
				}
			}

			Channel c = new Channel(this, user, true, bReadOnly, id);

			if (lLog != null) 
			{
				lLog.write(c,
					"CONNECT USER " + username + " PASSWORD \"" + password
					+ "\"");
			}

			registerChannel(c);

			return c;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param channel
		 */
		public void registerChannel(Channel channel) 
		{
			int size = cChannel.Count;
			int id = channel.getId();

			if (id >= size) 
			{
				cChannel.Add(channel);
			}

			cChannel.RemoveAt(id);
			cChannel.Insert(id,channel);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param user
		 * @param password
		 * @param statement
		 *
		 * @return
		 */
		public byte[] execute(string user, string password, string statement) 
		{
			Result r = null;

			try 
			{
				Channel channel = connect(user, password);

				r = execute(statement, channel);

				execute("DISCONNECT", channel);
			} 
			catch (Exception e) 
			{
				r = new Result(e.Message);
			}

			try 
			{
				return r.getBytes();
			} 
			catch (Exception e) 
			{
				return new byte[0];
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param statement
		 * @param channel
		 *
		 * @return
		 */
		public Result execute(string statement, Channel channel) 
		{
			if (Trace.TRACE) 
			{
				Trace.trace(statement);
			}

			Tokenizer c = new Tokenizer(statement);
			Parser    p = new Parser(this, c, channel);
			Result    rResult = new Result();

			try 
			{
				if (lLog != null && lLog.cCache != null) 
				{
					lLog.cCache.cleanUp();
				}

				if (Trace.ASSERT) 
				{
					Trace.assert(!channel.isNestedTransaction());
				}

				Trace.check(channel != null, Trace.ACCESS_IS_DENIED);
				Trace.check(!bShutdown, Trace.DATABASE_IS_SHUTDOWN);

				while (true) 
				{
					int     begin = c.getPosition();
					bool script = false;
					string  sToken = c.getstring();

					if (sToken.Equals("")) 
					{
						break;
					} 
					else if (sToken.Equals("SELECT")) 
					{
						rResult = p.processSelect();
					} 
					else if (sToken.Equals("INSERT")) 
					{
						rResult = p.processInsert();
					} 
					else if (sToken.Equals("UPDATE")) 
					{
						rResult = p.processUpdate();
					} 
					else if (sToken.Equals("DELETE")) 
					{
						rResult = p.processDelete();
					} 
					else if(sToken.Equals("ALTER")) 
					{
						rResult=p.processAlter();
					} 
					else if (sToken.Equals("CREATE")) 
					{
						rResult = processCreate(c, channel);
						script = true;
					} 
					else if (sToken.Equals("DROP")) 
					{
						rResult = processDrop(c, channel);
						script = true;
					} 
					else if (sToken.Equals("GRANT")) 
					{
						rResult = processGrantOrRevoke(c, channel, true);
						script = true;
					} 
					else if (sToken.Equals("REVOKE")) 
					{
						rResult = processGrantOrRevoke(c, channel, false);
						script = true;
					} 
					else if (sToken.Equals("CONNECT")) 
					{
						rResult = processConnect(c, channel);
					} 
					else if (sToken.Equals("DISCONNECT")) 
					{
						rResult = processDisconnect(c, channel);
					} 
					else if (sToken.Equals("SET")) 
					{
						rResult = processSet(c, channel);
						script = true;
					} 
					else if (sToken.Equals("SCRIPT")) 
					{
						rResult = processScript(c, channel);
					} 
					else if (sToken.Equals("COMMIT")) 
					{
						rResult = processCommit(c, channel);
						script = true;
					} 
					else if (sToken.Equals("ROLLBACK")) 
					{
						rResult = processRollback(c, channel);
						script = true;
					} 
					else if (sToken.Equals("SHUTDOWN")) 
					{
						rResult = processShutdown(c, channel);
					} 
					else if (sToken.Equals("CHECKPOINT")) 
					{
						rResult = processCheckpoint(channel);
					} 
					else if (sToken.Equals("CALL")) 
					{
						rResult = p.processCall();
					} 
					else if (sToken.Equals(";")) 
					{

						// ignore
					} 
					else 
					{
						throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
					}

					if (script && lLog != null) 
					{
						int end = c.getPosition();

						lLog.write(channel, c.getPart(begin, end));
					}
				}
			} 
			catch (Exception e) 
			{
				rResult = new Result(Trace.getMessage(e) + " in statement ["
					+ statement + "]");
			} 

			return rResult;
		}

		/**
		 * Method declaration
		 *
		 */
		public void setReadOnly() 
		{
			bReadOnly = true;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public ArrayList getTables() 
		{
			return tTable;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param ref
		 */
		public void setReferentialIntegrity(bool refint) 
		{
			bReferentialIntegrity = refint;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isReferentialIntegrity() 
		{
			return bReferentialIntegrity;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public Hashtable getAlias() 
		{
			return hAlias;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public string getAlias(string s) 
		{
			object o = hAlias[s];

			if (o == null) 
			{
				return s;
			}

			return (string) o;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public Log getLog() 
		{
			return lLog;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param name
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Table getTable(string name, Channel channel) 
		{
			Table t = null;

			for (int i = 0; i < tTable.Count; i++) 
			{
				t = (Table) tTable[i];

				if (t.getName().Equals(name)) 
				{
					return t;
				}
			}

			t = dInfo.getSystemTable(name, channel);

			if (t == null) 
			{
				throw Trace.error(Trace.TABLE_NOT_FOUND, name);
			}

			return t;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param drop
		 * @param insert
		 * @param cached
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Result getScript(bool drop, bool insert, bool cached,
			Channel channel) 
		{
			return dInfo.getScript(drop, insert, cached, channel);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param t
		 *
		 * @throws Exception
		 */
		public void linkTable(Table t) 
		{
			string name = t.getName();

			for (int i = 0; i < tTable.Count; i++) 
			{
				Table o = (Table) tTable[i];

				if (o.getName().Equals(name)) 
				{
					throw Trace.error(Trace.TABLE_ALREADY_EXISTS, name);
				}
			}

			tTable.Add(t);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isIgnoreCase() 
		{
			return bIgnoreCase;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processScript(Tokenizer c,
			Channel channel) 
		{
			string sToken = c.getstring();

			if (c.wasValue()) 
			{
				sToken = (string) c.getAsValue();

				Log.scriptToFile(this, sToken, true, channel);

				return new Result();
			} 
			else 
			{
				c.back();

				// try to script all: drop, insert; but no positions for cached tables
				return getScript(true, true, false, channel);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processCreate(Tokenizer c,
			Channel channel) 
		{
			channel.checkReadWrite();
			channel.checkAdmin();

			string sToken = c.getstring();

			if (sToken.Equals("TABLE")) 
			{
				processCreateTable(c, channel, false);
			} 
			else if (sToken.Equals("MEMORY")) 
			{
				c.getThis("TABLE");
				processCreateTable(c, channel, false);
			} 
			else if (sToken.Equals("CACHED")) 
			{
				c.getThis("TABLE");
				processCreateTable(c, channel, true);
			} 
			else if (sToken.Equals("USER")) 
			{
				string u = c.getstringToken();

				c.getThis("PASSWORD");

				string  p = c.getstringToken();
				bool admin;

				if (c.getstring().Equals("ADMIN")) 
				{
					admin = true;
				} 
				else 
				{
					admin = false;
				}

				aAccess.createUser(u, p, admin);
			} 
			else if (sToken.Equals("ALIAS")) 
			{
				string name = c.getstring();

				sToken = c.getstring();

				Trace.check(sToken.Equals("FOR"), Trace.UNEXPECTED_TOKEN, sToken);

				sToken = c.getstring();

				hAlias.Add(name, sToken);
			} 
			else 
			{
				bool unique = false;

				if (sToken.Equals("UNIQUE")) 
				{
					unique = true;
					sToken = c.getstring();
				}

				if (!sToken.Equals("INDEX")) 
				{
					throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
				}

				string name = c.getName();

				c.getThis("ON");

				Table t = getTable(c.getstring(), channel);

				addIndexOn(c, channel, name, t, unique);
			}

			return new Result();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param t
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private int[] processColumnList(Tokenizer c,
			Table t) 
		{
			ArrayList v = new ArrayList();

			c.getThis("(");

			while (true) 
			{
				v.Add(c.getstring());

				string sToken = c.getstring();

				if (sToken.Equals(")")) 
				{
					break;
				}

				if (!sToken.Equals(",")) 
				{
					throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
				}
			}

			int s = v.Count;
			int[] col = new int[s];

			for (int i = 0; i < s; i++) 
			{
				col[i] = t.getColumnNr((string) v[i]);
			}

			return col;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param channel
		 * @param t
		 * @param col
		 * @param name
		 * @param unique
		 *
		 * @throws Exception
		 */
		private void createIndex(Channel channel, Table t, int[] col,
			string name,
			bool unique) 
		{
			channel.commit();

			if (t.isEmpty()) 
			{
				t.createIndex(col, name, unique);
			} 
			else 
			{
				Table tn = t.moveDefinition(null);

				tn.createIndex(col, name, unique);
				tn.moveData(t);
				dropTable(t.getName());
				linkTable(tn);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 * @param name
		 * @param t
		 *
		 * @throws Exception
		 */
		private void addForeignKeyOn(Tokenizer c, Channel channel, string name,
			Table t) 
		{
			int[] col = processColumnList(c, t);

			c.getThis("REFERENCES");

			Table t2 = getTable(c.getstring(), channel);
			int[]   col2 = processColumnList(c, t2);

			if (t.getIndexForColumns(col) == null) 
			{
				createIndex(channel, t, col, "SYSTEM_FOREIGN_KEY_" + name, false);
			}

			if (t2.getIndexForColumns(col2) == null) 
			{
				createIndex(channel, t2, col2, "SYSTEM_REFERENCE_" + name, false);
			}

			t.addConstraint(new Constraint(Constraint.FOREIGN_KEY, t2, t, col2,
				col));
			t2.addConstraint(new Constraint(Constraint.MAIN, t2, t, col2, col));
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 * @param name
		 * @param t
		 *
		 * @throws Exception
		 */
		private void addUniqueConstraintOn(Tokenizer c, Channel channel,
			string name,
			Table t) 
		{
			int[] col = processColumnList(c, t);

			createIndex(channel, t, col, name, true);
			t.addConstraint(new Constraint(Constraint.UNIQUE, t, col));
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 * @param name
		 * @param t
		 * @param unique
		 *
		 * @throws Exception
		 */
		private void addIndexOn(Tokenizer c, Channel channel, string name,
			Table t, bool unique) 
		{
			int[] col = processColumnList(c, t);

			createIndex(channel, t, col, name, unique);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 * @param cached
		 *
		 * @throws Exception
		 */
		private void processCreateTable(Tokenizer c, Channel channel,
			bool cached) 
		{
			Table  t;
			string sToken = c.getName();

			if (cached && lLog != null) 
			{
				t = new Table(this, true, sToken, true);
			} 
			else 
			{
				t = new Table(this, true, sToken, false);
			}

			c.getThis("(");

			int     primarykeycolumn = -1;
			int     column = 0;
			bool constraint = false;

			while (true) 
			{
				bool identity = false;

				sToken = c.getstring();

				if (sToken.Equals("CONSTRAINT") || sToken.Equals("PRIMARY")
					|| sToken.Equals("FOREIGN") || sToken.Equals("UNIQUE")) 
				{
					c.back();

					constraint = true;

					break;
				}

				string sColumn = sToken;
				int    iType = Column.getTypeNr(c.getstring());

				if (iType == Column.VARCHAR && bIgnoreCase) 
				{
					iType = Column.VARCHAR_IGNORECASE;
				}

				sToken = c.getstring();

				if (iType == Column.DOUBLE && sToken.Equals("PRECISION")) 
				{
					sToken = c.getstring();
				}

				if (sToken.Equals("(")) 
				{

					// overread length
					do 
					{
						sToken = c.getstring();
					} while (!sToken.Equals(")"));

					sToken = c.getstring();
				}

				bool nullable = true;

				if (sToken.Equals("NULL")) 
				{
					sToken = c.getstring();
				} 
				else if (sToken.Equals("NOT")) 
				{
					c.getThis("NULL");

					nullable = false;
					sToken = c.getstring();
				}

				if (sToken.Equals("IDENTITY")) 
				{
					identity = true;

					Trace.check(primarykeycolumn == -1, Trace.SECOND_PRIMARY_KEY,
						sColumn);

					sToken = c.getstring();
					primarykeycolumn = column;
				}

				if (sToken.Equals("PRIMARY")) 
				{
					c.getThis("KEY");
					Trace.check(identity || primarykeycolumn == -1,
						Trace.SECOND_PRIMARY_KEY, sColumn);

					primarykeycolumn = column;
					sToken = c.getstring();
				}

				t.addColumn(sColumn, iType, nullable, identity);

				if (sToken.Equals(")")) 
				{
					break;
				}

				if (!sToken.Equals(",")) 
				{
					throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
				}

				column++;
			}

			if (primarykeycolumn != -1) 
			{
				t.createPrimaryKey(primarykeycolumn);
			} 
			else 
			{
				t.createPrimaryKey();
			}

			if (constraint) 
			{
				int i = 0;

				while (true) 
				{
					sToken = c.getstring();

					string name = "SYSTEM_CONSTRAINT" + i;

					i++;

					if (sToken.Equals("CONSTRAINT")) 
					{
						name = c.getstring();
						sToken = c.getstring();
					}

					if (sToken.Equals("PRIMARY")) 
					{
						c.getThis("KEY");
						addUniqueConstraintOn(c, channel, name, t);
					} 
					else if (sToken.Equals("UNIQUE")) 
					{
						addUniqueConstraintOn(c, channel, name, t);
					} 
					else if (sToken.Equals("FOREIGN")) 
					{
						c.getThis("KEY");
						addForeignKeyOn(c, channel, name, t);
					}

					sToken = c.getstring();

					if (sToken.Equals(")")) 
					{
						break;
					}

					if (!sToken.Equals(",")) 
					{
						throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
					}
				}
			}

			channel.commit();
			linkTable(t);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processDrop(Tokenizer c,
			Channel channel) 
		{
			channel.checkReadWrite();
			channel.checkAdmin();

			string sToken = c.getstring();

			if (sToken.Equals("TABLE")) 
			{
				sToken = c.getstring();

				if (sToken.Equals("IF")) 
				{
					sToken = c.getstring();    // EXISTS
					sToken = c.getstring();    // <table>

					dropTable(sToken, true);
				} 
				else 
				{
					dropTable(sToken, false);
				}
				channel.commit();
			} 
			else if (sToken.Equals("USER")) 
			{
				aAccess.dropUser(c.getstringToken());
			} 
			else if (sToken.Equals("INDEX")) 
			{
				sToken = c.getstring();

				if (!c.wasLongName()) 
				{
					throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
				}

				string table = c.getLongNameFirst();
				string index = c.getLongNameLast();
				Table  t = getTable(table, channel);

				t.checkDropIndex(index);

				Table tn = t.moveDefinition(index);

				tn.moveData(t);
				dropTable(table);
				linkTable(tn);
				channel.commit();
			} 
			else 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
			}

			return new Result();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 * @param grant
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processGrantOrRevoke(Tokenizer c, Channel channel,
			bool grant) 
		{
			channel.checkReadWrite();
			channel.checkAdmin();

			int    right = 0;
			string sToken;

			do 
			{
				string sRight = c.getstring();

				right |= Access.getRight(sRight);
				sToken = c.getstring();
			} while (sToken.Equals(","));

			if (!sToken.Equals("ON")) 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
			}

			string table = c.getstring();

			if (table.Equals("CLASS")) 
			{

				// object is saved as 'CLASS "java.lang.Math"'
				// tables like 'CLASS "xy"' should not be created
				table += " \"" + c.getstring() + "\"";
			} 
			else 
			{
				getTable(table, channel);    // to make sure the table exists
			}

			c.getThis("TO");

			string user = c.getstringToken();
			//			string command;

			if (grant) 
			{
				aAccess.grant(user, table, right);

				//				command = "GRANT";
			} 
			else 
			{
				aAccess.revoke(user, table, right);

				//				command = "REVOKE";
			}

			return new Result();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processConnect(Tokenizer c,
			Channel channel) 
		{
			c.getThis("USER");

			string username = c.getstringToken();

			c.getThis("PASSWORD");

			string password = c.getstringToken();
			User   user = aAccess.getUser(username, password);

			channel.commit();
			channel.setUser(user);

			return new Result();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processDisconnect(Tokenizer c,
			Channel channel) 
		{
			if (!channel.isClosed()) 
			{
				channel.disconnect();
				cChannel.Insert(channel.getId(),null);
			}

			return new Result();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processSet(Tokenizer c,
			Channel channel) 
		{
			string sToken = c.getstring();

			if (sToken.Equals("PASSWORD")) 
			{
				channel.checkReadWrite();
				channel.setPassword(c.getstringToken());
			} 
			else if (sToken.Equals("READONLY")) 
			{
				channel.commit();
				channel.setReadOnly(processTrueOrFalse(c));
			} 
			else if (sToken.Equals("LOGSIZE")) 
			{
				channel.checkAdmin();

				int i = Int32.FromString(c.getstring());

				if (lLog != null) 
				{
					lLog.setLogSize(i);
				}
			} 
			else if (sToken.Equals("IGNORECASE")) 
			{
				channel.checkAdmin();

				bIgnoreCase = processTrueOrFalse(c);
			} 
			else if (sToken.Equals("MAXROWS")) 
			{
				int i = Int32.FromString(c.getstring());

				channel.setMaxRows(i);
			} 
			else if (sToken.Equals("AUTOCOMMIT")) 
			{
				channel.setAutoCommit(processTrueOrFalse(c));
			} 
			else if (sToken.Equals("TABLE")) 
			{
				channel.checkReadWrite();
				channel.checkAdmin();

				Table t = getTable(c.getstring(), channel);

				c.getThis("INDEX");
				c.getstring();
				t.setIndexRoots((string) c.getAsValue());
			} 
			else if (sToken.Equals("REFERENCIAL_INTEGRITY")
				|| sToken.Equals("REFERENTIAL_INTEGRITY")) 
			{
				channel.checkAdmin();

				bReferentialIntegrity = processTrueOrFalse(c);
			} 
			else if (sToken.Equals("WRITE_DELAY")) 
			{
				channel.checkAdmin();

				bool delay = processTrueOrFalse(c);

				if (lLog != null) 
				{
					lLog.setWriteDelay(delay);
				}
			} 
			else 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
			}

			return new Result();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private bool processTrueOrFalse(Tokenizer c) 
		{
			string sToken = c.getstring();

			if (sToken.Equals("TRUE")) 
			{
				return true;
			} 
			else if (sToken.Equals("FALSE")) 
			{
				return false;
			} 
			else 
			{
				throw Trace.error(Trace.UNEXPECTED_TOKEN, sToken);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processCommit(Tokenizer c,
			Channel channel) 
		{
			string sToken = c.getstring();

			if (!sToken.Equals("WORK")) 
			{
				c.back();
			}

			channel.commit();

			return new Result();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processRollback(Tokenizer c,
			Channel channel) 
		{
			string sToken = c.getstring();

			if (!sToken.Equals("WORK")) 
			{
				c.back();
			}

			channel.rollback();

			return new Result();
		}

		/**
		 * Method declaration
		 *
		 */
		public void finalize() 
		{
			try 
			{
				close(0);
			} 
			catch (Exception e) 
			{
				// it's too late now
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
		private void close(int type) 
		{
			if (lLog == null) 
			{
				return;
			}

			lLog.stop();

			if (type == -1) 
			{
				lLog.shutdown();
			} 
			else if (type == 0) 
			{
				lLog.close(false);
			} 
			else if (type == 1) 
			{
				lLog.close(true);
			}

			lLog = null;
			bShutdown = true;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processShutdown(Tokenizer c,
			Channel channel) 
		{
			channel.checkAdmin();

			// don't disconnect system user; need it to save database
			for (int i = 1; i < cChannel.Count; i++) 
			{
				Channel d = (Channel) cChannel[i];

				if (d != null) 
				{
					d.disconnect();
				}
			}

			cChannel.Clear();

			string token = c.getstring();

			if (token.Equals("IMMEDIATELY")) 
			{
				close(-1);
			} 
			else if (token.Equals("COMPACT")) 
			{
				close(1);
			} 
			else 
			{
				c.back();
				close(0);
			}

			processDisconnect(c, channel);

			return new Result();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param channel
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private Result processCheckpoint(Channel channel) 
		{
			channel.checkAdmin();

			if (lLog != null) 
			{
				lLog.checkpoint();
			}

			return new Result();
		}

		/**
		/**
		 * Method declaration
		 *
		 *
		 * @param name
		 *
		 * @throws Exception
		 */
		private void dropTable(string name) 
		{
			dropTable(name, false);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param name
		 * @param bExists
		 *
		 * @throws Exception
		 */
		private void dropTable(string name, bool bExists) 
		{
			for (int i = 0; i < tTable.Count; i++) 
			{
				Table o = (Table) tTable[i];

				if (o.getName().Equals(name)) 
				{
					tTable.RemoveAt(i);

					return;
				}
			}

			if (!bExists) 
			{
				throw Trace.error(Trace.TABLE_NOT_FOUND, name);
			}
		}

	}
}
