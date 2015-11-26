/*
 * Channel.cs
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
	class Channel 
	{
		private Database	dDatabase;
		private User		uUser;
		private ArrayList   tTransaction;
		private bool		bAutoCommit;
		private bool		bNestedTransaction;
		private bool		bNestedOldAutoCommit;
		private int			iNestedOldTransIndex;
		private bool		bReadOnly;
		private int			iMaxRows;
		private int			iLastIdentity;
		private bool		bClosed;
		private int			iId;

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void finalize() 
		{
			disconnect();
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param c
		 * @param id
		 */
		public Channel(Channel c, int id) 
		{
			iId = id;
			dDatabase = c.dDatabase;
			uUser = c.uUser;
			tTransaction = new ArrayList();
			bAutoCommit = true;
			bReadOnly = c.bReadOnly;
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param db
		 * @param user
		 * @param autocommit
		 * @param readonly
		 * @param id
		 */
		public Channel(Database db, User user, bool autocommit, bool ReadOnly,
			int id) 
		{
			iId = id;
			dDatabase = db;
			uUser = user;
			tTransaction = new ArrayList();
			bAutoCommit = autocommit;
			bReadOnly = ReadOnly;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getId() 
		{
			return iId;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void disconnect() 
		{
			if (bClosed) 
			{
				return;
			}

			rollback();

			dDatabase = null;
			uUser = null;
			tTransaction = null;
			bClosed = true;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isClosed() 
		{
			return bClosed;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 */
		public void setLastIdentity(int i) 
		{
			iLastIdentity = i;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getLastIdentity() 
		{
			return iLastIdentity;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public Database getDatabase() 
		{
			return dDatabase;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public string getUsername() 
		{
			return uUser.getName();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param user
		 */
		public void setUser(User user) 
		{
			uUser = user;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void checkAdmin()
		{
			uUser.checkAdmin();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param object
		 * @param right
		 *
		 * @throws Exception
		 */
		public void check(string dbobject, int right)
		{
			uUser.check(dbobject, right);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void checkReadWrite() 
		{
			Trace.check(!bReadOnly, Trace.DATABASE_IS_READONLY);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 */
		public void setPassword(string s) 
		{
			uUser.setPassword(s);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param table
		 * @param row
		 *
		 * @throws Exception
		 */
		public void addTransactionDelete(Table table, object[] row) 
		{
			if (!bAutoCommit) 
			{
				Transaction t = new Transaction(true, table, row);

				tTransaction.Add(t);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param table
		 * @param row
		 *
		 * @throws Exception
		 */
		public void addTransactionInsert(Table table, object[] row) 
		{
			if (!bAutoCommit) 
			{
				Transaction t = new Transaction(false, table, row);

				tTransaction.Add(t);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param autocommit
		 *
		 * @throws Exception
		 */
		public void setAutoCommit(bool autocommit) 
		{
			commit();

			bAutoCommit = autocommit;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void commit() 
		{
			tTransaction.Clear();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void rollback() 
		{
			int i = tTransaction.Count - 1;

			while (i >= 0) 
			{
				Transaction t = (Transaction) tTransaction[i];

				t.rollback();

				i--;
			}

			tTransaction.Clear();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void beginNestedTransaction() 
		{
			Trace.assert(!bNestedTransaction, "beginNestedTransaction");

			bNestedOldAutoCommit = bAutoCommit;

			// now all transactions are logged
			bAutoCommit = false;
			iNestedOldTransIndex = tTransaction.Count;
			bNestedTransaction = true;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param rollback
		 *
		 * @throws Exception
		 */
		public void endNestedTransaction(bool rollback) 
		{
			Trace.assert(bNestedTransaction, "endNestedTransaction");

			int i = tTransaction.Count - 1;

			if (rollback) 
			{
				while (i >= iNestedOldTransIndex) 
				{
					Transaction t = (Transaction) tTransaction[i];

					t.rollback();

					i--;
				}
			}

			bNestedTransaction = false;
			bAutoCommit = bNestedOldAutoCommit;

			if (bAutoCommit == true) 
			{
				tTransaction.RemoveRange(iNestedOldTransIndex,(tTransaction.Count - iNestedOldTransIndex));
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param readonly
		 */
		public void setReadOnly(bool ReadOnly) 
		{
			bReadOnly = ReadOnly;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isReadOnly() 
		{
			return bReadOnly;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param max
		 */
		public void setMaxRows(int max) 
		{
			iMaxRows = max;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getMaxRows() 
		{
			return iMaxRows;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isNestedTransaction() 
		{
			return bNestedTransaction;
		}

	}
}
