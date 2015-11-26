/*
 * Table.cs
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
	using System.Text;
	using System.Collections;

	/**
	 * Class declaration
	 *
	 *
	 * @version 1.0.0.1
	 */
	class Table 
	{
		private string   sName;
		private ArrayList   vColumn;
		private ArrayList   vIndex;		 // vIndex(0) is always the primary key index
		private int      iVisibleColumns;    // table may contain a hidden primary key
		private int      iColumnCount;    // inclusive the (maybe hidden) primary key
		private int      iPrimaryKey;
		private bool  bCached;
		private Database dDatabase;
		private Log      lLog;
		private int      iIndexCount;
		private int      iIdentityColumn;    // -1 means no such row
		private int      iIdentityId;
		private ArrayList   vConstraint;
		private int      iConstraintCount;
		public Cache	     cCache;

		/**
		 * Constructor declaration
		 *
		 *
		 * @param db
		 * @param log
		 * @param name
		 * @param cached
		 */
		public Table(Database db, bool log, string name, bool cached) 
		{
			dDatabase = db;
			lLog = log ? db.getLog() : null;

			if (cached) 
			{
				cCache = lLog.cCache;
				bCached = true;
			}

			sName = name;
			iPrimaryKey = -1;
			iIdentityColumn = -1;
			vColumn = new ArrayList();
			vIndex = new ArrayList();
			vConstraint = new ArrayList();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 */
		public void addConstraint(Constraint c) 
		{
			vConstraint.Add(c);

			iConstraintCount++;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public ArrayList getConstraints() 
		{
			return vConstraint;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param name
		 * @param type
		 *
		 * @throws Exception
		 */
		public void addColumn(string name, int type) 
		{
			addColumn(name, type, true, false);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 *
		 * @throws Exception
		 */
		public void addColumn(Column c) 
		{
			addColumn(c.sName, c.iType, c.isNullable(), c.isIdentity());
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param name
		 * @param type
		 * @param nullable
		 * @param identity
		 *
		 * @throws Exception
		 */
		public void addColumn(string name, int type, bool nullable,
			bool identity) 
		{
			if (identity) 
			{
				Trace.check(type == Column.INTEGER, Trace.WRONG_DATA_TYPE, name);
				Trace.check(iIdentityColumn == -1, Trace.SECOND_PRIMARY_KEY,
					name);

				iIdentityColumn = iColumnCount;
			}

			Trace.assert(iPrimaryKey == -1, "Table.addColumn");
			vColumn.Add(new Column(name, nullable, type, identity));

			iColumnCount++;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param result
		 *
		 * @throws Exception
		 */
		public void addColumns(Result result) 
		{
			for (int i = 0; i < result.getColumnCount(); i++) 
			{
				addColumn(result.sLabel[i], result.iType[i], true, false);
			}
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
		public int getInternalColumnCount() 
		{

			// todo: this is a temporary solution;
			// the the hidden column is not really required
			return iColumnCount;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param withoutindex
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Table moveDefinition(string withoutindex) 
		{
			Table tn = new Table(dDatabase, true, getName(), isCached());

			for (int i = 0; i < getColumnCount(); i++) 
			{
				tn.addColumn(getColumn(i));
			}

			// todo: there should be nothing special with the primary key!
			if (iVisibleColumns < iColumnCount) 
			{
				tn.createPrimaryKey();
			} 
			else 
			{
				tn.createPrimaryKey(getPrimaryIndex().getColumns()[0]);
			}

			Index idx = null;

			while (true) 
			{
				idx = getNextIndex(idx);

				if (idx == null) 
				{
					break;
				}

				if (withoutindex != null && idx.getName().Equals(withoutindex)) 
				{
					continue;
				}

				if (idx == getPrimaryIndex()) 
				{
					continue;
				}

				tn.createIndex(idx);
			}

			for (int i = 0; i < iConstraintCount; i++) 
			{
				Constraint c = (Constraint) vConstraint[i];

				c.replaceTable(this, tn);
			}

			tn.vConstraint = vConstraint;

			return tn;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getColumnCount() 
		{
			return iVisibleColumns;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getIndexCount() 
		{
			return iIndexCount;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getIdentityColumn() 
		{
			return iIdentityColumn;
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
		public int getColumnNr(string c) 
		{
			int i = searchColumn(c);

			if (i == -1) 
			{
				throw Trace.error(Trace.COLUMN_NOT_FOUND, c);
			}

			return i;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 *
		 * @return
		 */
		public int searchColumn(string c) 
		{
			for (int i = 0; i < iColumnCount; i++) 
			{
				if (c.Equals(((Column) vColumn[i]).sName)) 
				{
					return i;
				}
			}

			return -1;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 *
		 * @return
		 */
		public string getColumnName(int i) 
		{
			return getColumn(i).sName;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 *
		 * @return
		 */
		public int getColumnType(int i) 
		{
			return getColumn(i).iType;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 *
		 * @return
		 */
		public bool getColumnIsNullable(int i) 
		{
			return getColumn(i).isNullable();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Index getPrimaryIndex() 
		{
			if (iPrimaryKey == -1) 
			{
				return null;
			}

			return getIndex(0);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param column
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Index getIndexForColumn(int column) 
		{
			for (int i = 0; i < iIndexCount; i++) 
			{
				Index h = getIndex(i);

				if (h.getColumns()[0] == column) 
				{
					return h;
				}
			}

			return null;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param col
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Index getIndexForColumns(int[] col) 
		{
			for (int i = 0; i < iIndexCount; i++) 
			{
				Index h = getIndex(i);
				int[]   icol = h.getColumns();
				int   j = 0;

				for (; j < col.Length; j++) 
				{
					if (j >= icol.Length) 
					{
						break;
					}

					if (icol[j] != col[j]) 
					{
						break;
					}
				}

				if (j == col.Length) 
				{
					return h;
				}
			}

			return null;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public string getIndexRoots() 
		{
			Trace.assert(bCached, "Table.getIndexRootData");

			string s = "";

			for (int i = 0; i < iIndexCount; i++) 
			{
				Node f = getIndex(i).getRoot();

				if (f != null) 
				{
					s = s + f.getKey() + " ";
				} 
				else 
				{
					s = s + "-1 ";
				}
			}

			s += iIdentityId;

			return s;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @throws Exception
		 */
		public void setIndexRoots(string s) 
		{

			// the user may try to set this; this is not only internal problem
			Trace.check(bCached, Trace.TABLE_NOT_FOUND);

			int j = 0;

			for (int i = 0; i < iIndexCount; i++) 
			{
				int n = s.IndexOf(' ', j);
				int p = s.Substring(j, n).ToInt32();

				if (p != -1) 
				{
					Row  r = cCache.getRow(p, this);
					Node f = r.getNode(i);

					getIndex(i).setRoot(f);
				}

				j = n + 1;
			}

			iIdentityId = s.Substring(j).ToInt32();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param index
		 *
		 * @return
		 */
		public Index getNextIndex(Index index) 
		{
			int i = 0;

			if (index != null) 
			{
				for (; i < iIndexCount && getIndex(i) != index; i++);

				i++;
			}

			if (i < iIndexCount) 
			{
				return getIndex(i);
			}

			return null;    // no more indexes
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 *
		 * @return
		 */
		public int getType(int i) 
		{
			return getColumn(i).iType;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param column
		 *
		 * @throws Exception
		 */
		public void createPrimaryKey(int column) 
		{
			Trace.assert(iPrimaryKey == -1, "Table.createPrimaryKey(column)");

			iVisibleColumns = iColumnCount;
			iPrimaryKey = column;

			int[] col = new int[1];
			col[0] = column;

			createIndex(col, "SYSTEM_PK", true);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void createPrimaryKey() 
		{
			Trace.assert(iPrimaryKey == -1, "Table.createPrimaryKey");
			addColumn("SYSTEM_ID", Column.INTEGER, true, true);
			createPrimaryKey(iColumnCount - 1);

			iVisibleColumns = iColumnCount - 1;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param index
		 *
		 * @throws Exception
		 */
		public void createIndex(Index index) 
		{
			createIndex(index.getColumns(), index.getName(), index.isUnique());
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param column
		 * @param name
		 * @param unique
		 *
		 * @throws Exception
		 */
		public void createIndex(int[] column, string name,
			bool unique) 
		{
			Trace.assert(iPrimaryKey != -1, "createIndex");

			for (int i = 0; i < iIndexCount; i++) 
			{
				Index index = getIndex(i);

				if (name.Equals(index.getName())) 
				{
					throw Trace.error(Trace.INDEX_ALREADY_EXISTS);
				}
			}

			int s = column.Length;

			// The primary key field is added for non-unique indexes
			// making all indexes unique
			int[] col = new int[unique ? s : s + 1];
			int[] type = new int[unique ? s : s + 1];

			for (int j = 0; j < s; j++) 
			{
				col[j] = column[j];
				type[j] = getColumn(col[j]).iType;
			}

			if (!unique) 
			{
				col[s] = iPrimaryKey;
				type[s] = getColumn(iPrimaryKey).iType;
			}

			Index newindex = new Index(name, col, type, unique);

			if (iIndexCount != 0) 
			{
				Trace.assert(isEmpty(), "createIndex");
			}

			vIndex.Add(newindex);

			iIndexCount++;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param index
		 *
		 * @throws Exception
		 */
		public void checkDropIndex(string index) 
		{
			for (int i = 0; i < iIndexCount; i++) 
			{
				if (index.Equals(getIndex(i).getName())) 
				{
					Trace.check(i != 0, Trace.DROP_PRIMARY_KEY);

					return;
				}
			}

			throw Trace.error(Trace.INDEX_NOT_FOUND, index);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isEmpty() 
		{
			return getIndex(0).getRoot() == null;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public object[] getNewRow()
		{
			return new object[iColumnCount];
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param from
		 *
		 * @throws Exception
		 */
		public void moveData(Table from) 
		{
			Index index = from.getPrimaryIndex();
			Node  n = index.first();

			while (n != null) 
			{
				if (Trace.STOP) 
				{
					Trace.stop();
				}

				object[] o = n.getData();

				insertNoCheck(o, null);

				n = index.next(n);
			}

			index = getPrimaryIndex();
			n = index.first();

			while (n != null) 
			{
				if (Trace.STOP) 
				{
					Trace.stop();
				}

				object[] o = n.getData();

				from.deleteNoCheck(o, null);

				n = index.next(n);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param col
		 * @param deleted
		 * @param inserted
		 *
		 * @throws Exception
		 */
		public void checkUpdate(int[] col, Result deleted,
			Result inserted) 
		{
			if (dDatabase.isReferentialIntegrity()) 
			{
				for (int i = 0; i < iConstraintCount; i++) 
				{
					Constraint v = (Constraint) vConstraint[i];

					v.checkUpdate(col, deleted, inserted);
				}
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param result
		 * @param c
		 *
		 * @throws Exception
		 */
		public void insert(Result result, Channel c) 
		{

			// if violation of constraints can occur, insert must be rolled back
			// outside of this function!
			Record r = result.rRoot;
			int    len = result.getColumnCount();

			while (r != null) 
			{
				object[] row = getNewRow();

				for (int i = 0; i < len; i++) 
				{
					row[i] = r.data[i];
				}

				insert(row, c);

				r = r.next;
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param row
		 * @param c
		 *
		 * @throws Exception
		 */
		public void insert(object[] row, Channel c) 
		{
			if (dDatabase.isReferentialIntegrity()) 
			{
				for (int i = 0; i < iConstraintCount; i++) 
				{
					((Constraint) vConstraint[i]).checkInsert(row);
				}
			}

			insertNoCheck(row, c);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param row
		 * @param c
		 *
		 * @throws Exception
		 */
		public void insertNoCheck(object[] row, Channel c) 
		{
			insertNoCheck(row, c, true);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param row
		 * @param c
		 * @param log
		 *
		 * @throws Exception
		 */
		public void insertNoCheck(object[] row, Channel c,
			bool log) 
		{
			int i;

			if (iIdentityColumn != -1) 
			{
				if (row[iIdentityColumn] == null)
				{
					if (c != null) 
					{
						c.setLastIdentity(iIdentityId);
					}

					row[iIdentityColumn] = iIdentityId++;
				} 
				else 
				{
					i = (int) row[iIdentityColumn];

					if (iIdentityId <= i) 
					{
						if (c != null) 
						{
							c.setLastIdentity(i);
						}

						iIdentityId = i + 1;
					}
				}
			}

			for (i = 0; i < iColumnCount; i++) 
			{
				if (row[i] == null &&!getColumn(i).isNullable()) 
				{
					throw Trace.error(Trace.TRY_TO_INSERT_NULL);
				}
			}

			try 
			{
				Row r = new Row(this, row);

				for (i = 0; i < iIndexCount; i++) 
				{
					Node n = r.getNode(i);

					getIndex(i).insert(n);
				}
			} 
			catch (Exception e) 
			{    // rollback insert
				for (--i; i >= 0; i--) 
				{
					getIndex(i).delete(row, i == 0);
				}

				throw e;		      // and throw error again
			}

			if (c != null) 
			{
				c.addTransactionInsert(this, row);
			}

			if (lLog != null) 
			{
				lLog.write(c, getInsertStatement(row));
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param row
		 * @param c
		 *
		 * @throws Exception
		 */
		public void delete(object[] row, Channel c) 
		{
			if (dDatabase.isReferentialIntegrity()) 
			{
				for (int i = 0; i < iConstraintCount; i++) 
				{
					((Constraint) vConstraint[i]).checkDelete(row);
				}
			}

			deleteNoCheck(row, c);

		}

		/**
		 * Method declaration
		 *
		 *
		 * @param row
		 * @param c
		 *
		 * @throws Exception
		 */
		public void deleteNoCheck(object[] row, Channel c) 
		{
			deleteNoCheck(row, c, true);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param row
		 * @param c
		 * @param log
		 *
		 * @throws Exception
		 */
		public void deleteNoCheck(object[] row, Channel c,
			bool log) 
		{
			for (int i = 1; i < iIndexCount; i++) 
			{
				getIndex(i).delete(row, false);
			}

			// must delete data last
			getIndex(0).delete(row, true);

			if (c != null) 
			{
				c.addTransactionDelete(this, row);
			}

			if (lLog != null) 
			{
				lLog.write(c, getDeleteStatement(row));
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param row
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public string getInsertStatement(object[] row) 
		{
			StringBuilder a = new StringBuilder();
			a.Append("INSERT INTO ");

			a.Append(getName());
			a.Append(" VALUES(");

			for (int i = 0; i < iVisibleColumns; i++) 
			{
				a.Append(Column.createstring(row[i], getColumn(i).iType));
				a.Append(',');
			}
			a.Remove((a.Length - 1),1); // pop off the last comma
			a.Append(')');

			return a.ToString();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public bool isCached() 
		{
			return bCached;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param s
		 *
		 * @return
		 */
		public Index getIndex(string s) 
		{
			for (int i = 0; i < iIndexCount; i++) 
			{
				Index h = getIndex(i);

				if (s.Equals(h.getName())) 
				{
					return h;
				}
			}

			// no such index
			return null;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 *
		 * @return
		 */
		public Column getColumn(int i) 
		{
			return (Column) vColumn[i];
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param i
		 *
		 * @return
		 */
		private Index getIndex(int i) 
		{
			return (Index) vIndex[i];
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param row
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private string getDeleteStatement(object[] row) 
		{
			StringBuilder a = new StringBuilder();
			a.Append("DELETE FROM ");

			a.Append(sName);
			a.Append(" WHERE ");

			if (iVisibleColumns < iColumnCount) 
			{
				for (int i = 0; i < iVisibleColumns; i++) 
				{
					a.Append(getColumn(i).sName);
					a.Append('=');
					a.Append(Column.createstring(row[i], getColumn(i).iType));

					if (i < iVisibleColumns - 1) 
					{
						a.Append(" AND ");
					}
				}
			} 
			else 
			{
				a.Append(getColumn(iPrimaryKey).sName);
				a.Append("=");
				a.Append(Column.createstring(row[iPrimaryKey],
					getColumn(iPrimaryKey).iType));
			}

			return a.ToString();
		}

	}
}
