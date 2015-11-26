/*
 * Cache.cs
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

	/**
	 * Cache class declaration
	 * <P>The cache class implements the handling of cached tables.
	 *
	 * @version 1.0.0.1
	 * @see Row
	 * @see CacheFree
	 */
	class Cache 
	{
		private FileStream rFile;
		private static int LENGTH = 1 << 14;
		private static int MAX_CACHE_SIZE = LENGTH * 3 / 4;
		private Row[]      rData;
		private Row[]	   rWriter;
		private Row	       rFirst;		   // must point to one of rData[]
		private Row		   rLastChecked;	   // can be any row
		private string	   sName;
		private static int MASK = (LENGTH) - 1;
		private int		   iFreePos;
		private static int FREE_POS_POS = 0;    // where iFreePos is saved
		private static int INITIAL_FREE_POS = 4;
		private static int MAX_FREE_COUNT = 1024;
		private CacheFree  fRoot;
		private int	       iFreeCount;
		private int	       iCacheSize;

		/**
		 * Cache constructor declaration
		 * <P>The cache constructor sets up the initial parameters of the cache
		 * object, setting the name used for the file, etc.
		 *
		 * @param name of database file
		 */
		public Cache(string name) 
		{
			sName = name;
			rData = new Row[LENGTH];
			rWriter = new Row[LENGTH];
		}

		/**
		 * open method declaration
		 * <P>The open method creates or opens a database file.
		 *
		 * @param flag that indicates if this cache is readonly
		 *
		 * @throws Exception
		 */
		public void open(bool ReadOnly) 
		{
			try 
			{
				bool exists = false;
				File    f = new File(sName);

				if (f.Exists && f.Length > FREE_POS_POS) 
				{
					exists = true;
				}

				if (ReadOnly)
				{
					rFile = new FileStream(sName, System.IO.FileMode.OpenOrCreate,FileAccess.Read);	
				}
				else
				{
					rFile = new FileStream(sName, System.IO.FileMode.OpenOrCreate,FileAccess.ReadWrite);	
				}
				
				if (exists) 
				{
					rFile.Seek(FREE_POS_POS,SeekOrigin.Begin);

					BinaryReader b = new BinaryReader(rFile);

					iFreePos = b.ReadInt32();
				} 
				else 
				{
					iFreePos = INITIAL_FREE_POS;
				}
			} 
			catch (Exception e) 
			{
				throw Trace.error(Trace.FILE_IO_ERROR,
					"error " + e + " opening " + sName);
			}
		}

		/**
		 * flush method declaration
		 * <P>The flush method saves all cached data to the file, saves the free position
		 * and closes the file.
		 *
		 * @throws Exception
		 */
		public void flush() 
		{
			try 
			{
				rFile.Seek(FREE_POS_POS,SeekOrigin.Begin);
				BinaryWriter b = new BinaryWriter(rFile);
				b.Write(iFreePos);
				saveAll();
				rFile.Close();
			} 
			catch (Exception e) 
			{
				throw Trace.error(Trace.FILE_IO_ERROR,
					"error " + e + " closing " + sName);
			}
		}

		/**
		 * shutdown method declaration
		 * <P>the shutdown method closes the cache file.  It does not flush pending writes.
		 *
		 * @throws Exception
		 */
		public void shutdown() 
		{
			try 
			{
				rFile.Close();
			} 
			catch (Exception e) 
			{
				throw Trace.error(Trace.FILE_IO_ERROR,
					"error " + e + " in shutdown " + sName);
			}
		}

		/**
		 * free method declaration
		 * <P>This method marks space in the database file as free.
		 * <P><B>Note: </B>If more than MAX_FREE_COUNT free positios then
		 * they are probably all are too small anyway; so we start a new list
		 * <P>todo: This is wrong when deleting lots of records
		 *
		 * @param r (Row object to be marked free)
		 * @param pos (Offset in the file this Row was stored at)
		 * @param length (Size of the Row object to free)
		 *
		 * @throws Exception
		 */
		public void free(Row r, int pos, int length) 
		{
			iFreeCount++;

			CacheFree n = new CacheFree();

			n.iPos = pos;
			n.iLength = length;

			if (iFreeCount > MAX_FREE_COUNT) 
			{
				iFreeCount = 0;
			} 
			else 
			{
				n.fNext = fRoot;
			}

			fRoot = n;

			// it's possible to remove roots to
			remove(r);
		}

		/**
		 * add method declaration
		 * <P>This method adds a Row to the Cache.  It walks the
		 * list of CacheFree objects to see if there is available space
		 * to store the new Row, reusing space if it exists, otherwise
		 * we grow the file.
		 *
		 * @param r (Row to be added to Cache)
		 *
		 * @throws Exception
		 */
		public void add(Row r) 
		{
			int       size = r.iSize;
			CacheFree f = fRoot;
			CacheFree last = null;
			int       i = iFreePos;

			while (f != null) 
			{
				if (Trace.TRACE) 
				{
					Trace.stop();
				}
				// first that is long enough
				if (f.iLength >= size) 
				{
					i = f.iPos;
					size = f.iLength - size;

					if (size < 8) 
					{

						// remove almost empty blocks
						if (last == null) 
						{
							fRoot = f.fNext;
						} 
						else 
						{
							last.fNext = f.fNext;
						}

						iFreeCount--;
					} 
					else 
					{
						f.iLength = size;
						f.iPos += r.iSize;
					}

					break;
				}

				last = f;
				f = f.fNext;
			}

			r.iPos = i;

			if (i == iFreePos) 
			{
				iFreePos += size;
			}

			int k = i & MASK;
			Row before = rData[k];

			if (before == null) 
			{
				before = rFirst;
			}

			r.insert(before);

			iCacheSize++;
			rData[k] = r;
			rFirst = r;
		}

		/**
		 * getRow method declaration
		 * <P>This method reads a Row object from the cache.
		 *
		 * @param pos (offset of the requested Row in the cache)
		 * @param t (Table this Row belongs to)
		 *
		 * @return The Row object as read from the cache.
		 *
		 * @throws Exception
		 */
		public Row getRow(int pos, Table t) 
		{
			int k = pos & MASK;
			Row r = rData[k];
			Row start = r;

			while (r != null) 
			{
				if (Trace.STOP) 
				{
					Trace.stop();
				}

				int p = r.iPos;

				if (p == pos) 
				{
					return r;
				} 
				else if ((p & MASK) != k) 
				{
					break;
				}

				r = r.rNext;

				if (r == start) 
				{
					break;
				}
			}

			Row before = rData[k];

			if (before == null) 
			{
				before = rFirst;
			}

			try 
			{
				rFile.Seek(pos,SeekOrigin.Begin);

				BinaryReader b = new BinaryReader(rFile);

				int  size = b.ReadInt32();
				byte[] buffer = new byte[size];

				buffer = b.ReadBytes(size);

				MemoryStream bin = new MemoryStream(buffer);
				BinaryReader din = new BinaryReader(bin);

				r = new Row(t, din, pos, before);
				r.iSize = size;
			} 
			catch (IOException e) 
			{
				Console.WriteLine(e.StackTrace);

				throw Trace.error(Trace.FILE_IO_ERROR, "reading: " + e);
			}

			// todo: copy & paste here
			iCacheSize++;
			rData[k] = r;
			rFirst = r;

			return r;
		}

		/**
		 * cleanUp method declaration
		 * <P>This method cleans up the cache when it grows too large. It works by
		 * checking Rows in held in the Cache's iLastAccess member and removing
		 * Rows that haven't been accessed in the longest time.
		 *
		 * @throws Exception
		 */
		public void cleanUp() 
		{
			if (iCacheSize < MAX_CACHE_SIZE) 
			{
				return;
			}

			int count = 0, j = 0;

			while (j++ < LENGTH && iCacheSize + LENGTH > MAX_CACHE_SIZE
				&& (count * 16) < LENGTH) 
			{
				if (Trace.STOP) 
				{
					Trace.stop();
				}

				Row r = getWorst();

				if (r == null) 
				{
					return;
				}

				if (r.bChanged) 
				{
					rWriter[count++] = r;
				} 
				else 
				{

					// here we can't remove roots
					if (!r.canRemove()) 
					{
						remove(r);
					}
				}
			}

			if (count != 0) 
			{
				saveSorted(count);
			}

			for (int i = 0; i < count; i++) 
			{

				// here we can't remove roots
				Row r = rWriter[i];

				if (!r.canRemove()) 
				{
					remove(r);
				}

				rWriter[i] = null;
			}
		}

		/**
		 * remove method declaration
		 * <P>This method is used to remove Rows from the Cache. It is called
		 * by the cleanUp method.
		 *
		 * @param r (Row to be removed)
		 *
		 * @throws Exception
		 */
		private void remove(Row r) 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(!r.bChanged);

				// make sure rLastChecked does not point to r
			}

			if (r == rLastChecked) 
			{
				rLastChecked = rLastChecked.rNext;

				if (rLastChecked == r) 
				{
					rLastChecked = null;
				}
			}

			// make sure rData[k] does not point here
			int k = r.iPos & MASK;

			if (rData[k] == r) 
			{
				Row n = r.rNext;

				rFirst = n;

				if (n == r || (n.iPos & MASK) != k) 
				{
					n = null;
				}

				rData[k] = n;
			}

			// make sure rFirst does not point here
			if (r == rFirst) 
			{
				rFirst = rFirst.rNext;

				if (r == rFirst) 
				{
					rFirst = null;
				}
			}

			r.free();

			iCacheSize--;
		}

		/**
		 * getWorst method declaration
		 * <P>This method finds the Row with the smallest (oldest) iLastAccess member.
		 * Called by the cleanup method.
		 *
		 * @return The selected Row object
		 *
		 * @throws Exception
		 */
		private Row getWorst() 
		{
			if (rLastChecked == null) 
			{
				rLastChecked = rFirst;
			}

			Row r = rLastChecked;

			if (r == null) 
			{
				return null;
			}

			Row candidate = r;
			int worst = Row.iCurrentAccess;

			// algorithm: check the next rows and take the worst
			for (int i = 0; i < 6; i++) 
			{
				int w = r.iLastAccess;

				if (w < worst) 
				{
					candidate = r;
					worst = w;
				}

				r = r.rNext;
			}

			rLastChecked = r.rNext;

			return candidate;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void saveAll() 
		{
			if (rFirst == null) 
			{
				return;
			}

			Row r = rFirst;

			while (true) 
			{
				int count = 0;
				Row begin = r;

				do 
				{
					if (Trace.STOP) 
					{
						Trace.stop();
					}

					if (r.bChanged) 
					{
						rWriter[count++] = r;
					}

					r = r.rNext;
				} while (r != begin && count < LENGTH);

				if (count == 0) 
				{
					return;
				}

				saveSorted(count);

				for (int i = 0; i < count; i++) 
				{
					rWriter[i] = null;
				}
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param count
		 *
		 * @throws Exception
		 */
		private void saveSorted(int count) 
		{
			if (count < 1)
			{
				return;
			}

			sort(rWriter, 0, count - 1);

			try 
			{
				rFile.Seek(rWriter[0].iPos,SeekOrigin.Begin);
				BinaryWriter b = new BinaryWriter(rFile);

				for (int i = 0; i < count; i++) 
				{
					b.Write(rWriter[i].write());
				}
			} 
			catch (Exception e) 
			{
				throw Trace.error(Trace.FILE_IO_ERROR, "saveSorted " + e);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param w
		 * @param l
		 * @param r
		 *
		 * @throws Exception
		 */
		private static void sort(Row[] w, int l,
			int r) 
		{
			int i, j, p;

			while (r - l > 10) 
			{
				i = (r + l) >> 1;

				if (w[l].iPos > w[r].iPos) 
				{
					swap(w, l, r);
				}

				if (w[i].iPos < w[l].iPos) 
				{
					swap(w, l, i);
				} 
				else if (w[i].iPos > w[r].iPos) 
				{
					swap(w, i, r);
				}

				j = r - 1;

				swap(w, i, j);

				p = w[j].iPos;
				i = l;

				while (true) 
				{
					if (Trace.STOP) 
					{
						Trace.stop();
					}

					while (w[++i].iPos < p);

					while (w[--j].iPos > p);

					if (i >= j) 
					{
						break;
					}

					swap(w, i, j);
				}

				swap(w, i, r - 1);
				sort(w, l, i - 1);

				l = i + 1;
			}

			for (i = l + 1; i <= r; i++) 
			{
				if (Trace.STOP) 
				{
					Trace.stop();
				}

				Row t = w[i];

				for (j = i - 1; j >= l && w[j].iPos > t.iPos; j--) 
				{
					w[j + 1] = w[j];
				}

				w[j + 1] = t;
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param w
		 * @param a
		 * @param b
		 */
		private static void swap(Row[] w, int a, int b) 
		{
			Row t = w[a];

			w[a] = w[b];
			w[b] = t;
		}

	}
}
