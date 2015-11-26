/*
 * Result.cs
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
	 * Class declaration
	 *
	 *
	 * @version 1.0.0.1
	 */
	class Result 
	{
		private Record       rTail;
		private int          iSize;
		private int          iColumnCount;
		static int           UPDATECOUNT = 0, ERROR = 1, DATA = 2;
		public int		     iMode;
		public string	     sError;
		public int		     iUpdateCount;
		public Record	     rRoot;
		public string[]	     sLabel;
		public string[]	     sTable;
		public string[]	     sName;
		public int[]		 iType;

		/**
		 * Constructor declaration
		 *
		 */
		public Result() 
		{
			iMode = UPDATECOUNT;
			iUpdateCount = 0;
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param error
		 */
		public Result(string error) 
		{
			iMode = ERROR;
			sError = error;
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param columns
		 */
		public Result(int columns) 
		{
			prepareData(columns);

			iColumnCount = columns;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getSize() 
		{
			return iSize;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param columns
		 */
		public void setColumnCount(int columns) 
		{
			iColumnCount = columns;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getColumnCount() 
		{
			return iColumnCount;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param a
		 */
		public void append(Result a) 
		{
			if (rRoot == null) 
			{
				rRoot = a.rRoot;
			} 
			else 
			{
				rTail.next = a.rRoot;
			}

			rTail = a.rTail;
			iSize += a.iSize;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param d
		 */
		public void add(object[] d) 
		{
			Record r = new Record();

			r.data = d;

			if (rRoot == null) 
			{
				rRoot = r;
			} 
			else 
			{
				rTail.next = r;
			}

			rTail = r;
			iSize++;
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param b
		 */
		public Result(byte[] b) 
		{
			MemoryStream bin = new MemoryStream(b);
			BinaryReader din = new BinaryReader(bin);

			try 
			{
				iMode = din.ReadInt32();

				if (iMode == ERROR) 
				{
					throw Trace.getError(din.ReadString());
				} 
				else if (iMode == UPDATECOUNT) 
				{
					iUpdateCount = din.ReadInt32();
				} 
				else if (iMode == DATA) 
				{
					int l = din.ReadInt32();

					prepareData(l);

					iColumnCount = l;

					for (int i = 0; i < l; i++) 
					{
						iType[i] = din.ReadInt32();
						sLabel[i] = din.ReadString();
						sTable[i] = din.ReadString();
						sName[i] = din.ReadString();
					}

					while (din.PeekChar() != -1) 
					{
						add(Column.readData(din, l));
					}
				}
			} 
			catch (Exception e) 
			{
				Trace.error(Trace.TRANSFER_CORRUPTED);
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
		public byte[] getBytes() 
		{
			MemoryStream bout = new MemoryStream();
			BinaryWriter dout = new BinaryWriter(bout);

			try 
			{
				dout.Write(iMode);

				if (iMode == UPDATECOUNT) 
				{
					dout.Write(iUpdateCount);
				} 
				else if (iMode == ERROR) 
				{
					dout.Write(sError);
				} 
				else 
				{
					int l = iColumnCount;

					dout.Write(l);

					Record n = rRoot;

					for (int i = 0; i < l; i++) 
					{
						dout.Write(iType[i]);
						dout.Write(sLabel[i]);
						dout.Write(sTable[i]);
						dout.Write(sName[i]);
					}

					while (n != null) 
					{
						Column.writeData(dout, l, iType, n.data);

						n = n.next;
					}
				}

				return bout.ToArray();
			} 
			catch (Exception e) 
			{
				throw Trace.error(Trace.TRANSFER_CORRUPTED);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param columns
		 */
		private void prepareData(int columns) 
		{
			iMode = DATA;
			sLabel = new string[columns];
			sTable = new string[columns];
			sName = new string[columns];
			iType = new int[columns];
		}

	}
}
