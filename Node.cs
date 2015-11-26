/*
 * Node.cs
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
	class Node 
	{
		public int		iBalance;    // currently, -2 means 'deleted'
		public int		iLeft, iRight, iParent;
		public Node	nLeft, nRight, nParent;
		private int iId;	     // id of index this table

		public Node	nNext;    // node of next index (nNext==null || nNext.iId=iId+1)

		public Row		rData;

		/**
		 * Constructor declaration
		 *
		 *
		 * @param r
		 * @param in
		 * @param id
		 */
		public Node(Row r, BinaryReader din, int id) 
		{
			iId = id;
			rData = r;
			iBalance = din.ReadInt32();
			iLeft = din.ReadInt32();
			iRight = din.ReadInt32();
			iParent = din.ReadInt32();

			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);
			}
		}

		/**
		 * Constructor declaration
		 *
		 *
		 * @param r
		 * @param id
		 */
		public Node(Row r, int id) 
		{
			iId = id;
			rData = r;
		}

		/**
		 * Method declaration
		 *
		 */
		public void delete() 
		{
			iBalance = -2;
			nLeft = nRight = nParent = null;
			iLeft = iRight = iParent = 0;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 */
		public int getKey() 
		{
			return rData.iPos;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public Node getLeft() 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);
			}

			if (iLeft == 0) 
			{
				return nLeft;
			}

			// rData.iLastAccess=Row.iCurrentAccess++;
			return rData.getNode(iLeft, iId);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param n
		 *
		 * @throws Exception
		 */
		public void setLeft(Node n) 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);
			}

			rData.changed();

			if (n == null) 
			{
				iLeft = 0;
				nLeft = null;
			} 
			else if (n.rData.iPos != 0) 
			{
				iLeft = n.rData.iPos;
			} 
			else 
			{
				nLeft = n;
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
		public Node getRight() 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);
			}

			if (iRight == 0) 
			{
				return nRight;
			}

			// rData.iLastAccess=Row.iCurrentAccess++;
			return rData.getNode(iRight, iId);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param n
		 *
		 * @throws Exception
		 */
		public void setRight(Node n) 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);
			}

			rData.changed();

			if (n == null) 
			{
				iRight = 0;
				nRight = null;
			} 
			else if (n.rData.iPos != 0) 
			{
				iRight = n.rData.iPos;
			} 
			else 
			{
				nRight = n;
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
		public Node getParent() 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);
			}

			if (iParent == 0) 
			{
				return nParent;
			}

			// rData.iLastAccess=Row.iCurrentAccess++;
			return rData.getNode(iParent, iId);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param n
		 *
		 * @throws Exception
		 */
		public void setParent(Node n) 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);
			}

			rData.changed();

			if (n == null) 
			{
				iParent = 0;
				nParent = null;
			} 
			else if (n.rData.iPos != 0) 
			{
				iParent = n.rData.iPos;
			} 
			else 
			{
				nParent = n;
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
		public int getBalance() 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);

				// rData.iLastAccess=Row.iCurrentAccess++;
			}

			return iBalance;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param b
		 *
		 * @throws Exception
		 */
		public void setBalance(int b) 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);
			}

			if (iBalance != b) 
			{
				rData.changed();

				iBalance = b;
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
		public object[] getData() 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);
			}

			return rData.getData();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param n
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public bool equals(Node n) 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);

				// rData.iLastAccess=Row.iCurrentAccess++;
			}

			if (Trace.ASSERT) 
			{
				if (n != this) 
				{
					Trace.assert(rData.iPos == 0 || n == null
						|| n.rData.iPos != rData.iPos);
				} 
				else 
				{
					Trace.assert(n.rData.iPos == rData.iPos);
				}
			}

			return n == this;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param out
		 *
		 * @throws IOException
		 * @throws Exception
		 */
		public void write(BinaryWriter dout) 
		{
			if (Trace.ASSERT) 
			{
				Trace.assert(iBalance != -2);
			}

			dout.Write(iBalance);
			dout.Write(iLeft);
			dout.Write(iRight);
			dout.Write(iParent);

			if (nNext != null) 
			{
				nNext.write(dout);
			}
		}

	}
}
