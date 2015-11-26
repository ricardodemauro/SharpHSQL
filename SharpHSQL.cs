/*
 * SharpHSQL.cs
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

	public class SharpHSQL
	{
		public SharpHSQL()
		{
			//
			// TODO: Add Constructor Logic here
			//
		}

		public static int Main(string[] args)
		{
			bool inmem = false;
			bool selftest = false;
			Database db;

			foreach (string arg in args)
			{
				if (arg.ToLower().Equals("-m"))
				{
					inmem = true;
				}
				else if (arg.ToLower().Equals("-selftest"))
				{
					selftest = true;
				}
			}
			if (inmem == true)
			{
				// Create an in memory database
				Console.WriteLine("In-memory database mode");
				db = new Database("."); 
			}
			else 
			{
				// Create a disk based database
				Console.WriteLine("Durable database mode");
				db = new Database("test"); 
			}

			Channel myChannel = db.connect("sa","");
			Result rs;
			string query = "";

			// do a little performance test
			if (selftest == true)
			{
				Console.WriteLine("Running performance test.\n\n");
				query = "CREATE TABLE Addr(ID INT PRIMARY KEY, First CHAR, Name CHAR, ZIP INT)";
				db.execute(query,myChannel);
				query = "CREATE INDEX iName ON Addr(Name)";
				db.execute(query,myChannel);
				DateTime time = DateTime.Now;
				for (int i = 0; i < 10000; i++) 
				{
					query = "INSERT INTO Addr VALUES(" + i + ",'Mark" + i + "'," + "'Tutt" + (10000 - i - (i % 31)) + "', " + (3000 + i % 100) + ")";
					rs = db.execute(query,myChannel);
					if (rs.sError != null)
					{
						Console.WriteLine(rs.sError);
					}
				}

				TimeSpan execution = DateTime.Now.Subtract(time);
				Console.WriteLine("Inserted 10000 records in " + execution.TotalMilliseconds.ToInt64() + " milliseconds, " + ((10000 * 1000) /execution.TotalMilliseconds.ToInt64()) + " inserts per second.");
	
				query = "SELECT * FROM Addr";
				rs = db.execute(query,myChannel);
				if (rs.getSize() != 10000)
				{
					Console.WriteLine("An error occurred, rowcount should be 10000 but is " + rs.getSize());
				}

				time = DateTime.Now;

				for (int i = 0; i < 10000; i++) 
				{
					query = "UPDATE Addr SET Name='Michael" + (i + (i % 31)) + "' WHERE ID=" + i;
					rs = db.execute(query,myChannel);
					if (rs.sError != null)
					{
						Console.WriteLine(rs.sError);
					}
				}

				execution = DateTime.Now.Subtract(time);
				Console.WriteLine("Updated 10000 records in " + execution.TotalMilliseconds.ToInt64() + " milliseconds, " + ((10000 * 1000) /execution.TotalMilliseconds.ToInt64()) + " updates per second.");

				time = DateTime.Now;

				for (int i = 0; i < 10000; i++) 
				{
					query = "SELECT * FROM Addr WHERE ID=" + (10000 - 1 - i);
					rs = db.execute(query,myChannel);
					if (rs.sError != null)
					{
						Console.WriteLine(rs.sError);
					}
				}

				execution = DateTime.Now.Subtract(time);
				Console.WriteLine("Executed 10000 selects in " + execution.TotalMilliseconds.ToInt64() + " milliseconds, " + ((10000 * 1000) /execution.TotalMilliseconds.ToInt64()) + " selects per second.");

				time = DateTime.Now;

				for (int i = 0; i < 10000; i++) 
				{
					query = "DELETE FROM Addr WHERE ID=" + (10000 - 1 - i);
					rs = db.execute(query,myChannel);
					if (rs.sError != null)
					{
						Console.WriteLine(rs.sError);
					}
				}

				execution = DateTime.Now.Subtract(time);
				Console.WriteLine("Deleted 10000 records in " + execution.TotalMilliseconds.ToInt64() + " milliseconds, " + ((10000 * 1000) /execution.TotalMilliseconds.ToInt64()) + " deletes per second.");
			}

			Console.WriteLine("\nInteractive SQL ready, type 'quit' to exit\n");

			query = "";

			while (!query.ToLower().Equals("quit"))
			{
				Console.Write("SQL> ");
				query = Console.ReadLine();
				if (!query.ToLower().Equals("quit"))
				{
					rs = db.execute(query,myChannel);
					if (rs.sError != null)
					{
						Console.WriteLine(rs.sError);
					}
					else
					{
						Console.Write(rs.getSize() + " rows returned, " + rs.iUpdateCount + " rows affected.\n\n");
						if (rs.rRoot != null)
						{
							Record r = rs.rRoot;
							int column_count = rs.getColumnCount();
							for (int x = 0; x < column_count;x++)
							{
								Console.Write(rs.sLabel[x]);
								Console.Write("\t");
							}
							Console.Write("\n");
							while (r != null)
							{
								for (int x = 0; x < column_count;x++)	
								{
									Console.Write(r.data[x]);
									Console.Write("\t");
								}
								Console.Write("\n");
								r = r.next;
							}
							Console.Write("\n");
						}
					}
				}
			}
			return 0;
		}
	}
}