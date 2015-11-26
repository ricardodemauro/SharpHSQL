/*
 * Log.cs
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
	using System.Xml;
	using System.Collections;
	using System.IO;
	using System.Threading;

	/**
	 * <P>This class is responsible for most file handling.
	 * A HSQL database consists of a .properties file, a
	 * .script file (contains a SQL script), a
	 * .data file (contains data of cached tables) and a
	 * .backup file (contains the compressed .data file)
	 *
	 * <P>This is an example of the .properties file. The version and the
	 * modified properties are automatically created by the database and
	 * should not be changed manually:
	 * <pre>
	 * modified=no
	 * version=1.3
	 * </pre>
	 * The following lines are optional, this means they are not
	 * created automatically by the database, but they are interpreted
	 * if they exist in the .script file. They have to be created
	 * manually if required. If they don't exist the default is used.
	 * This are the defaults of the database 'test':
	 * <pre>
	 * script=test.script
	 * data=test.data
	 * backup=test.backup
	 * readonly=false
	 * </pre>
	 */
	class Log
	{
		private static int COPY_BLOCK_SIZE = 1 << 16;  // block size for copying data
		private string		       sName;
		private Database	       dDatabase;
		private Channel	           cSystem;
		private StreamWriter	   wScript;
		private string	           sFileProperties;
		private string	           sFileScript;
		private string	           sFileCache;
		private string	           sFileBackup;
		private string			   sModified;
		private string			   sVersion;
		private bool	           bRestoring;
		private bool	           bReadOnly;
		private int		           iLogSize =	200;  // default: .script file is max 200 MB big
		private int		           iLogCount;
		private Thread	           tRunner;
		private bool               bNeedFlush;
		private bool               bWriteDelay;
		private int		           mLastId;
		public Cache		               cCache;

		/**
		 * Constructor declaration
		 *
		 *
		 * @param db
		 * @param system
		 * @param name
		 */
		public Log(Database db, Channel system, string name) 
		{
			dDatabase = db;
			cSystem = system;
			sName = name;
			sFileProperties = sName + ".cfg";
			sFileScript = sName + ".log";
			sFileCache = sName + ".data";
			sFileBackup = sName + ".backup";
		}

		/**
		 * Method declaration
		 *
		 */
		public void run() 
		{
			while (tRunner != null) 
			{
				try 
				{
					//					tRunner.sleep(1000);

					if (bNeedFlush) 
					{
						wScript.Flush();

						bNeedFlush = false;
					}

					// todo: try to do Cache.cleanUp() here, too
				} 
				catch (Exception e) 
				{

					// ignore exceptions; may be InterruptedException or IOException
				}
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param delay
		 */
		public void setWriteDelay(bool delay) 
		{
			bWriteDelay = delay;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		public bool open() 
		{
			bool newdata = false;

			if (Trace.TRACE) 
			{
				Trace.trace();
			}

			if (!(new File(sFileProperties)).Exists) 
			{
				create();
				// this is a new database
				newdata = true;
			}

			// todo: some parts are not necessary for read-only access
			loadProperties();


			if (bReadOnly == true)
			{
				dDatabase.setReadOnly();

				cCache = new Cache(sFileCache);

				cCache.open(true);
				runScript();

				return false;
			}

			bool needbackup = false;

			if (sModified.Equals("yes-new-files")) 
			{
				renameNewToCurrent(sFileScript);
				renameNewToCurrent(sFileBackup);
			} 
			else if (sModified.Equals("yes")) 
			{
				if (isAlreadyOpen()) 
				{
					throw Trace.error(Trace.DATABASE_ALREADY_IN_USE);
				}

				// recovering after a crash (or forgot to close correctly)
				restoreBackup();

				needbackup = true;
			}

			sModified = "yes";
			saveProperties();

			cCache = new Cache(sFileCache);

			cCache.open(false);
			runScript();

			if (needbackup) 
			{
				close(false);
 			    sModified = "yes";
				saveProperties();
				cCache.open(false);
			}

			openScript();

			// this is a existing database
			return newdata;
		}

		/**
		 * Method declaration
		 *
		 */
		public void stop() 
		{
			tRunner = null;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param compact
		 *
		 * @throws Exception
		 */
		public void close(bool compact) 
		{
		
	
			if (Trace.TRACE) 
			{
				Trace.trace();
			}

			if (bReadOnly) 
			{
				return;
			}

			// no more scripting
			closeScript();

			// create '.script.new' (for this the cache may be still required)
			writeScript(compact);

			// flush the cache (important: after writing the script)
			cCache.flush();

			// create '.backup.new' using the '.data'
			backup();

			// we have the new files
			sModified = "yes-new-files";
			saveProperties();

			// old files can be removed and new files renamed
			renameNewToCurrent(sFileScript);
			renameNewToCurrent(sFileBackup);

			// now its done completely
			sModified = "no";
			saveProperties();
			closeProperties();

			if (compact) 
			{

				// stop the runner thread of this process (just for security)
				stop();

				// delete the .data so then a new file is created
				(new File(sFileCache)).Delete();
				(new File(sFileBackup)).Delete();

				// all files are closed now; simply open & close this database
				Database db = new Database(sName);

				db.getLog().close(false);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void checkpoint() 
		{
			close(false);
			sModified = "yes";
			saveProperties();
			cCache.open(false);
			openScript();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param mb
		 */
		public void setLogSize(int mb) 
		{
			iLogSize = mb;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param c
		 * @param s
		 *
		 * @throws Exception
		 */
		public void write(Channel c, string s) 
		{
			if (bRestoring || s == null || s.Equals("")) 
			{
				return;
			}

			if (!bReadOnly) 
			{
				int id = 0;

				if (c != null) 
				{
					id = c.getId();
				}

				if (id != mLastId) 
				{
					s = "/*C" + id + "*/" + s;
					mLastId = id;
				}

				try 
				{
					writeLine(wScript, s);

					if (bWriteDelay) 
					{
						bNeedFlush = true;
					} 
					else 
					{
						wScript.Flush();
					}
				} 
				catch (IOException e) 
				{
					Trace.error(Trace.FILE_IO_ERROR, sFileScript);
				}

				if (iLogSize > 0 && iLogCount++ > 100) 
				{
					iLogCount = 0;

					if ((new File(sFileScript)).Length > iLogSize * 1024 * 1024) 
					{
						checkpoint();
					}
				}
			}

		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		public void shutdown() 
		{
			tRunner = null;

			cCache.shutdown();
			closeScript();
			closeProperties();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param db
		 * @param file
		 * @param full
		 * @param channel
		 *
		 * @throws Exception
		 */
		public static void scriptToFile(Database db, string file, bool full,
			Channel channel) 
		{
			if ((new File(file)).Exists) 
			{

				// there must be no such file; overwriting not allowed for security
				throw Trace.error(Trace.FILE_IO_ERROR, file);
			}

			try 
			{
				DateTime   time = DateTime.Now;

				// only ddl commands; needs not so much memory
				Result r;

				if (full) 
				{

					// no drop, no insert, and no positions for cached tables
					r = db.getScript(false, false, false, channel);
				} 
				else 
				{

					// no drop, no insert, but positions for cached tables
					r = db.getScript(false, false, true, channel);
				}

				Record     n = r.rRoot;
				StreamWriter w = new StreamWriter(file);	

				while (n != null) 
				{
					writeLine(w, (string) n.data[0]);

					n = n.next;
				}

				// inserts are done separetely to save memory
				ArrayList tables = db.getTables();

				for (int i = 0; i < tables.Count; i++) 
				{
					Table t = (Table) tables[i];

					// cached tables have the index roots set in the ddl script
					if (full ||!t.isCached()) 
					{
						Index primary = t.getPrimaryIndex();
						Node  x = primary.first();

						while (x != null) 
						{
							writeLine(w, t.getInsertStatement(x.getData()));

							x = primary.next(x);
						}
					}
				}

				w.Close();

				TimeSpan execution = DateTime.Now.Subtract(time);

				if (Trace.TRACE) 
				{
					Trace.trace(execution.TotalMilliseconds.ToInt64());
				}
			} 
			catch (IOException e) 
			{
				Trace.error(Trace.FILE_IO_ERROR, file + " " + e);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param file
		 */
		private void renameNewToCurrent(string file) 
		{

			// even if it crashes here, recovering is no problem
			if ((new File(file + ".new")).Exists) 
			{

				// if we have a new file
				// delete the old (maybe already deleted)
				(new File(file)).Delete();

				// rename the new to the current
				new File(file + ".new").MoveTo(file);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void closeProperties() 
		{
			try 
			{
				if (Trace.TRACE) 
				{
					Trace.trace();
				}
			} 
			catch (Exception e) 
			{
				throw Trace.error(Trace.FILE_IO_ERROR, sFileProperties + " " + e);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void create() 
		{
			if (Trace.TRACE) 
			{
				Trace.trace(sName);
			}

			XmlTextWriter writer = new XmlTextWriter(sFileProperties, null);
		    writer.Formatting = Formatting.Indented;
			writer.Indentation=4;
     		writer.WriteStartDocument();
			writer.WriteComment("SharpHSQL Configuration");
			writer.WriteProcessingInstruction("Instruction","Configuration Record");
			writer.WriteStartElement("Properties","");
			writer.WriteStartAttribute("LogFile","");
			writer.WriteString(sFileScript);
			writer.WriteEndAttribute();
			writer.WriteStartAttribute("DataFile","");
			writer.WriteString(sFileCache);
			writer.WriteEndAttribute();
			writer.WriteStartAttribute("Backup","");
			writer.WriteString(sFileBackup);
			writer.WriteEndAttribute();
			writer.WriteStartAttribute("Version","");
			writer.WriteString("1.0");
			writer.WriteEndAttribute();
			writer.WriteStartAttribute("ReadOnly","");
			writer.WriteString("false");
			writer.WriteEndAttribute();
			writer.WriteStartAttribute("Modified","");
			writer.WriteString("no");
			writer.WriteEndElement();
			writer.WriteEndDocument();
			writer.Flush();
			writer.Close();

			saveProperties();
		}

		/**
		 * Method declaration
		 *
		 *
		 * @return
		 *
		 * @throws Exception
		 */
		private bool isAlreadyOpen() 
		{

			// reading the last modified, wait 3 seconds, read again.
			// if the same information was read the file was not changed
			// and is probably, except the other process is blocked
			if (Trace.TRACE) 
			{
				Trace.trace();
			}

			File f = new File(sFileScript);
			DateTime l1 = f.LastWriteTime;

			try 
			{
				Thread.Sleep(3000);
			} 
			catch (Exception e) {}

			DateTime l2 = f.LastWriteTime;

			if (l1 != l2) 
			{
				return true;
			}

			// check by trying to delete the properties file
			// this will not work if some application has the file open
			// this is why the properties file is kept open when running ;-)
			// todo: check if this works in all operating systems
			closeProperties();

			if (Trace.TRACE) 
			{
				Trace.trace();
			}

			try 
			{
				(new File(sFileProperties)).Delete();
			}
			catch (Exception e)
			{
				return true;
			}

			// the file was deleted, so recreate it now
			saveProperties();

			return false;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void loadProperties() 
		{
			try 
			{
				XmlTextReader reader = new XmlTextReader(sFileProperties);
				//Read the tokens from the reader
				while ( reader.Read() )
				{
					if (XmlNodeType.Element == reader.NodeType)
					{
						sFileScript = reader.GetAttribute("LogFile");
						sFileCache = reader.GetAttribute("DataFile");
						sFileBackup = reader.GetAttribute("Backup");
						sModified = reader.GetAttribute("Modified");
		 		        sVersion = reader.GetAttribute("Version");
		 		        bReadOnly = reader.GetAttribute("ReadOnly").ToLower().Equals("true");
					}
				}
				reader.Close();
 			}
			catch (Exception e)
			{
				Console.WriteLine("Property File Exeception:", e.ToString());
			}

			if (Trace.TRACE) 
			{
				Trace.trace();
			}

		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void saveProperties() 
		{
			XmlTextWriter writer = new XmlTextWriter(sFileProperties, null);
		    writer.Formatting = Formatting.Indented;
			writer.Indentation=4;
     		writer.WriteStartDocument();
			writer.WriteComment("SharpHSQL Configuration");
			writer.WriteProcessingInstruction("Instruction","Configuration Record");
			writer.WriteStartElement("Properties","");
			writer.WriteStartAttribute("LogFile","");
			writer.WriteString(sFileScript);
			writer.WriteEndAttribute();
			writer.WriteStartAttribute("DataFile","");
			writer.WriteString(sFileCache);
			writer.WriteEndAttribute();
			writer.WriteStartAttribute("Backup","");
			writer.WriteString(sFileBackup);
			writer.WriteEndAttribute();
			writer.WriteStartAttribute("Version","");
			writer.WriteString(sVersion);
			writer.WriteEndAttribute();
			writer.WriteStartAttribute("ReadOnly","");
			if (bReadOnly == true)
			{
				writer.WriteString("true");
			}
			else
			{
				writer.WriteString("false");
			}
			writer.WriteEndAttribute();
			writer.WriteStartAttribute("Modified","");
			writer.WriteString(sModified);
			writer.WriteEndElement();
			writer.WriteEndDocument();
			writer.Flush();
			writer.Close();

			closeProperties();

			if (Trace.TRACE) 
			{
				Trace.trace();
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void backup() 
		{
			if (Trace.TRACE) 
			{
				Trace.trace();

				// if there is no cache file then backup is not necessary
			}

			if (!(new File(sFileCache)).Exists) 
			{
				return;
			}

			try 
			{
				DateTime time = DateTime.Now;

				// create a '.new' file; rename later
				BinaryWriter f = new BinaryWriter(new FileStream(sFileBackup + ".new",FileMode.OpenOrCreate,FileAccess.Write));
				byte[]		 b = new byte[COPY_BLOCK_SIZE];
				BinaryReader fin = new BinaryReader(new FileStream(sFileCache,FileMode.Open,FileAccess.Read));

				while (true) 
				{
					int l = fin.Read(b, 0, COPY_BLOCK_SIZE);

					if (l == 0) 
					{
						break;
					}

					f.Write(b, 0, l);
				}

				f.Close();
				fin.Close();

				TimeSpan execution = DateTime.Now.Subtract(time);

				if (Trace.TRACE) 
				{
					Trace.trace(execution.TotalMilliseconds.ToInt64());
				}
			}
			catch (Exception e) 
			{
				throw Trace.error(Trace.FILE_IO_ERROR, sFileBackup);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void restoreBackup() 
		{
			if (Trace.TRACE) 
			{
				Trace.trace("not closed last time!");
			}

			if (!(new File(sFileBackup)).Exists) 
			{

				// the backup don't exists because it was never made or is empty
				// the cache file must be deleted in this case
				(new File(sFileCache)).Delete();

				return;
			}

			try 
			{
				DateTime		time = DateTime.Now;
				BinaryReader f = new BinaryReader(new FileStream(sFileBackup,FileMode.Open,FileAccess.Read));
				BinaryWriter cache = new BinaryWriter(new FileStream(sFileCache,FileMode.OpenOrCreate,FileAccess.Write));
				byte[]		b = new byte[COPY_BLOCK_SIZE];

				while (true) 
				{
					int l = f.Read(b, 0, COPY_BLOCK_SIZE);

					if (l == 0) 
					{
						break;
					}

					cache.Write(b, 0, l);
				}

				cache.Close();
				f.Close();

				TimeSpan execution = DateTime.Now.Subtract(time);

				if (Trace.TRACE) 
				{
					Trace.trace(execution.TotalMilliseconds.ToInt64());
				}
			} 
			catch (Exception e) 
			{
				throw Trace.error(Trace.FILE_IO_ERROR, sFileBackup);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void openScript() 
		{
			if (Trace.TRACE) 
			{
				Trace.trace();
			}

			try 
			{
				// todo: use a compressed stream
				wScript = new StreamWriter(sFileScript,true);
			} 
			catch (Exception e) 
			{
				Trace.error(Trace.FILE_IO_ERROR, sFileScript);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void closeScript() 
		{
			if (Trace.TRACE) 
			{
				Trace.trace();
			}

			try 
			{
				if (wScript != null) 
				{
					wScript.Close();

					wScript = null;
				}
			} 
			catch (Exception e) 
			{
				Trace.error(Trace.FILE_IO_ERROR, sFileScript);
			}
		}

		/**
		 * Method declaration
		 *
		 *
		 * @throws Exception
		 */
		private void runScript() 
		{
			if (Trace.TRACE) 
			{
				Trace.trace();
			}

			if (!(new File(sFileScript)).Exists) 
			{
				return;
			}

			bRestoring = true;

			dDatabase.setReferentialIntegrity(false);

			ArrayList channel = new ArrayList();

			channel.Add(cSystem);

			Channel current = cSystem;
			int     size = 1;

			try 
			{
				DateTime	     time = DateTime.Now;
				StreamReader r = new StreamReader(sFileScript);

				while (true) 
				{
					string s = r.ReadLine();

					if (s == null) 
					{
						break;
					}

					if (s.StartsWith("/*C")) 
					{
						int id = Int32.FromString(s.Substring(3,(s.IndexOf('*', 4)-3)));

						if (id > channel.Count)
						{
							current = new Channel(cSystem, id);

							channel.Insert(id, current);
							dDatabase.registerChannel(current);
						}
						else 
						{
							current = (Channel) channel[id - 1];
						}

						s = s.Substring(s.IndexOf('/', 1) + 1);
					}

					if (!s.Equals("")) 
					{
						dDatabase.execute(s, current);
					}

					if (s.Equals("DISCONNECT")) 
					{
						int id = current.getId();

						current = new Channel(cSystem, id);

						channel.RemoveAt(id);
						channel.Insert(id, current);
					}
				}

				r.Close();

				for (int i = 0; i < size; i++) 
				{
					current = (Channel) channel[i];

					if (current != null) 
					{
						current.rollback();
					}
				}

				TimeSpan execution = DateTime.Now.Subtract(time);

				if (Trace.TRACE) 
				{
					Trace.trace(execution.TotalMilliseconds.ToInt64());
				}
			} 
			catch (IOException e) 
			{
				throw Trace.error(Trace.FILE_IO_ERROR, sFileScript + " " + e);
			}

			dDatabase.setReferentialIntegrity(true);

			bRestoring = false;
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param full
		 *
		 * @throws Exception
		 */
		private void writeScript(bool full) 
		{
			if (Trace.TRACE) 
			{
				Trace.trace();

				// create script in '.new' file
			}

			(new File(sFileScript + ".new")).Delete();

			// script; but only positions of cached tables, not full
			scriptToFile(dDatabase, sFileScript + ".new", full, cSystem);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param w
		 * @param s
		 *
		 * @throws IOException
		 */
		private static void writeLine(StreamWriter w, string s) 
		{
			w.WriteLine(s);
		}

		/**
		 * Method declaration
		 *
		 *
		 * @param r
		 *
		 * @return
		 *
		 * @throws IOException
		 */
		private static string readLine(TextReader r) 
		{
			string s = r.ReadLine();

			return stringConverter.asciiToUnicode(s);
		}

	}
}
