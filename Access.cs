/*
 * Access.cs
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
	using System.Text;

	/**
	 * Access Class
	 * <P>The collection (ArrayList) of User object instances within a specific
	 * database.  Methods are provided for creating, modifying and deleting users,
	 * as well as manipulating their access rights to the database objects.
	 *
	 *
	 * @version 1.0.0.1
	 * @see User
	 */
	class Access 
	{
		public static int	SELECT = 1, DELETE = 2, INSERT = 4, UPDATE = 8, ALL = 15;
		private ArrayList   uUser;
		private User		uPublic;

		/**
		 * Access Class constructor
		 * <P>Creates a new ArrayList to contain the User object instances, as well
		 * as creating an initial PUBLIC user, with no password.
		 *
		 * @throws Exception
		 */
		public Access() 
		{
			uUser = new ArrayList();
			uPublic = createUser("PUBLIC", null, false);
		}

		/**
		 * getRight method declaration
		 * <P>This getRight method takes a string argument of the name of the access right.
		 *
		 * @param A string representation of the right.
		 *
		 * @return A static int representing the string right passed in.
		 */
		public static int getRight(string right) 
		{
			if (right.Equals("ALL")) 
			{
				return ALL;
			} 
			else if (right.Equals("SELECT")) 
			{
				return SELECT;
			} 
			else if (right.Equals("UPDATE")) 
			{
				return UPDATE;
			} 
			else if (right.Equals("DELETE")) 
			{
				return DELETE;
			} 
			else if (right.Equals("INSERT")) 
			{
				return INSERT;
			}

			throw Trace.error(Trace.UNEXPECTED_TOKEN, right);
		}

		/**
		 * getRight method declaration
		 * <P>This getRight method takes a int argument of the access right.
		 *
		 * @param A static int representing the right passed in.
		 *
		 * @return A string representation of the right or rights associated with the argument.
		 *
		 * @throws Exception
		 */
		public static string getRight(int right) 
		{

			if (right == ALL) 
			{
				return "ALL";
			} 
			else if (right == 0) 
			{
				return null;
			}

			StringBuilder b = new StringBuilder();

			if ((right & SELECT) != 0) 
			{
				b.Append("SELECT,");
			}

			if ((right & UPDATE) != 0) 
			{
				b.Append("UPDATE,");
			}

			if ((right & DELETE) != 0) 
			{
				b.Append("DELETE,");
			}

			if ((right & INSERT) != 0) 
			{
				b.Append("INSERT,");
			}

			string s = b.ToString();

			return s.Substring(0, s.Length - 1);
		}

		/**
		 * createUser method declaration
		 * <P>This method is used to create a new user.  The collection of users
		 * is first checked for a duplicate name, and an exception will be thrown
		 * if a user of the same name already exists.
		 *
		 * @param name (User login)
		 * @param password (Plaintext password)
		 * @param admin (Is this a database admin user?)
		 *
		 * @return An instance of the newly created User object
		 */
		public User createUser(string name, string password, bool admin) 
		{
			for (int i = 0; i < uUser.Count; i++) 
			{
				User u = (User)uUser[i];

				if (u != null && u.getName().Equals(name)) 
				{
					throw Trace.error(Trace.USER_ALREADY_EXISTS, name);
				}
			}

			User unew = new User(name, password, admin, uPublic);

			uUser.Add(unew);

			return unew;
		}

		/**
		 * dropUser method declaration
		 * <P>This method is used to drop a user.  Since we are using a vector
		 * to hold the User objects, we must iterate through the ArrayList looking
		 * for the name.  The user object is currently set to null, and all access
		 * rights revoked.
		 * <P><B>Note:</B>An ACCESS_IS_DENIED exception will be thrown if an attempt
		 * is made to drop the PUBLIC user.
		 *
		 * @param name of the user to be dropped
		 */
		public void dropUser(string name) 
		{
			Trace.check(!name.Equals("PUBLIC"), Trace.ACCESS_IS_DENIED);

			for (int i = 0; i < uUser.Count; i++) 
			{
				User u = (User) uUser[i];

				if (u != null && u.getName().Equals(name)) 
				{

					// todo: find a better way. Problem: removeElementAt would not
					// work correctly while others are connected
					uUser[i] = null;
					u.revokeAll();    // in case the user is referenced in another way

					return;
				}
			}

			throw Trace.error(Trace.USER_NOT_FOUND, name);
		}

		/**
		 * getUser method declaration
		 * <P>This method is used to return an instance of a particular User object,
		 * given the user name and password.
		 *
		 * <P><B>Note:</B>An ACCESS_IS_DENIED exception will be thrown if an attempt
		 * is made to get the PUBLIC user.
		 *
		 * @param user name
		 * @param user password
		 *
		 * @return The requested User object
		 *
		 * @throws Exception
		 */
		public User getUser(string name, string password) 
		{
			Trace.check(!name.Equals("PUBLIC"), Trace.ACCESS_IS_DENIED);

			if (name == null) 
			{
				name = "";
			}

			if (password == null) 
			{
				password = "";
			}

			User u = get(name);

			u.checkPassword(password);

			return u;
		}

		/**
		 * getUsers method declaration
		 * <P>This method is used to access the entire ArrayList of User objects for this database.
		 *
		 * @return The ArrayList of our User objects
		 */
		public ArrayList getUsers() 
		{
			return uUser;
		}

		/**
		 * grant method declaration
		 * <P>This method is used to grant a user rights to database objects.
		 *
		 * @param name of the user
		 * @param object in the database
		 * @param right to grant to the user
		 *
		 * @throws Exception
		 */
		public void grant(string name, string dbobject, int right) 
		{
			get(name).grant(dbobject, right);
		}

		/**
		 * revoke method declaration
		 * <P>This method is used to revoke a user's rights to database objects.
		 *
		 * @param name of the user
		 * @param object in the database
		 * @param right to grant to the user
		 *
		 * @throws Exception
		 */
		public void revoke(string name, string dbobject, int right) 
		{
			get(name).revoke(dbobject, right);
		}

		/*
		 * This private method is used to access the User objects in the collection
		 * and perform operations on them.
		 */
		private User get(string name) 
		{
			for (int i = 0; i < uUser.Count; i++) 
			{
				User u = (User) uUser[i];

				if (u != null && u.getName().Equals(name)) 
				{
					return u;
				}
			}

			throw Trace.error(Trace.USER_NOT_FOUND, name);
		}

	}

}
