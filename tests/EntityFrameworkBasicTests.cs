#if NET40
using Npgsql;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Core.EntityClient;

namespace NpgsqlTests
{
    [TestFixture]
    public class EntityFrameworkBasicTests : TestBase
    {
        public EntityFrameworkBasicTests(string backendVersion)
            : base(backendVersion)
        {
        }

        [TestFixtureSetUp]
        public override void TestFixtureSetup()
        {
            base.TestFixtureSetup();
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                if (context.Database.Exists())
                    context.Database.Delete();//We delete to be 100% schema is synced
                context.Database.Create();
            }

            // Create sequence for the IntComputedValue property.
            using (var createSequenceConn = new NpgsqlConnection(ConnectionStringEF))
            {
                createSequenceConn.Open();
                ExecuteNonQuery("create sequence blog_int_computed_value_seq", createSequenceConn);
                ExecuteNonQuery("alter table \"dbo\".\"Blogs\" alter column \"IntComputedValue\" set default nextval('blog_int_computed_value_seq');", createSequenceConn);

            }
            

            // Create functions for ExecuteFunction test.
            {
                ExecuteNonQuery(""
                    + "CREATE OR REPLACE FUNCTION pass_thru_int(p integer) "
                    + "  RETURNS integer AS "
                    + "'select $1;' "
                    + "  LANGUAGE sql STABLE "
                    + "  COST 100; "
                    );
                ExecuteNonQuery(""
                    + "CREATE OR REPLACE FUNCTION pass_thru_str(p character varying) "
                    + "  RETURNS character varying AS "
                    + "'select $1;' "
                    + "  LANGUAGE sql STABLE "
                    + "  COST 100; "
                    );
            }

        }

        /// <summary>
        /// Clean any previous entites before our test
        /// </summary>
        [SetUp]
        protected override void SetUp()
        {
            base.SetUp();
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                context.Blogs.RemoveRange(context.Blogs);
                context.Posts.RemoveRange(context.Posts);
                context.SaveChanges();
            }
        }

        public class Blog
        {
            public int BlogId { get; set; }
            public string Name { get; set; }

            public virtual List<Post> Posts { get; set; }

            [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
            public int IntComputedValue { get; set; }
        }

        public class Post
        {
            public int PostId { get; set; }
            public string Title { get; set; }
            public string Content { get; set; }
            public byte Rating { get; set; }

            public int BlogId { get; set; }
            public virtual Blog Blog { get; set; }
        }

        public class BloggingContext : DbContext
        {
            public BloggingContext(string connection)
                : base(new NpgsqlConnection(connection), true)
            {
            }

            public DbSet<Blog> Blogs { get; set; }
            public DbSet<Post> Posts { get; set; }
        }

        [Test]
        public void InsertAndSelect()
        {
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var blog = new Blog()
                {
                    Name = "Some blog name"
                };
                blog.Posts = new List<Post>();
                for (int i = 0; i < 5; i++)
                    blog.Posts.Add(new Post()
                    {
                        Content = "Some post content " + i,
                        Rating = (byte)i,
                        Title = "Some post Title " + i
                    });
                context.Blogs.Add(blog);
                context.SaveChanges();
            }

            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var posts = from p in context.Posts
                            select p;
                Assert.AreEqual(5, posts.Count());
                foreach (var post in posts)
                {
                    StringAssert.StartsWith("Some post Title ", post.Title);
                }
            }
        }

        [Test]
        public void SelectWithWhere()
        {
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var blog = new Blog()
                {
                    Name = "Some blog name"
                };
                blog.Posts = new List<Post>();
                for (int i = 0; i < 5; i++)
                    blog.Posts.Add(new Post()
                    {
                        Content = "Some post content " + i,
                        Rating = (byte)i,
                        Title = "Some post Title " + i
                    });
                context.Blogs.Add(blog);
                context.SaveChanges();
            }

            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var posts = from p in context.Posts
                            where p.Rating < 3
                            select p;
                Assert.AreEqual(3, posts.Count());
                foreach (var post in posts)
                {
                    Assert.Less(post.Rating, 3);
                }
            }
        }

        [Test]
        public void OrderBy()
        {
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                Random random = new Random();
                var blog = new Blog()
                {
                    Name = "Some blog name"
                };

                blog.Posts = new List<Post>();
                for (int i = 0; i < 10; i++)
                    blog.Posts.Add(new Post()
                    {
                        Content = "Some post content " + i,
                        Rating = (byte)random.Next(0, 255),
                        Title = "Some post Title " + i
                    });
                context.Blogs.Add(blog);
                context.SaveChanges();
            }

            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var posts = from p in context.Posts
                            orderby p.Rating
                            select p;
                Assert.AreEqual(10, posts.Count());
                byte previousValue = 0;
                foreach (var post in posts)
                {
                    Assert.GreaterOrEqual(post.Rating, previousValue);
                    previousValue = post.Rating;
                }
            }
        }

        [Test]
        public void OrderByThenBy()
        {
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                Random random = new Random();
                var blog = new Blog()
                {
                    Name = "Some blog name"
                };

                blog.Posts = new List<Post>();
                for (int i = 0; i < 10; i++)
                    blog.Posts.Add(new Post()
                    {
                        Content = "Some post content " + i,
                        Rating = (byte)random.Next(0, 255),
                        Title = "Some post Title " + (i % 3)
                    });
                context.Blogs.Add(blog);
                context.SaveChanges();
            }

            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var posts = context.Posts.AsQueryable<Post>().OrderBy((p) => p.Title).ThenByDescending((p) => p.Rating);
                Assert.AreEqual(10, posts.Count());
                foreach (var post in posts)
                {
                    //TODO: Check outcome
                    Console.WriteLine(post.Title + " " + post.Rating);
                }
            }
        }

        [Test]
        public void TestComputedValue()
        {
            using (var context = new BloggingContext(ConnectionStringEF))
            {
                var blog = new Blog()
                {
                    Name = "Some blog name"
                };

                context.Blogs.Add(blog);
                context.SaveChanges();

                Assert.Greater(blog.BlogId, 0);
                Assert.Greater(blog.IntComputedValue, 0);
            }

        }


        [Test]
        public void GenerateObjectContext()
        {
            string csdl = "BloggingContextModel\\npgsql_tests.csdl";
            string ssdl = "BloggingContextModel\\npgsql_tests.ssdl";
            string msl = "BloggingContextModel\\npgsql_tests.msl";

            // http://msdn.microsoft.com/en-US/library/bb738533(v=vs.110).aspx

            EntityConnectionStringBuilder entityBuilder = new EntityConnectionStringBuilder();

            entityBuilder.Provider = "Npgsql";
            entityBuilder.ProviderConnectionString = Conn.ConnectionString;
            entityBuilder.Metadata = csdl + "|" + ssdl + "|" + msl;

            using (var context = new ObjectContext(new EntityConnection(entityBuilder.ToString()), true))
            {

            }
        }

        [Test]
        public void ExecuteFunction()
        {
            string csdl = "BloggingContextModel\\npgsql_tests.csdl";
            string ssdl = "BloggingContextModel\\npgsql_tests.ssdl";
            string msl = "BloggingContextModel\\npgsql_tests.msl";

            // http://msdn.microsoft.com/en-US/library/bb738533(v=vs.110).aspx

            EntityConnectionStringBuilder entityBuilder = new EntityConnectionStringBuilder();

            entityBuilder.Provider = "Npgsql";
            entityBuilder.ProviderConnectionString = Conn.ConnectionString;
            entityBuilder.Metadata = csdl + "|" + ssdl + "|" + msl;

            using (var context = new ObjectContext(new EntityConnection(entityBuilder.ToString()), true))
            {
                {
                    var res_int = context.ExecuteFunction<int>("npgsql_testsEntities.pass_thru_int", new ObjectParameter("p", 12345678)).ToArray();
                    Assert.AreEqual(res_int.Length, 1);
                    Assert.AreEqual(res_int[0], 12345678);
                }
                {
                    var res_str = context.ExecuteFunction<string>("npgsql_testsEntities.pass_thru_str", new ObjectParameter("p", "Hello world!")).ToArray();
                    Assert.AreEqual(res_str.Length, 1);
                    Assert.AreEqual(res_str[0], "Hello world!");
                }
            }
        }

        //Hunting season is open Happy hunting on OrderBy,GroupBy,Min,Max,Skip,Take,ThenBy... and all posible combinations
    }
}
#endif