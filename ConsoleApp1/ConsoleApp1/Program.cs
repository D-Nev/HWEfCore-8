using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using System.Data;

namespace ConsoleApp1;

class Program
{
    static void Main()
    {
        string connectionString = GetConnectionString();
        using (IDbConnection connection = new SqlConnection(connectionString))
        {
            //1
            string authorName = "J.K. Rowling";
            Author author = connection.QueryFirstOrDefault<Author>("SELECT * FROM Authors WHERE Name = @AuthorName", new { AuthorName = authorName });
            var books = GetBooksByAuthor(connection, author.Id);
            //2
            DeleteBookWithMinPriceInCategory(connection, 2);
            //3
            IncreasePriceForAllBooks(connection);
            //4
            var priceRangeBooks = GetBooksInPriceRange(connection, 10, 20);
            //5
            var booksCountPerAuthor = GetBookCountPerAuthor(connection);
        }
    }
    static List<Book> GetBooksByAuthor(IDbConnection connection, int authorId)
    {
        string query = @"
        SELECT b.*, a.Id, a.Name, c.Id, c.Name
        FROM Books b
        JOIN Authors a ON b.AuthorId = a.Id
        JOIN Categories c ON b.CategoryId = c.Id
        WHERE b.AuthorId = @AuthorId";
  
        var books = connection.Query<Book, Author, Category, Book>(query,
            (book, author, category) =>
            {
                book.Author = author;
                book.Category = category;
                return book;
            },
            new { AuthorId = authorId },
            splitOn: "Id, Id");
        return books.ToList();
    }
    static void DeleteBookWithMinPriceInCategory(IDbConnection connection, int categoryId)
    {
        string query = @"
        DELETE FROM Books
        WHERE Id IN (
            SELECT TOP 1 b.Id
            FROM Books b
            WHERE b.CategoryId = @CategoryId
            ORDER BY b.Price ASC
        )";

        connection.Execute(query, new { CategoryId = categoryId });
    }

    static void IncreasePriceForAllBooks(IDbConnection connection)
    {
        string query = @"
        UPDATE Books
        SET Price = Price * 1.05";
        connection.Execute(query);
    }
    static List<Book> GetBooksInPriceRange(IDbConnection connection, decimal minPrice, decimal maxPrice)
    {
        string query = @"
        SELECT b.*, a.Id, a.Name, c.Id, c.Name
        FROM Books b
        JOIN Authors a ON b.AuthorId = a.Id
        JOIN Categories c ON b.CategoryId = c.Id
        WHERE b.Price >= @MinPrice AND b.Price <= @MaxPrice";

        var books = connection.Query<Book, Author, Category, Book>(query,
            (book, author, category) =>
            {
                book.Author = author;
                book.Category = category;
                return book;
            },
            new { MinPrice = minPrice, MaxPrice = maxPrice },
            splitOn: "Id, Id");

        return books.AsList();
    }
    static List<BookAuthorsResult> GetBookCountPerAuthor(IDbConnection connection)
    {
        string query = @"
        SELECT a.Id, a.Name, COUNT(b.Id) as BooksCount
        FROM Authors a
        LEFT JOIN Books b ON a.Id = b.AuthorId
        GROUP BY a.Id, a.Name
        HAVING COUNT(b.Id) > 0";

        var result = connection.Query<BookAuthorsResult>(query).ToList();
        return result;
    }
    static string GetConnectionString()
    {
        var builder = new ConfigurationBuilder();
        builder.SetBasePath(Directory.GetCurrentDirectory());
        builder.AddJsonFile("appsettings.json");
        var config = builder.Build();
        return config.GetConnectionString("DefaultConnection");
    }


    public class BookAuthorsResult
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int BooksCount { get; set; }
    }
    public class Book
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public int AuthorId { get; set; }
        public int CategoryId { get; set; }
        public decimal Price { get; set; }
        public DateTime ReleaseDate { get; set; }
        public Author Author { get; set; }
        public Category Category { get; set; }
    }
    public class Author
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
    public class Category
    {
        public int Id { get; set; }
        public string Name { get; set; }
    }
}
