// queries.js - MongoDB Week 1 Assignment
const { MongoClient } = require("mongodb");

// Local MongoDB URI (replace with your Atlas connection if needed)
const uri = "mongodb://127.0.0.1:27017";
const dbName = "plp_bookstore";

async function main() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log(" Connected to MongoDB");

    const db = client.db(dbName);
    const books = db.collection("books");

    // ===============================
    //  TASK 2: BASIC CRUD OPERATIONS
    // ===============================

    console.log("\n Find all books in the 'Fiction' genre:");
    console.log(await books.find({ genre: "Fiction" }).toArray());

    console.log("\n Find books published after 1950:");
    console.log(await books.find({ published_year: { $gt: 1950 } }).toArray());

    console.log("\n Find books by George Orwell:");
    console.log(await books.find({ author: "George Orwell" }).toArray());

    console.log("\n Update the price of '1984' to 15.99:");
    await books.updateOne({ title: "1984" }, { $set: { price: 15.99 } });
    console.log(await books.findOne({ title: "1984" }));

    console.log("\n Delete a book by title 'Moby Dick':");
    await books.deleteOne({ title: "Moby Dick" });
    console.log("Deleted 'Moby Dick'");

    // ===============================
    //  TASK 3: ADVANCED QUERIES
    // ===============================

    console.log("\n Books in stock and published after 2010:");
    console.log(
      await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray()
    );

    console.log("\n Projection (title, author, price only):");
    console.log(await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray());

    console.log("\n⬆ Sort books by price (ascending):");
    console.log(await books.find().sort({ price: 1 }).toArray());

    console.log("\n⬇ Sort books by price (descending):");
    console.log(await books.find().sort({ price: -1 }).toArray());

    console.log("\n Pagination (Page 1, 5 books per page):");
    console.log(await books.find().limit(5).toArray());

    console.log("\n Pagination (Page 2, skip first 5 books):");
    console.log(await books.find().skip(5).limit(5).toArray());

    // ===============================
    //  TASK 4: AGGREGATION PIPELINES
    // ===============================

    console.log("\n Average price of books by genre:");
    console.log(await books.aggregate([
      { $group: { _id: "$genre", avgPrice: { $avg: "$price" } } },
      { $sort: { avgPrice: -1 } }
    ]).toArray());

    console.log("\n Author with the most books:");
    console.log(await books.aggregate([
      { $group: { _id: "$author", bookCount: { $sum: 1 } } },
      { $sort: { bookCount: -1 } },
      { $limit: 1 }
    ]).toArray());

    console.log("\n Group books by decade and count:");
    console.log(await books.aggregate([
      {
        $group: {
          _id: { $subtract: [{ $divide: ["$published_year", 10] }, { $mod: [{ $divide: ["$published_year", 10] }, 1] }] },
          count: { $sum: 1 }
        }
      },
      { $project: { decade: { $multiply: ["$_id", 10] }, count: 1, _id: 0 } },
      { $sort: { decade: 1 } }
    ]).toArray());

    // ===============================
    //  TASK 5: INDEXING
    // ===============================

    console.log("\n Creating index on 'title':");
    await books.createIndex({ title: 1 });
    console.log(await books.indexes());

    console.log("\n Creating compound index on 'author' and 'published_year':");
    await books.createIndex({ author: 1, published_year: -1 });
    console.log(await books.indexes());

    console.log("\n Using explain() to show performance improvement:");
    const explain = await books.find({ title: "The Hobbit" }).explain("executionStats");
    console.log(JSON.stringify(explain.executionStats, null, 2));

  } catch (err) {
    console.error(" Error:", err);
  } finally {
    await client.close();
    console.log("\n Connection closed.");
  }
}

main();
